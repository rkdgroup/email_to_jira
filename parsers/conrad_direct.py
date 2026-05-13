"""Parser for Conrad Direct broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


# Lines emitted as SELECTS that are actually delivery/format flags, not selection criteria.
_DELIVERY_TOKENS = re.compile(r"^(?:FTP|EMAIL|E[-\s]?MAIL|DISK|TAPE|CD|FIXED|ASCII|EXCEL)$", re.IGNORECASE)


class ConradDirectParser(BaseBrokerParser):
    broker_key: str = "conrad_direct"

    def _clean_conrad_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'\s*\n\s*', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    def _strip_pricing(self, line: str) -> str:
        """Remove '@ $...' pricing tails and 'Base Price ...' from a line."""
        line = re.sub(r"\s*@\s*\$.*$", "", line)
        line = re.sub(r"\s*Base\s+Price.*$", "", line, flags=re.IGNORECASE)
        return line.strip()

    def _strip_sidebar(self, line: str) -> str:
        """Remove the right-column '* Net Name *' style sidebar text from a line."""
        # Two-or-more spaces followed by an asterisk-bracketed cell.
        line = re.sub(r"\s{2,}\*[^\n]*$", "", line)
        # Or a trailing run of asterisks.
        line = re.sub(r"\s+\*{3,}\s*$", "", line)
        return line.strip()

    def parse(self, text: str) -> ParseResult:

        # 1. Basic identifiers
        po_number    = self._find(text, r"PURCHASE\s*ORDER\s*NO:\s*(\S+)")
        brok_mail_po = self._find(text, r"BROK/MAIL\s*PO:\s*(\S+)")

        # 2. Names — MAILER line only (not "To:" which matches SHIP TO)
        mailer_raw  = self._find(text, r"MAILER:\s*([^\n]+)")
        mailer_name = self._clean_conrad_text(mailer_raw)

        # Canonical short form (matches the allowed list in CLAUDE.md / list_managers.md).
        list_manager = "CONRAD"

        # 3. List and quantity
        list_raw  = self._find(text, r"LIST:\s*([^\n]+)")
        list_name = self._clean_conrad_text(list_raw)

        requested_quantity = 0
        base_segment = ""
        # The qty line in Conrad PDFs looks like:
        #   "      19,000  0-12 Mo. $5-$99.99 Donors          Base Price @ $   85.00  /M"
        # We want the donor segment between qty and Base Price (or any pricing/end-of-line).
        qty_match = re.search(
            r"LIST:[^\n]*\n\s*([\d,]+)\s+([^\n]+)",
            text,
        )
        if qty_match:
            requested_quantity = int(qty_match.group(1).replace(",", ""))
            base_segment = self._strip_pricing(qty_match.group(2))
            base_segment = self._clean_conrad_text(base_segment)

        # SELECTS block — capture every selection line until the next labelled section.
        selects: list[str] = []
        selects_block_match = re.search(
            r"SELECTS?:\s*([\s\S]+?)(?=\n[ \t]*(?:MATERIAL:|SHIP\s+TO:|TERMS:|PAYMENT|-{10,})|\Z)",
            text,
            flags=re.IGNORECASE,
        )
        if selects_block_match:
            for raw_line in selects_block_match.group(1).splitlines():
                line = self._strip_pricing(raw_line)
                line = self._strip_sidebar(line)
                line = re.sub(r"\s+", " ", line).strip()
                if not line:
                    continue
                # Skip delivery-method tokens like "FTP" / "Email" — they are not selection criteria.
                if _DELIVERY_TOKENS.match(line):
                    continue
                selects.append(line)

        segment_parts = []
        if base_segment:
            segment_parts.append(base_segment)
        segment_parts.extend(selects)
        segment = "\n".join(segment_parts)

        # 4. Availability rule
        availability_rule = "All Available" if re.search(r"\*FULL\s*RUN\*|ALL\s+AVAILABLE", text, re.IGNORECASE) else "Nth"

        # 5. Dates
        mail_date    = self._normalize_date(self._find(text, r"MAIL\s*DATE:\s*(\d{1,2}/\d{1,2}/\d{2,4})"))
        ship_by_date = self._normalize_date(self._find(text, r"NEEDED\s*BY:?\s*(\d{1,2}/\d{1,2}/\d{2,4})"))

        # 6. Contact — CONTACT: name, then phone line, then email on same or next line
        requestor_name  = ""
        requestor_email = ""
        contact_block = re.search(r"CONTACT:\s*([^\n]+)([\s\S]{0,120})", text, re.IGNORECASE)
        if contact_block:
            requestor_name = contact_block.group(1).strip()
            email_m = re.search(r"[\w.+-]+@[\w.-]+\.\w+", contact_block.group(2))
            if email_m:
                requestor_email = email_m.group()

        # 7. Shipping
        # Ship via: prefer explicit SHIP VIA label, then look for FTP/email keywords
        ship_via_raw = self._find(text, r"SHIP\s+VIA:\s*(\S+)")
        shipping_method = self._map_shipping_method(ship_via_raw)
        if not shipping_method:
            if re.search(r"\bFTP\b|Please\s+FTP", text, re.IGNORECASE):
                shipping_method = "FTP"
            elif re.search(r"Please\s*Email\s*Names\s*To", text, re.IGNORECASE):
                shipping_method = "Email"

        ship_to_email = self._find(text, r"Please\s*Email\s*Names\s*To:\s*([\w.+-]+@[\w.-]+\.\w+)")
        # Per memory note: if Ship To Email contains "@", Shipping Method must be "Email", not "Other".
        if ship_to_email and "@" in ship_to_email and shipping_method == "Other":
            shipping_method = "Email"

        file_format   = self._detect_file_format(text)

        # 8. Key code — text after "PO# <po> And|& <key code>" on the MATERIAL line.
        # Allow any non-newline characters in the key code (e.g. "Tafoya For Senate/SAVEMNPH02").
        key_code = ""
        key_m = re.search(r"PO#\s+\S+\s+(?:And|&)\s+([^\n]+)", text, re.IGNORECASE)
        if key_m:
            key_code = self._clean_conrad_text(key_m.group(1))

        # 9. Omissions — gather every "(Please) Omit X" instruction.
        # Skip pricing lines (containing @ or $) and trim the right-column sidebar text.
        omits: list[str] = []
        for m in re.finditer(r"\b(?:[Pp]lease\s+)?[Oo]mit\b\s+([^\n]+)", text):
            raw_omit = m.group(1)
            # Pricing line — skip entirely.
            if "@" in raw_omit and "$" in raw_omit:
                continue
            cleaned = self._strip_sidebar(raw_omit)
            cleaned = self._strip_pricing(cleaned)
            cleaned = re.sub(r"\s+", " ", cleaned).strip().rstrip(".")
            if cleaned and cleaned.lower() not in {o.lower() for o in omits}:
                omits.append(cleaned)
        omission_description = "; ".join(omits)
        other_fees = self._detect_state_omits(omission_description)

        # 10. Special seed instructions
        special_seed_instructions = self._extract_special_seed_instructions(text)

        return ParseResult(
            source=f"rule:{self.broker_key}",
            confidence=CONFIDENCE_RULE_BASED,
            mailer_name=mailer_name,
            mailer_po=brok_mail_po or po_number,
            list_name=list_name,
            list_manager=list_manager,
            requested_quantity=requested_quantity,
            manager_order_number=po_number,
            mail_date=mail_date,
            ship_by_date=ship_by_date,
            requestor_name=requestor_name,
            requestor_email=requestor_email,
            ship_to_email=ship_to_email,
            key_code=key_code,
            availability_rule=availability_rule,
            file_format=file_format,
            shipping_method=shipping_method,
            shipping_instructions=f"CC: {requestor_email}" if requestor_email else "",
            omission_description=omission_description,
            other_fees=other_fees,
            special_seed_instructions=special_seed_instructions,
            segment_criteria=segment,
        )
