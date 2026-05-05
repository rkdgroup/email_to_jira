"""Parser for Conrad Direct broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class ConradDirectParser(BaseBrokerParser):
    broker_key: str = "conrad_direct"

    def _clean_conrad_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'\s*\n\s*', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    def parse(self, text: str) -> ParseResult:

        # 1. Basic identifiers
        po_number    = self._find(text, r"PURCHASE\s*ORDER\s*NO:\s*(\S+)")
        brok_mail_po = self._find(text, r"BROK/MAIL\s*PO:\s*(\S+)")

        # 2. Names — MAILER line only (not "To:" which matches SHIP TO)
        mailer_raw  = self._find(text, r"MAILER:\s*([^\n]+)")
        mailer_name = self._clean_conrad_text(mailer_raw)

        # List manager is always Conrad Direct
        list_manager = "CONRAD DIRECT"

        # 3. List and quantity
        list_raw  = self._find(text, r"LIST:\s*([^\n]+)")
        list_name = self._clean_conrad_text(list_raw)

        requested_quantity = 0
        segment = ""
        qty_match = re.search(
            r"LIST:[\s\S]+?\n\s*([\d,]+)\s+([\s\S]+?)(?=\n\s*(?:\*FULL\s*RUN\*|Base Price|SELECTS?:|[A-Z]{3,}:)|$)",
            text,
        )
        if qty_match:
            requested_quantity = int(qty_match.group(1).replace(",", ""))
            segment = self._clean_conrad_text(qty_match.group(2))

        # SELECTS line — capture selection criteria only, strip price columns (@ $...)
        selects_raw = self._find(text, r"SELECTS?:\s*([^\n]+)")
        if selects_raw:
            selects_clean = re.sub(r"\s*@\s*\$.*", "", selects_raw).strip()
            selects_clean = self._clean_conrad_text(selects_clean)
            if selects_clean:
                segment = f"{segment}  {selects_clean}".strip() if segment else selects_clean

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
        file_format   = self._detect_file_format(text)

        # 8. Key code — text after PO# ... And|& on the MATERIAL line
        key_code = ""
        key_m = re.search(r"PO#\s+\S+\s+(?:And|&)\s+([\w\s#]+)(?=\n|$)", text, re.IGNORECASE)
        if key_m:
            key_code = self._clean_conrad_text(key_m.group(1))

        # 9. Omissions — stop at dashes separator or SHIP TO
        omission_raw = self._find(
            text,
            r"[Oo]mit\s+([\s\S]+?)(?=[-]{10,}|\n[ \t]*SHIP\s+TO:|\n[ \t]*TERMS:|\n\n)",
            group=1,
        )
        omission_description = self._clean_conrad_text(omission_raw)
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
