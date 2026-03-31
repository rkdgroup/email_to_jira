"""Parser for CELCO Nonprofit broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class CelcoParser(BaseBrokerParser):
    broker_key: str = "celco"

    def parse(self, text: str) -> ParseResult:
        """Parse CELCO list rental/exchange order PDF text."""
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

        # --- Manager order number (ORDER #) ---
        # Format: "ORDER #\nD04-085280-NI"
        manager_order_number = ""
        for i, ln in enumerate(lines):
            if ln == "ORDER #" and i + 1 < len(lines):
                manager_order_number = lines[i + 1]
                break

        # --- mailer_po = ORDER # (e.g. D04-085463-CE) ---
        mailer_po = manager_order_number

        # --- Order date ---
        order_date_raw = ""
        for i, ln in enumerate(lines):
            if ln == "DATE" and i + 1 < len(lines):
                dm = re.match(r"(\d{1,2}/\d{1,2}/\d{2,4})", lines[i + 1])
                if dm:
                    order_date_raw = dm.group(1)
                break
        order_date = self._normalize_date(order_date_raw)

        # --- Client reference ---
        client_ref = ""
        for i, ln in enumerate(lines):
            if ln == "CLIENT REF" and i + 1 < len(lines):
                client_ref = lines[i + 1]
                break

        # --- Contact info ---
        contact_name = ""
        contact_email = ""
        # Find the CONTACT AT section
        for i, ln in enumerate(lines):
            if ln == "CONTACT" and i + 1 < len(lines) and lines[i + 1] == "AT":
                # Look forward for name and email
                for j in range(i + 2, min(i + 10, len(lines))):
                    if re.match(r"[\w.+-]+@", lines[j]):
                        contact_email = lines[j]
                    elif (re.match(r"^[A-Z][a-z]", lines[j]) and not lines[j].startswith("Page") and
                          not contact_name):
                        contact_name = lines[j]
                break

        # Fallback: celco email
        if not contact_email:
            celco_email_match = re.search(r"([\w.+-]+@celco\w*\.\w+)", text, re.IGNORECASE)
            if celco_email_match:
                contact_email = celco_email_match.group(1)

        # --- Order type ---
        order_type = ""
        if re.search(r"LIST\s+EXCHANGE\s+ORDER", text, re.IGNORECASE):
            order_type = "Exchange"
        elif re.search(r"LIST\s+RENTAL\s+ORDER", text, re.IGNORECASE):
            order_type = "Rental"

        # --- Mailer (USER field) ---
        mailer_name = ""
        for i, ln in enumerate(lines):
            if ln == "USER" and i + 1 < len(lines):
                mailer_name = lines[i + 1]
                break

        # --- Offer ---
        offer = ""
        for i, ln in enumerate(lines):
            if ln == "OFFER" and i + 1 < len(lines):
                offer = lines[i + 1]
                break
            if ln.startswith("OFFER"):
                rest = ln.replace("OFFER", "").strip()
                if rest:
                    offer = rest
                break

        # --- Mail date ---
        mail_date = ""
        for i, ln in enumerate(lines):
            if ln == "MAIL DATE" and i + 1 < len(lines):
                dm = re.match(r"(\d{1,2}/\d{1,2}/\d{2,4})", lines[i + 1])
                if dm:
                    mail_date = self._normalize_date(dm.group(1))
                break

        # --- Wanted by (ship_by_date) ---
        ship_by_date = ""
        for i, ln in enumerate(lines):
            if ln == "WANTED BY" and i + 1 < len(lines):
                dm = re.match(r"(\d{1,2}/\d{1,2}/\d{2,4})", lines[i + 1])
                if dm:
                    ship_by_date = self._normalize_date(dm.group(1))
                break

        # --- List name ---
        # The list name appears RIGHT BEFORE the "LIST" label
        # e.g. "PROJECT OPEN HAND\nLIST\nSEGMENT" or "ALLIANCE FOR RETIRED AMERICANS\nLIST\nSEGMENT"
        list_name = ""
        for i, ln in enumerate(lines):
            if ln == "LIST" and i > 0:
                candidate = lines[i - 1]
                # Make sure it's not a legal text line or label
                if (len(candidate) > 3 and not candidate.endswith(".") and
                        not candidate.startswith("OMIT") and
                        "CELCO" not in candidate and "damages" not in candidate.lower() and
                        "list" not in candidate.lower()):
                    list_name = candidate
                break

        # --- List manager: derive from list ---
        # In CELCO, the list owner is the organization whose list is being used
        list_manager = "CELCO"

        # --- Segment ---
        # In CELCO's two-column layout, right-column values appear AFTER the label block.
        # Layout in extracted text: [list_value] → LIST → SEGMENT → FORMAT → KEYCODE → [segment_value] → ...
        # Must skip past FORMAT and KEYCODE labels to reach the segment value.
        _CELCO_LABELS = {"FORMAT", "KEYCODE", "LIST", "SEGMENT", "USER", "OFFER",
                         "MAIL DATE", "WANTED BY", "SHIP VIA", "QUANTITY", "ORDER #",
                         "DATE", "CLIENT REF", "CONTACT", "AT", "SHIP TO", "MARK ALL"}
        segment = ""
        for i, ln in enumerate(lines):
            if ln == "SEGMENT" and i + 1 < len(lines):
                for j in range(i + 1, min(i + 10, len(lines))):
                    candidate = lines[j]
                    if candidate not in _CELCO_LABELS and len(candidate) > 2 and not candidate.endswith(":"):
                        segment = candidate
                        break
                break

        # --- Key code ---
        key_code = ""
        for i, ln in enumerate(lines):
            if ln == "KEYCODE" and i + 1 < len(lines):
                # Check next line - if it's a label or legal text, keycode is empty
                next_ln = lines[i + 1]
                if (not next_ln.startswith("Mailer") and not next_ln.startswith("finder") and
                        len(next_ln) > 0 and len(next_ln) < 30 and
                        not re.match(r"^[A-Z][a-z]", next_ln)):
                    key_code = next_ln
                break

        # --- Quantity ---
        requested_quantity = 0
        # Look for the quantity pattern: "2,183\nM" or "5,000\nQUANTITY"
        # Pattern 1: number followed by M on next line (indicating /M pricing)
        for i, ln in enumerate(lines):
            m = re.match(r"^([\d,]+)$", ln)
            if m and i + 1 < len(lines):
                val = int(m.group(1).replace(",", ""))
                if 50 <= val <= 999999 and lines[i + 1] in ("M", "QUANTITY"):
                    requested_quantity = val
                    break
        # Fallback: look for QUANTITY label
        if not requested_quantity:
            for i, ln in enumerate(lines):
                if ln == "QUANTITY" and i > 0:
                    for j in range(max(0, i - 3), i):
                        m = re.match(r"^([\d,]+)$", lines[j])
                        if m:
                            requested_quantity = int(m.group(1).replace(",", ""))
                            break
                    break
        # Fallback: any standalone number >= 100 in the middle section
        if not requested_quantity:
            for ln in lines:
                m = re.match(r"^([\d,]+)$", ln)
                if m:
                    val = int(m.group(1).replace(",", ""))
                    if 100 <= val <= 999999:
                        requested_quantity = val
                        break

        # --- Availability rule ---
        availability_rule = "Nth"
        if re.search(r"ALL\s+AVAILABLE", text, re.IGNORECASE):
            availability_rule = "All Available"

        # --- Shipping method ---
        ship_via = ""
        for i, ln in enumerate(lines):
            if ln == "SHIP VIA" and i > 0:
                ship_via = lines[i - 1]  # value precedes label in CELCO column layout
                break

        shipping_method = self._map_shipping_method(ship_via)
        # Also check for EMAIL/FTP in text
        if not shipping_method:
            if re.search(r"E-?MAIL\s+TRANSMISSION", text, re.IGNORECASE):
                shipping_method = "Email"
            elif re.search(r"\bFTP\b", text):
                shipping_method = "FTP"

        # --- Ship To section ---
        ship_to_email = ""
        requestor_name = ""
        requestor_email = ""
        # Look for SHIP TO section
        for i, ln in enumerate(lines):
            if ln == "SHIP TO":
                for j in range(i + 1, min(i + 8, len(lines))):
                    candidate = lines[j]
                    if candidate.startswith("MARK ALL"):
                        break
                    m = re.search(r"([\w.+-]+@[\w.-]+\.\w+)", candidate)
                    if m:
                        ship_to_email = m.group(1)
                    elif (re.match(r"^[A-Z][a-z]+ [A-Z]", candidate) and
                          not requestor_name and "@" not in candidate):
                        requestor_name = candidate
                break

        # Check for email instructions at bottom
        email_match = re.search(
            r"(?:PLEASE\s+EMAIL|email|Email\s+to|PLease\s+send)[:\s]+([\w.+-]+@[\w.-]+\.\w+)",
            text, re.IGNORECASE
        )
        if email_match:
            if not ship_to_email:
                ship_to_email = email_match.group(1)
            requestor_email = email_match.group(1)

        # Also check for "send shipping confirmation to" or "send Data via SFTP"
        confirm_match = re.search(
            r"(?:confirmation\s+to|send.*to)\s+([\w.+-]+@[\w.-]+\.\w+)",
            text, re.IGNORECASE
        )
        if confirm_match and not ship_to_email:
            ship_to_email = confirm_match.group(1)

        if not requestor_email:
            requestor_email = contact_email

        # --- Shipping instructions = CC: requestor_email ---
        shipping_instructions = ""
        if requestor_email:
            shipping_instructions = f"CC: {requestor_email}"

        # --- File format ---
        file_format = self._detect_file_format(text)

        # --- Omission description ---
        omission_description = ""
        omit_match = re.search(r"OMIT[ \t:]+(.+?)(?:\n|$)", text, re.IGNORECASE)
        if omit_match:
            omission_description = omit_match.group(1).strip()

        # --- Other fees: auto-detect State Omits ---
        other_fees = self._detect_state_omits(omission_description)

        # --- Billable account ---
        billable_account = ""

        return ParseResult(
            source=f"rule:{self.broker_key}",
            confidence=CONFIDENCE_RULE_BASED,
            mailer_name=mailer_name,
            mailer_po=mailer_po,
            list_name=list_name,
            list_manager=list_manager,
            requested_quantity=requested_quantity,
            manager_order_number=manager_order_number,
            mail_date=mail_date,
            ship_by_date=ship_by_date,
            requestor_name=requestor_name,
            requestor_email=requestor_email,
            ship_to_email=ship_to_email,
            key_code=key_code,
            billable_account=billable_account,
            availability_rule=availability_rule,
            file_format=file_format,
            shipping_method=shipping_method,
            shipping_instructions=shipping_instructions,
            omission_description=omission_description,
            other_fees=other_fees,
            segment_criteria=segment,
        )

