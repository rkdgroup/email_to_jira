"""Parser for Data Axle and SimioCloud broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class DataAxleParser(BaseBrokerParser):
    broker_key: str = "data_axle"

    def _clean_nextmark_text(self, text: str) -> str:
        """Helper to remove NextMark CSV-style quotes, commas, and excess whitespace."""
        if not text:
            return ""
        # Strip literal quotes and leading commas caused by table formatting
        text = re.sub(r'["\n,]+', ' ', text)
        # Collapse multiple spaces into one
        return re.sub(r'\s+', ' ', text).strip()

    def parse(self, text: str) -> ParseResult:
        """Parse Data Axle rental/exchange order PDF text."""

        # --- Order number ---
        manager_order_number = self._find(text, r"Order\s*#\s*(\d+)")
        key_code_from_order = self._find(text, r"Order\s*#\s*\d+-(\S+)")

        # --- Mailer PO and list abbreviation from Ship Label ---
        ship_label = self._find(text, r"Ship\s*Label[:\s]*([^\n]+)")
        mailer_po = ""
        list_abbreviation = ""
        if ship_label:
            ship_label = self._clean_nextmark_text(ship_label)
            m = re.search(r"PO[#:\s]*([0-9]+)", ship_label, re.IGNORECASE)
            if m:
                mailer_po = m.group(1).strip()
                parts = ship_label.split("/")
                if len(parts) >= 2:
                    list_abbreviation = parts[1].strip()
            else:
                m = re.search(r"(\d{4,})", ship_label)
                if m:
                    mailer_po = m.group(1)
        
        if not mailer_po:
            mailer_po = manager_order_number

        # --- Explicit Key Code ---
        key_code_raw = self._find(text, r"Key\s*Code:[ \t\",]*([^\n]+)")
        key_code = self._clean_nextmark_text(key_code_raw)
        # Try Ship Label Key: field before falling back to order suffix
        if not key_code and ship_label:
            km = re.search(r"Key[:/\s]+([^/\s]+)", ship_label, re.IGNORECASE)
            if km:
                key_code = km.group(1).strip()
        if not key_code:
            key_code = key_code_from_order

        # --- Order type ---
        order_type = ""
        if re.search(r"Rental\s+Order", text, re.IGNORECASE):
            order_type = "Rental"
        elif re.search(r"Exchange\s+Order", text, re.IGNORECASE):
            order_type = "Exchange"

        # --- List manager ---
        list_manager = "DATA-AXLE"

        # --- From: contact info ---
        from_contact = ""
        from_email = ""
        from_match = re.search(r"From:\s*\n([\s\S]+?)(?:Mailer:|$)", text, re.IGNORECASE)
        if from_match:
            from_section = from_match.group(1)
            m = re.search(r"([\w.+-]+@[\w.-]+\.\w+)", from_section)
            if m:
                from_email = m.group(1)
            from_lines = [ln.strip() for ln in from_section.strip().split("\n") if ln.strip()]
            if len(from_lines) >= 2:
                from_contact = from_lines[1]

        # --- Mailer ---
        mailer_match = re.search(r"Mailer:[\s\",]*([\s\S]+?)(?=\n\s*\"?(?:Offer)[^\n]*:|\n\n|$)", text, re.IGNORECASE)
        mailer_name = self._clean_nextmark_text(mailer_match.group(1)) if mailer_match else ""

        # --- Offer ---
        offer_raw = self._find(text, r"Offer:[\s\",]*([^\n]+)")
        offer = self._clean_nextmark_text(offer_raw)

        # --- Mail date ---
        mail_date_raw = self._find(text, r"Mail\s*Date:[\s\",]*(\d{1,2}/\d{1,2}/\d{2,4})")
        mail_date = self._normalize_date(mail_date_raw)

        # --- List name (Media field) ---
        media_match = re.search(r"Media:[\s\",]*([\s\S]+?)(?=\n\s*\"?(?:Test/Cont|Base|Selects|Addressing|Order Quantity)[^\n]*:|\n\n|$)", text, re.IGNORECASE)
        list_name = self._clean_nextmark_text(media_match.group(1)) if media_match else ""

        # --- Quantity and availability ---
        # _find_quantity already handles cleaning for the int/str pair
        requested_quantity, availability_rule = self._find_quantity(
            text, r"Order\s*Quantity:[\s\",]*(.+)"
        )

        # --- Ship by date ---
        ship_by_raw = self._find(text, r"Needed\s*By:[\s\",]*(\d{1,2}/\d{1,2}/\d{2,4})")
        ship_by_date = self._normalize_date(ship_by_raw)

        # --- Shipping method ---
        shipping_via = self._find(text, r"Shipping\s*Via:[\s\",]*([^\n]+)")
        if not shipping_via:
            shipping_via = self._find(text, r"Ship\s*Via:[\s\",]*([^\n]+)")
        shipping_method = self._map_shipping_method(self._clean_nextmark_text(shipping_via))

        # --- Ship to email ---
        ship_to_match = re.search(r"Ship\s*to:[\s\S]+?([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
        ship_to_email = ship_to_match.group(1) if ship_to_match else ""

        # --- Requestor & Shipping Instructions ---
        requestor_name = from_contact if from_contact else ""
        requestor_email = from_email if from_email else ship_to_email
        
        cc_match = re.search(r"cc:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
        cc_email = cc_match.group(1) if cc_match else requestor_email
        shipping_instructions = f"CC: {cc_email}" if cc_email else ""

        # --- File format ---
        file_format = self._detect_file_format(text)

        # --- Omission description ---
        omission_description = ""
        omit_match = re.search(r"OMIT[ \t:]+([\s\S]+?)(?=\n[A-Z][a-z]+:|Job\s*#:|\n\n|$)", text, re.IGNORECASE)
        if omit_match:
            omission_description = self._clean_nextmark_text(omit_match.group(1))

        # --- Segment criteria ---
        base_match = re.search(
            r"\bBase:[\s\",]*([\s\S]+?)(?=\n\s*\"?(?:Selects|Addressing|Order Quantity|Other Fees)[^\n]*:|\n\n|$)", 
            text, 
            re.IGNORECASE
        )
        base_criteria = self._clean_nextmark_text(base_match.group(1)) if base_match else ""

        selects_match = re.search(
            r"Selects:[\s\",]*([\s\S]+?)(?=\n\s*\"?(?:Addressing|Order Quantity|Key Code|Other Fees)[^\n]*:|\n\n|$)",
            text,
            re.IGNORECASE
        )
        selects_criteria = self._clean_nextmark_text(selects_match.group(1)) if selects_match else ""

        if base_criteria and selects_criteria:
            segment_criteria = f"Base: {base_criteria} | Selects: {selects_criteria}"
        else:
            segment_criteria = base_criteria or selects_criteria

        # --- Other fees ---
        other_fees_raw = self._find(text, r"Other\s*Fees:[ \t\"]*([^\n]+)")
        other_fees = self._clean_nextmark_text(other_fees_raw)
        if not other_fees:
            other_fees = self._detect_state_omits(omission_description)

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
            billable_account="",
            availability_rule=availability_rule,
            file_format=file_format,
            shipping_method=shipping_method,
            shipping_instructions=shipping_instructions,
            omission_description=omission_description,
            other_fees=other_fees,
            segment_criteria=segment_criteria,
        )


class SimioCloudParser(DataAxleParser):
    """SimioCloud orders (WE ARE MOORE platform) — same format as Data Axle."""
    broker_key: str = "simiocloud"

    def parse(self, text: str) -> ParseResult:
        from dataclasses import replace
        result = super().parse(text)
        # SimioCloud is WE ARE MOORE's ordering platform, not Data Axle
        return replace(result, list_manager="WE ARE MOORE")