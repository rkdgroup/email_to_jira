"""Parser for ADSTRA broker PDF orders (Purchase Order format from adstradata.com)."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class AdstraParser(BaseBrokerParser):
    broker_key: str = "adstra"

    def parse(self, text: str) -> ParseResult:
        # --- Manager Order # (J-prefix or I-prefix, e.g. J0503) ---
        manager_order_number = self._find(text, r"Adstra\s+order#:\s*([JI]\d+)", group=1)

        # --- Mailer PO (6-digit Broker PO or BRK-prefixed) ---
        mailer_po = self._find(text, r"Broker\s+PO:\s*(\S+)")
        if not mailer_po:
            mailer_po = self._find(text, r"\b(BRK-\d+|\d{6})\b")

        # --- Mailer Name ---
        mailer_name = self._find(text, r"Mailer:\s*(.+?)(?:\n|Adstra order)")

        # --- List Name (strip list code in parens) ---
        list_name_raw = self._find(text, r"List:\s*(.+?)(?:\n|Price)")
        list_name = re.sub(r"\s*\(\d+\)\s*$", "", list_name_raw).strip()

        # --- List Manager = ADSTRA (issuing company) ---
        list_manager = "ADSTRA"

        # --- Dates ---
        mail_date = self._find_date(text, r"Mail\s+Date:\s*(\d{1,2}/\d{1,2}/\d{2,4})")
        ship_by_date = self._find_date(text, r"Ship\s+By:\s*(\d{1,2}/\d{1,2}/\d{2,4})")

        # --- Quantity and Availability ---
        qty_raw = self._find(text, r"Quantity:\s*([\d,]+(?:\s+OR\s+ALL\s+AVAILABLE)?)", group=1)
        requested_quantity, availability_rule = self._find_quantity(text, r"Quantity:\s*([\d,]+(?:\s+OR\s+ALL\s+AVAILABLE)?)")

        # --- Key Code ---
        key_code = self._find(text, r"Key(?:\s+Code)?:\s*(\S+)")
        # Key: field is often blank — reject generic noise
        if key_code and re.match(r"^\d{3}$|^$", key_code):
            key_code = ""

        # --- Shipping ---
        via_raw = self._find(text, r"VIA:\s*(\S+(?:\s*\S+)?)")
        shipping_method = self._map_shipping_method(via_raw)
        if not shipping_method:
            if re.search(r"\bE-?MAIL\b", text, re.IGNORECASE):
                shipping_method = "Email"
            elif re.search(r"\bFTP\b", text):
                shipping_method = "FTP"

        # --- Ship-To email ---
        ship_to_email = self._find(text, r"ATTN:\s*([\w.+-]+@[\w.-]+\.\w+)")
        if not ship_to_email:
            # Look for email after SHIP TO: line
            m = re.search(r"SHIP\s+TO:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
            ship_to_email = m.group(1) if m else ""

        # --- Requestor (Contact block near BOBBI DURRETT / ADSTRADATA) ---
        requestor_name = self._find(text, r"Contact:\s*([A-Z][A-Z\s]+?)(?:\n|$)")
        requestor_name = requestor_name.strip().title() if requestor_name else "Bobbi Durrett"
        requestor_email = self._find(text, r"([\w.+-]+@adstradata\.com)", group=1)
        if not requestor_email:
            requestor_email = "BOBBI.DURRETT@ADSTRADATA.COM"

        # --- Shipping instructions ---
        shipping_instructions = f"CC: {requestor_email}" if requestor_email else ""

        # --- Omission description ---
        omission_description = self._find(text, r"OMIT:\s*(.+?)(?:\n|SHIP\s+TO)", group=1).strip()

        # --- Segment criteria (Selects: field) ---
        segment_criteria = self._find(text, r"Selects:\s*(.+?)(?:\n|Price)", group=1).strip()

        # --- Other fees --- leave blank; State Omits is not auto-inferred from omit count
        other_fees = ""

        # --- Special seed instructions (Insert: lines only) ---
        special_seed_instructions = self._extract_special_seed_instructions(text)

        # --- File format: E-MAIL material = Other ---
        file_format = self._detect_file_format(text)

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
            availability_rule=availability_rule,
            file_format=file_format,
            shipping_method=shipping_method,
            shipping_instructions=shipping_instructions,
            omission_description=omission_description,
            other_fees=other_fees,
            special_seed_instructions=special_seed_instructions,
            segment_criteria=segment_criteria,
        )
