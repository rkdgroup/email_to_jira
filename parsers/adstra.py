"""Parser for ADSTRA broker PDF orders (Purchase Order format from adstradata.com)."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class AdstraParser(BaseBrokerParser):
    broker_key: str = "adstra"

    def parse(self, text: str) -> ParseResult:
        # --- Manager Order # (J-prefix or I-prefix, e.g. J0832) ---
        manager_order_number = self._find(text, r"Adstra\s+order#:\s*([JI]\d+)", group=1)

        # --- Mailer PO (6-digit Broker PO) ---
        mailer_po = self._find(text, r"Broker\s+PO:\s*(\d{6})")
        if not mailer_po:
            mailer_po = self._find(text, r"\b(BRK-\d+|\d{6})\b")

        # --- Mailer Name ---
        # The name often appears on the line immediately preceding the "Mailer:" label
        mailer_name = self._find(text, r"([A-Z\s&]{3,})\nMailer:", group=1)
        if not mailer_name:
            mailer_name = self._find(text, r"Mailer:\s*\n\s*([^\n]+)", group=1)

        # --- List Name (Refined Multiline with Extended Boundary) ---
        # Look for everything between List: and the next definitive table or price field
        list_match = re.search(r"List:\s*(.*?)(?:\nPrice|\nQuantity|\nMaterial)", text, re.IGNORECASE | re.DOTALL)
        
        list_name_raw = ""
        if list_match:
            raw_block = list_match.group(1)
            clean_lines = []
            
            for line in raw_block.splitlines():
                line = line.strip()
                # Skip empty lines, misplaced labels, and address artifacts
                if not line or line.upper().startswith("SELECTS:") or line.upper() == "PURCHASE ORDER":
                    continue
                # Skip Broker address lines jumbled in (e.g. MANASSAS, VA 20110)
                if re.match(r"^[A-Z\s]+,\s*[A-Z]{2}\s*\d{5}", line, re.IGNORECASE):
                    continue
                
                clean_lines.append(line)
                
            list_name_raw = clean_lines[0] if clean_lines else ""

        # Strip list code in parens (e.g. (00552)) and clean up whitespace
        list_name = re.sub(r"\s*\(\d+\)\s*", " ", list_name_raw).strip()

        # --- List Manager = ADSTRA ---
        list_manager = "ADSTRA"

        # --- Dates ---
        mail_date = self._find_date(text, r"Mail\s+Date:\s*(\d{1,2}/\d{1,2}/\d{2,4})")
        ship_by_date = self._find_date(text, r"Ship\s+By:\s*(\d{1,2}/\d{1,2}/\d{2,4})")

        # --- Quantity and Availability ---
        requested_quantity, availability_rule = self._find_quantity(
            text, r"Quantity:\s*([\d,]+(?:\s+OR\s+ALL\s+AVAILABLE)?)"
        )

        # --- Key Code ---
        key_code = self._find(text, r"Key(?:\s+Code)?:\s*(\S+)")
        if key_code and re.match(r"^\d{3}$", key_code):
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
            m = re.search(r"SHIP\s+TO:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE) 
            ship_to_email = m.group(1) if m else ""

        # --- Requestor (Contact block) ---
        requestor_name = self._find(text, r"Contact:\s*([A-Z][A-Z\s]+?)(?:\n|$)")
        requestor_name = requestor_name.strip().title() if requestor_name else ""
        requestor_email = self._find(text, r"([\w.+-]+@adstradata\.com)", group=1) 

        # --- Shipping instructions ---
        shipping_instructions = f"CC: {requestor_email}" if requestor_email else ""

        # --- Omission description ---
        _STANDARD_OMITS = (
            "STANDARD OMITS (F6)\n"
            "NO PERSONAL NAME\n"
            "FED BLDGS, PRSN, LIB, SCHL, INST\n"
            "4-6 LINE ADDRESS\n"
            "NCC DNM FILE FOR LIST RENTAL (W/O 222222)\n"
            "\n"
            "STANDARD OMIT CRITERIA (Screen 1)\n"
            "APO, FPO\n"
            "PR, TERR, MILITARY\n"
            "COMPANY\n"
            "NCOA REASON CODES"
        )
        order_omits = self._find(text, r"OMIT:\s*(.+?)(?:\n|SHIP\s+TO)", group=1).strip()
        if order_omits:
            omission_description = f"{_STANDARD_OMITS}\n\n{order_omits}"
        else:
            omission_description = _STANDARD_OMITS

        # --- Segment criteria (Helper for multiline blocks) ---
        def extract_multiline_field(label_pattern, content, next_field_hints):
            """Extracts multiline text until a blank line or a new header is found."""
            stops = "|".join([rf"\n\s*{hint}:" for hint in next_field_hints])
            pattern = rf"{label_pattern}:\s*(.*?)(?:\n\s*\n|{stops}|$)"
            
            match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
            if match:
                lines = [ln.strip() for ln in match.group(1).splitlines() if ln.strip()]
                result = "\n".join(lines)
                if result and not re.match(r"^[A-Z\s/]+:$", result, re.IGNORECASE):
                    return result
            return ""

        doc_headers = ["L/O Ref", "Price", "Selects", "Quantity", "SHIP TO", "OMIT", "SPECIAL INSTRUCTIONS"]

        segment_criteria = extract_multiline_field(r"Pull\s+Description", text, doc_headers)
        selects_block = extract_multiline_field(r"Selects", text, doc_headers)
        if segment_criteria and selects_block:
            segment_criteria = segment_criteria + "\n" + selects_block
        elif not segment_criteria:
            segment_criteria = selects_block

        # --- Other fees & Special Seeds ---
        other_fees = ""
        special_seed_instructions = self._extract_special_seed_instructions(text)

        # --- File format ---
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