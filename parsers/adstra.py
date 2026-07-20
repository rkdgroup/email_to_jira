"""Parser for ADSTRA broker PDF orders (Purchase Order format from adstradata.com)."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class AdstraParser(BaseBrokerParser):
    broker_key: str = "adstra"

    def parse(self, text: str) -> ParseResult:
        # --- Manager Order # (J-prefix or I-prefix, e.g. J0832) ---
        manager_order_number = self._find(text, r"Adstra\s+order#:\s*([JI]\d+)", group=1)

        # --- Mailer PO (Broker PO — may be numeric or alphanumeric e.g. E14537) ---
        mailer_po = self._find(text, r"Broker\s+PO:\s*([A-Z0-9-]+)")
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

        # Extract 5-digit list code before stripping (used for adstra.yaml lookup)
        _code_m = re.search(r"\((\d{5})\)", list_name_raw)
        adstra_list_code = _code_m.group(1) if _code_m else ""
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
        # Normalize dotted abbreviations e.g. "F.T.P." → "FTP", "E-MAIL" → "EMAIL"
        via_normalized = re.sub(r"\.", "", via_raw or "")
        shipping_method = self._map_shipping_method(via_normalized)
        if not shipping_method:
            if re.search(r"\bE-?MAIL\b", text, re.IGNORECASE):
                shipping_method = "Email"
            elif re.search(r"\bF\.?T\.?P\.?\b", text):
                shipping_method = "FTP"

        # --- Ship-To email ---
        ship_to_email = self._find(text, r"ATTN:\s*([\w.+-]+@[\w.-]+\.\w+)")
        if not ship_to_email:
            m = re.search(r"SHIP\s+TO:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
            ship_to_email = m.group(1) if m else ""
        # FTP orders: pull notify email from Special Instructions block
        if not ship_to_email and shipping_method == "FTP":
            m = re.search(r"(?:EMAIL|CONFIRM|CONFIRMATION)[^\n]*?([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
            if m:
                ship_to_email = f"FTP NOTIFY: {m.group(1)}"

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
        # Collect OMIT lines from the order. Geographic / selection omits (e.g.
        # "OMIT NJ AND DC") live in the Pull Description block, while handling
        # omits (e.g. "OMIT CAN., P.R., ...") live in SPECIAL INSTRUCTIONS.
        # Scan both blocks so neither source is dropped.
        def _collect_omit_lines(block: str) -> list[str]:
            out: list[str] = []
            lines = block.splitlines()
            i = 0
            while i < len(lines):
                ln = lines[i].strip()
                # Keep OMIT lines, plus household-dedupe instructions ("1 PER
                # HOUSEHOLD", "1 PER HH") — the latter aren't OMIT lines but must
                # still land in the omission description with the state/flag omits.
                if ln and not re.search(r"DO\s+NOT\s+OMIT", ln, re.IGNORECASE) and (
                        re.search(r"\bOMIT\b", ln, re.IGNORECASE)
                        or re.search(r"\bPER\s+H(?:OUSEHOLD|H)\b", ln, re.IGNORECASE)):
                    # Join continuation lines: if this line ends with a comma,
                    # the next line continues the same list (e.g. wrapped state codes).
                    while ln.endswith(",") and i + 1 < len(lines):
                        i += 1
                        ln = ln + " " + lines[i].strip()
                    out.append(ln)
                i += 1
            return out

        # Pull Description block (stop at the next field label / SPECIAL INSTRUCTIONS)
        pd_m = re.search(
            r"Pull\s+Description:\s*(.*?)(?=\n\s*L/O\s+Ref|\n\s*SPECIAL\s+INSTRUCTIONS|\Z)",
            text, re.IGNORECASE | re.DOTALL,
        )
        # SPECIAL INSTRUCTIONS block (stop before FTP credentials / payment line)
        si_m = re.search(
            r"SPECIAL\s+INSTRUCTIONS[^\n]*\n(.*?)(?=UPLOAD\s+FILES|PAYMENT\s+DUE|\Z)",
            text, re.IGNORECASE | re.DOTALL,
        )
        # Pure state-omit lines ("OMIT NJ AND DC") are collapsed into a single
        # canonical "STATE OMITS: NJ, DC" line (mirroring the FLAG OMITS line).
        # Any other omit line is kept verbatim.
        state_codes: list[str] = []
        household_line: str = ""
        other_omit_lines: list[str] = []
        seen: set[str] = set()
        for ln in _collect_omit_lines(pd_m.group(1) if pd_m else "") + _collect_omit_lines(si_m.group(1) if si_m else ""):
            key = ln.upper()
            if key in seen:
                continue
            seen.add(key)
            codes = self._state_codes_from_omit(ln)
            if codes:
                for c in codes:
                    if c not in state_codes:
                        state_codes.append(c)
            elif re.search(r"\bPER\s+H(?:OUSEHOLD|H)\b", ln, re.IGNORECASE):
                # Household de-dupe (e.g. "1 PER HOUSEHOLD") — kept verbatim so a
                # count other than 1 isn't rewritten; first occurrence wins.
                if not household_line:
                    household_line = ln
            else:
                other_omit_lines.append(ln)
        order_omit_lines: list[str] = []
        if state_codes:
            order_omit_lines.append("STATE OMITS: " + ", ".join(state_codes))
        if household_line:
            order_omit_lines.append(household_line)
        order_omit_lines.extend(other_omit_lines)
        if order_omit_lines:
            omission_description = _STANDARD_OMITS + "\n\n" + "\n".join(order_omit_lines)
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
        other_fees = self._detect_state_omits(omission_description)
        special_seed_instructions = self._extract_special_seed_instructions(text)

        # --- File format (Saturn Corp = ASCII Fixed via FTP) ---
        if self._is_saturn_order(text):
            file_format = "ASCII Fixed"
            shipping_method = "FTP"
        else:
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
            adstra_list_code=adstra_list_code,
        )