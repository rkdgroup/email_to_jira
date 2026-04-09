"""Parser for Names in the News (NIN) broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class NamesInNewsParser(BaseBrokerParser):
    broker_key: str = "names_in_news"

    def parse(self, text: str) -> ParseResult:
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

        # Find Fulfillment Copy marker
        fc_idx = 0
        for i, ln in enumerate(lines):
            if "Fulfillment Copy" in ln:
                fc_idx = i
                break

        # The NIN format has labels in the left column (lines before Fulfillment Copy)
        # and values in the right column (lines after Fulfillment Copy).
        # The label order is fixed: LR #, Date:, Mailer P.O., Client No.:, Mailer:,
        # Offer:, List Owner:, CC To:, List:, Quantity:, Base $, Merge id:, Keycode:,
        # Furnished on:, Return to:, Ship to arrive by:, Via:, Mail Date:, Special Instructions:
        #
        # Values after Fulfillment Copy match this order but some may be multi-line
        # or empty. We parse by finding specific patterns.

        value_lines = lines[fc_idx + 1:]

        # --- LR # (manager_order_number) - first value, pattern [A-Z]\d{4,} ---
        manager_order_number = ""
        for ln in value_lines[:5]:
            if re.match(r"^[A-Z]\d{4,}$", ln):
                manager_order_number = ln
                break

        # --- Date - second value ---
        order_date = ""
        for ln in value_lines[:5]:
            dm = re.match(r"^(\d{2}/\d{2}/\d{4})$", ln)
            if dm:
                order_date = self._normalize_date(dm.group(1))
                break

        # --- Mailer P.O. - 7-digit number ---
        mailer_po = ""
        for ln in value_lines[:8]:
            if re.match(r"^\d{6,}$", ln):
                mailer_po = ln
                break

        # --- Client No. - pattern like 000988/000 ---

        # --- Mailer name ---
        # The mailer name is a long text line (organization name) in the values
        # It appears after the Client No.
        mailer_name = ""
        po_found = False
        client_no_found = False
        for ln in value_lines:
            if ln == mailer_po:
                po_found = True
                continue
            if po_found and not client_no_found:
                # This is the Client No. line
                client_no_found = True
                continue
            if client_no_found:
                # This should be the Mailer name
                if len(ln) > 10 and re.search(r"[A-Za-z]{3}", ln):
                    mailer_name = ln.strip()
                break

        # --- List name ---
        # The list name appears later in values. We look for the "Natl Humane Education" or
        # similar pattern. It appears after multi-line address and references.
        # Better approach: find the line that starts with "Natl" or contains known list patterns
        list_name = ""
        # The list name appears before the segment line and quantity
        # In the values section, look for: DMI/DATA MANAGEMENT (return to), then next is list name
        dmi_idx = -1
        for i, ln in enumerate(value_lines):
            if "DMI" in ln or "DATA MANAGEMENT" in ln:
                dmi_idx = i
                break
        if dmi_idx >= 0 and dmi_idx + 1 < len(value_lines):
            list_name = value_lines[dmi_idx + 1]

        # --- Quantity ---
        requested_quantity = 0
        availability_rule = "Nth"
        for ln in value_lines:
            m = re.match(r"^([\d,]+)$", ln)
            if m:
                val = int(m.group(1).replace(",", ""))
                if 100 <= val <= 999999:
                    requested_quantity = val
                    break

        if re.search(r"all\s+available|Entire\s+list", text, re.IGNORECASE):
            availability_rule = "All Available"

        # --- List manager ---
        list_manager = "NAMES IN THE NEWS"

        # --- Mail Date: appears later in values ---
        # Find the date that appears AFTER the FTP/shipping section
        # In NIN: Mail Date is near the end (line 99 = 04/13/2026)
        mail_date = ""
        # Look for Special Instructions marker, then find date after it
        si_idx = -1
        for i, ln in enumerate(value_lines):
            if "See Special Instructions" in ln or "Special Instructions" in ln:
                si_idx = i
                break
        if si_idx >= 0:
            for ln in value_lines[si_idx + 1:si_idx + 5]:
                dm = re.match(r"^(\d{2}/\d{2}/\d{4})$", ln)
                if dm:
                    mail_date = self._normalize_date(dm.group(1))
                    break

        # Ship to arrive by date
        ship_by_date = ""
        for ln in value_lines:
            dm = re.match(r"^(\d{2}/\d{2}/\d{4})$", ln)
            if dm and ln != order_date:
                # The first non-order-date is either ship by or mail date
                candidate = self._normalize_date(dm.group(1))
                if candidate != order_date and not ship_by_date:
                    ship_by_date = candidate
                    continue

        # --- Key code ---
        key_code = ""
        # Look for a 4-digit number alone (like 1842) or #NNNN in values
        for ln in value_lines:
            if re.match(r"^\d{4}$", ln):
                key_code = ln
                break

        # --- NIN Contact ---
        # The contact info appears near the end: name, phone, email@nincal.com, initials
        requestor_name = ""
        requestor_email = ""
        # Find the @nincal.com email which identifies the NIN contact
        for i, ln in enumerate(value_lines):
            m = re.search(r"([\w.+-]+@nincal\.com)", ln, re.IGNORECASE)
            if m:
                requestor_email = m.group(1)
                # Name is the line before the email line
                if i > 0:
                    candidate = value_lines[i - 1]
                    # Might be a phone number, skip it
                    if re.match(r"^\(\d{3}\)", candidate):
                        if i > 1:
                            candidate = value_lines[i - 2]
                    if re.match(r"^[A-Z][a-z]+ [A-Z]", candidate) and "@" not in candidate:
                        requestor_name = candidate
                break

        # --- Shipping method ---
        shipping_method = ""
        if re.search(r"\bFTP\b", text):
            shipping_method = "FTP"
        elif re.search(r"\bE-?mail\b", text, re.IGNORECASE):
            shipping_method = "Email"

        # --- Ship to email ---
        ship_to_email = ""
        all_emails = re.findall(r"[\w.+-]+@[\w.-]+\.\w+", text)
        for email in all_emails:
            if "info@" not in email and "nincal" not in email:
                ship_to_email = email
                break
        # FTP orders use "FTP NOTIFY: email" convention
        if ship_to_email and shipping_method == "FTP":
            ship_to_email = f"FTP NOTIFY: {ship_to_email}"

        # --- Shipping instructions = CC: requestor_email ---
        shipping_instructions = f"CC: {requestor_email}" if requestor_email else ""

        # --- Special seed instructions (e.g., "Insert: 78204-2720") ---
        special_seed_instructions = self._extract_special_seed_instructions(text)

        # --- Other fees: auto-detect State Omits ---
        omission_description = ""
        omit_match = re.search(r"OMIT[ \t:]+(.+?)(?:\n|$)", text, re.IGNORECASE)
        if omit_match:
            candidate = omit_match.group(1).strip()
            # Skip non-descriptive placeholder values from the order form
            if candidate.lower() not in ("required.", "required", "yes", "no", "n/a"):
                omission_description = candidate
        other_fees = self._detect_state_omits(omission_description)

        # --- Segment criteria ---
        # NIN orders use "Offer:" for selection criteria; fall back to generic Selects/Segment
        segment_criteria = (
            self._find(text, r"Offer[:\s]+([^\n]+)")
            or self._find(text, r"(?:Selects?|Segment)[:\s]+([^\n]+)")
        )

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
            shipping_method=shipping_method,
            shipping_instructions=shipping_instructions,
            omission_description=omission_description,
            other_fees=other_fees,
            special_seed_instructions=special_seed_instructions,
            segment_criteria=segment_criteria,
        )
