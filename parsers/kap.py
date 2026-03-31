"""Parser for Key Acquisition Partners (KAP) broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class KapParser(BaseBrokerParser):
    broker_key: str = "kap"

    def parse(self, text: str) -> ParseResult:
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

        # --- KAP FORMAT ---
        # Two-column layout with:
        #   LEFT labels: MAILER:, MAILER OFFER:, MAILER KEY:, MAILER CATEGORY:, OFFER CATEGORY:
        #   Then: BROKER:, BROKER ORDER #:, WANTED BY:
        #   RIGHT values appear after the label blocks
        #
        # Line structure (from DL205):
        #   0: LIST MANAGEMENT DIVISION
        #   1: ORDER DATE:
        #   2: KAP ORDER:
        #   3: 9922  JZ (S/B #)
        #   4: S/B #
        #   5: List rental - L
        #   6: DL205 (KAP ORDER value)
        #   7: 18185 (S/B # value)
        #   8: 03/05/2026 (ORDER DATE value)
        #   ...
        #   13-17: MAILER labels
        #   18+: right column values (category#, mailer, offer, key, ...)
        #   22-24: BROKER:, BROKER ORDER #:, WANTED BY:
        #   31+: right column values for those labels (MAIL DATE, broker name, S/B, BROKER ORDER#, dates)

        # --- KAP ORDER (manager_order_number) ---
        manager_order_number = ""
        m = re.search(r"(DL\d+)", text)
        if m:
            manager_order_number = m.group(1)

        # --- S/B # ---
        sb_number = ""
        for i, ln in enumerate(lines):
            if ln.startswith("S/B #") or ln == "S/B #":
                # Value is nearby - look for 5-digit number
                for j in range(max(0, i - 3), min(i + 3, len(lines))):
                    if re.match(r"^\d{4,5}$", lines[j]):
                        sb_number = lines[j]
                        break
                break

        # --- ORDER DATE ---
        order_date = ""
        for i, ln in enumerate(lines[:15]):
            dm = re.match(r"^(\d{2}/\d{2}/\d{4})$", ln)
            if dm:
                order_date = self._normalize_date(dm.group(1))
                break

        # --- Find the MAILER label block (MAILER:, MAILER OFFER:, ..., OFFER CATEGORY:) ---
        mailer_label_idx = -1
        for i, ln in enumerate(lines):
            if ln == "MAILER:":
                mailer_label_idx = i
                break

        # --- Find OFFER CATEGORY: (end of first label block) ---
        offer_cat_idx = -1
        if mailer_label_idx >= 0:
            for i in range(mailer_label_idx, min(mailer_label_idx + 8, len(lines))):
                if lines[i] == "OFFER CATEGORY:":
                    offer_cat_idx = i
                    break

        # --- Values for MAILER block appear right after OFFER CATEGORY: ---
        mailer_name = ""
        mailer_offer = ""
        key_code = ""
        if offer_cat_idx >= 0:
            # Values: [category_num, MAILER_NAME, OFFER, KEY, ...]
            val_start = offer_cat_idx + 1
            vals = []
            for j in range(val_start, min(val_start + 10, len(lines))):
                if lines[j].endswith(":") and not re.match(r"^\d", lines[j]):
                    break
                vals.append(lines[j])

            # First value is a category number, skip it
            # MAILER name is the first text value (second overall)
            if len(vals) >= 2:
                mailer_name = vals[1]  # e.g., "PARTNERS IN HEALTH"
            if len(vals) >= 3:
                mailer_offer = vals[2]  # e.g., "MEMBERSHIP"
            if len(vals) >= 4:
                key_code = vals[3]  # e.g., "AIP"

        # --- Mailer PO = BROKER ORDER # (e.g., 129214), NOT the DL number ---
        # DL number goes in manager_order_number and title only
        mailer_po = ""

        # --- BROKER ORDER # extraction ---
        # Find BROKER:, BROKER ORDER #:, WANTED BY: label block
        broker_order_idx = -1
        for i, ln in enumerate(lines):
            if ln == "BROKER ORDER #:":
                broker_order_idx = i
                break

        # Find WANTED BY: label
        wanted_by_idx = -1
        if broker_order_idx >= 0:
            for i in range(broker_order_idx, min(broker_order_idx + 5, len(lines))):
                if lines[i] == "WANTED BY:" or lines[i].startswith("WANTED BY:"):
                    wanted_by_idx = i
                    break

        # Values for BROKER block appear later, anchored by MAIL DATE label
        mail_date = ""
        ship_by_date = ""

        # Find "MAIL DATE" label (standalone, not "MAIL DATE:")
        mail_date_label_idx = -1
        for i, ln in enumerate(lines):
            if ln == "MAIL DATE":
                mail_date_label_idx = i
                break

        if mail_date_label_idx >= 0:
            # Values after MAIL DATE: broker_name, broker_sb, BROKER_ORDER#, wanted_by_date, mail_date
            broker_vals = lines[mail_date_label_idx + 1:]
            # Find broker order # (numeric value, e.g., 129214 or E12316)
            for ln in broker_vals[:10]:
                if re.match(r"^[A-Z]?\d{4,}$", ln):
                    mailer_po = ln
                    break
            # Find dates in the broker values section
            dates_found = []
            for ln in broker_vals[:10]:
                dm = re.match(r"^(\d{2}/\d{2}/\d{4})$", ln)
                if dm:
                    dates_found.append(dm.group(1))

            # First date after MAIL DATE label = WANTED BY (ship_by_date)
            # Second date = MAIL DATE
            if len(dates_found) >= 2:
                ship_by_date = self._normalize_date(dates_found[0])
                mail_date = self._normalize_date(dates_found[1])
            elif len(dates_found) == 1:
                mail_date = self._normalize_date(dates_found[0])

        # --- LIST name ---
        list_name = ""
        for i, ln in enumerate(lines):
            if ln == "LIST:" or ln.startswith("LIST:"):
                rest = ln.replace("LIST:", "").strip()
                if rest:
                    list_name = rest
                    break
                # Value should be the next significant line
                if i + 1 < len(lines):
                    # Skip PRICE: if it appears
                    for j in range(i + 1, min(i + 3, len(lines))):
                        if lines[j] == "PRICE:" or lines[j].startswith("PRICE:"):
                            continue
                        if len(lines[j]) > 3 and not lines[j].endswith(":"):
                            list_name = lines[j]
                            break
                break

        # --- RENTAL QTY ---
        # In KAP format, RENTAL QTY: is a label and the value appears a couple lines later
        # e.g., line 50=RENTAL QTY:, line 51=TEST/CONT:, line 52=12,500
        requested_quantity = 0
        availability_rule = "Nth"
        for i, ln in enumerate(lines):
            if ln == "RENTAL QTY:" or ln.startswith("RENTAL QTY:"):
                rest = ln.replace("RENTAL QTY:", "").strip()
                if rest:
                    m = re.match(r"([\d,]+)", rest)
                    if m:
                        requested_quantity = int(m.group(1).replace(",", ""))
                    break
                # Look forward for the value (may be 1-3 lines after)
                for j in range(i + 1, min(i + 5, len(lines))):
                    m = re.match(r"^([\d,]+)$", lines[j])
                    if m:
                        val = int(m.group(1).replace(",", ""))
                        if val >= 100:
                            requested_quantity = val
                            break
                break

        if re.search(r"All\s+available", text, re.IGNORECASE):
            availability_rule = "All Available"

        # --- List manager = broker (KAP) ---
        list_manager = "KAP"

        # --- Contact info ---
        requestor_name = ""
        requestor_email = ""
        contact_match = re.search(r"Contact:\s*(.+?)(?:\n|$)", text, re.IGNORECASE)
        if contact_match:
            contact_text = contact_match.group(1).strip()
            email_m = re.search(r"([\w.+-]+@[\w.-]+\.\w+)", contact_text)
            if email_m:
                requestor_email = email_m.group(1)
                name_part = contact_text[:contact_text.index(email_m.group(0))].strip()
                if name_part:
                    requestor_name = name_part
            else:
                requestor_name = contact_text

        # --- Ship to email ---
        ship_to_email = ""
        m = re.search(r"Email:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
        if m:
            ship_to_email = m.group(1)

        # --- Shipping method ---
        shipping_method = ""
        if re.search(r"\bFTP\b", text):
            shipping_method = "FTP"
        elif re.search(r"\bE-?mail\b", text, re.IGNORECASE):
            shipping_method = "Email"

        shipping_instructions = f"CC: {requestor_email}" if requestor_email else ""

        # --- Omission ---
        omission_description = ""
        m = re.search(r"Omit[ \t:]+(.+?)(?:\n|$)", text, re.IGNORECASE)
        if m:
            omission_description = m.group(1).strip()

        # --- Other fees: auto-detect State Omits ---
        other_fees = self._detect_state_omits(omission_description)

        # --- Segment criteria ---
        segment_criteria = self._find(text, r"(?:Selects?|Segment)[:\s]+([^\n]+)")

        # --- Summary: P.O. {DL_number} {list_name} ---
        summary = f"P.O. {manager_order_number} {list_name}" if manager_order_number and list_name else ""

        return ParseResult(
            source=f"rule:{self.broker_key}",
            confidence=CONFIDENCE_RULE_BASED,
            summary=summary,
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
            file_format="",
            shipping_method=shipping_method,
            shipping_instructions=shipping_instructions,
            omission_description=omission_description,
            other_fees=other_fees,
            segment_criteria=segment_criteria,
        )
