"""Parser for RMI Direct Marketing broker PDF orders (columnar table format)."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class RmiDirectParser(BaseBrokerParser):
    broker_key: str = "rmi_direct"

    def parse(self, text: str) -> ParseResult:
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

        # --- POSITIONAL MAPPING ---
        # RMI has a two-column layout that extracts as:
        #   values block 1 (header values: MGT#, date, job#, keycode, wantedby, maildate)
        #   label block 1 (Order Date, Job Number, Broker PO#, Key Code, Wanted By, Mail Date)
        #   clearance line
        #   values block 2 (Owner, list/qty, segment, omit, shipping info)
        #   label block 2 (Broker:, Owner:, Mailer:, ... at end)
        #   end values (mailer, offer, list name before the day-of-week)

        # --- MGT number (line 4 in both samples) ---
        manager_order_number = self._find(text, r"(MGT\d{2}-\d+)")
        # mailer_po comes from "Broker PO#" field, resolved below
        mailer_po = ""

        # --- Find label block positions ---
        # Find the "Order Date" label to know where label block 1 starts
        label1_start = -1
        for i, ln in enumerate(lines):
            if ln == "Order Date":
                label1_start = i
                break

        # The header values are between the MGT line and the label block
        # Mapping: values appear at positions (label1_start - N) through (label1_start - 1)
        # where labels are at label1_start through label1_start+5
        key_code = ""
        ship_by_date = ""
        mail_date = ""

        if label1_start > 0:
            # Find the MGT line
            mgt_idx = -1
            for i, ln in enumerate(lines[:label1_start]):
                if re.match(r"MGT\d{2}-\d+", ln):
                    mgt_idx = i
                    break

            if mgt_idx >= 0:
                # Values between mgt_idx+2 and label1_start are:
                # [order_date, job_number, broker_po_or_keycode, ...]
                # The exact count depends on how many fields there are
                val_lines = lines[mgt_idx + 2 : label1_start]
                # Labels: Order Date, Job Number, Broker PO#, Key Code, Wanted By, Mail Date
                # Find the label indices
                labels_in_block = []
                for j in range(label1_start, min(label1_start + 7, len(lines))):
                    if lines[j] in ("Order Date", "Job Number", "Broker PO#", "Key Code",
                                    "Wanted By", "Mail Date"):
                        labels_in_block.append(lines[j])

                # Map values to labels by aligning from the END (dates are most reliable)
                # because the number of values might be less than labels
                # (some fields share a value line in the two-column layout)
                offset = len(labels_in_block) - len(val_lines)
                if offset < 0:
                    offset = 0
                for idx, label in enumerate(labels_in_block):
                    val_idx = idx - offset
                    if 0 <= val_idx < len(val_lines):
                        val = val_lines[val_idx]
                        if label == "Broker PO#":
                            mailer_po = val
                        elif label == "Key Code":
                            key_code = val
                        elif label == "Wanted By":
                            ship_by_date = self._normalize_date(val)
                        elif label == "Mail Date":
                            mail_date = self._normalize_date(val)

        # Fallback: if no Broker PO# found, use MGT number
        if not mailer_po:
            mailer_po = manager_order_number

        # --- List manager = broker (RMI) ---
        list_manager = "RMI"

        # --- Owner (for reference, not list_manager) ---
        clearance_idx = -1
        for i, ln in enumerate(lines):
            if ln == "Clearance #":
                clearance_idx = i
                break

        if clearance_idx > 0:
            # Owner is at clearance_idx - 1 (the value before the label)
            # Actually the owner appears AFTER the clearance line
            # In the positional layout: Owner is the first non-label, non-number line after clearance
            owner_val = lines[clearance_idx - 1] if clearance_idx > 0 else ""
            # In 00385: line 17=CLR26-00115, line 18=Clearance #, line 19=Data Axle
            # In 00405: line 17=CLR26-00123, line 18=Clearance #, line 19=American Mailing Lists Corporation
            if clearance_idx + 1 < len(lines):
                pass  # Owner info available at lines[clearance_idx + 1] if needed

        # --- Quantity ---
        # The quantity appears after the owner, as a standalone comma number
        requested_quantity = 0
        availability_rule = "Nth"
        if clearance_idx > 0:
            for i in range(clearance_idx + 2, min(clearance_idx + 10, len(lines))):
                m = re.match(r"^([\d,]+)$", lines[i])
                if m:
                    val = int(m.group(1).replace(",", ""))
                    if val >= 100:
                        requested_quantity = val
                        break

        if re.search(r"ALL\s*[-\u2013]\s*All\s*Available|ALL\s+AVAILABLE", text, re.IGNORECASE):
            availability_rule = "All Available"

        # --- Mailer, Offer, List: at the end, 3 lines before day-of-week ---
        mailer_name = ""
        list_name = ""
        offer = ""

        day_idx = -1
        for i, ln in enumerate(lines):
            if re.match(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)", ln):
                day_idx = i
                break

        if day_idx >= 3:
            end_values = []
            for j in range(day_idx - 1, max(day_idx - 15, -1), -1):
                ln = lines[j]
                # Skip labels, keywords, numbers, pricing
                if (ln.endswith(":") or ln.startswith("(") or
                        ln in ("Material:", "E-mail", "Continuation", "Test",
                               "FTP", "Email", "FTP transfer", "E-mail")):
                    continue
                if re.match(r"^[\d,.]+$", ln):
                    continue
                if re.match(r"^[\d,.]+/[MF]$", ln):
                    continue
                # Skip short all-caps words that are select values (STATE, DOLLAR, etc.)
                if re.match(r"^[A-Z]+$", ln) and len(ln) <= 10:
                    continue
                if len(ln) > 2:
                    end_values.insert(0, ln)
                if len(end_values) >= 3:
                    break

            if len(end_values) >= 3:
                mailer_name = end_values[0]
                offer = end_values[1]
                list_name = end_values[2]
            elif len(end_values) == 2:
                mailer_name = end_values[0]
                list_name = end_values[1]
            elif len(end_values) == 1:
                mailer_name = end_values[0]

        list_name = list_name.strip()

        # --- Ship To email ---
        ship_to_email = ""
        m = re.search(r"TLIBRARIAN@[\w.-]+\.\w+|[\w.+-]+@data-management\.com", text, re.IGNORECASE)
        if m:
            ship_to_email = m.group()
        if not ship_to_email:
            m = re.search(r"NOTIFY:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
            if m:
                ship_to_email = m.group(1)

        # --- Contact info ---
        requestor_name = ""
        requestor_email = ""
        # Contact name appears right after the boilerplate "without preapproval..." block
        # It's the first name-like line before "Broker:" label
        for i, ln in enumerate(lines):
            if ln == "Broker:":
                # Go backwards to find name
                for j in range(i - 1, max(i - 10, -1), -1):
                    candidate = lines[j]
                    if (re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+", candidate) and
                            "@" not in candidate and not candidate.endswith(":")):
                        requestor_name = candidate
                        break
                break

        # EMail: label has the value on a nearby line (before the label in the columnar layout)
        # The value can be 5-8 lines before the label due to interleaved columns
        for i, ln in enumerate(lines):
            if ln == "EMail:":
                for j in range(max(0, i - 8), i):
                    m = re.search(r"([\w.+-]+@[\w.-]+\.\w+)", lines[j])
                    if m and "info@" not in m.group(1):
                        requestor_email = m.group(1)
                        break
                break
        if not requestor_email:
            requestor_email = self._find(text, r"([\w.+-]+@rmidirect\.com)")

        # --- Shipping method ---
        shipping_method = ""
        for i, ln in enumerate(lines):
            if ln == "VIA:":
                for j in range(max(0, i - 3), i):
                    if lines[j].lower() in ("email", "e-mail", "ftp"):
                        shipping_method = "Email" if "mail" in lines[j].lower() else "FTP"
                        break
                break
        if not shipping_method:
            if re.search(r"\bFTP\b", text):
                shipping_method = "FTP"
            elif re.search(r"\bE-?mail\b", text, re.IGNORECASE):
                shipping_method = "Email"

        # --- Shipping instructions = CC: requestor_email ---
        shipping_instructions = f"CC: {requestor_email}" if requestor_email else ""

        file_format = ""

        # --- Omissions ---
        omission_description = ""
        m = re.search(r"Omit[ \t:]+(.+?)(?:\n|$)", text, re.IGNORECASE)
        if m:
            omission_description = m.group(1).strip()

        # --- Other fees: auto-detect State Omits ---
        other_fees = self._detect_state_omits(omission_description)

        # --- Segment criteria ---
        segment_criteria = self._find(text, r"(?:Selects?|Segment)[:\s]+([^\n]+)")

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
            segment_criteria=segment_criteria,
        )
