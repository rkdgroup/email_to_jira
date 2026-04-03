"""Parser for Key Acquisition Partners (KAP) broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class KapParser(BaseBrokerParser):
    broker_key: str = "kap"

    def _clean_kap_text(self, s: str) -> str:
        """Collapse whitespace and line breaks from extracted KAP field values."""
        if not s:
            return ""
        return re.sub(r"\s+", " ", s).strip()

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

        # --- ORDER DATE ---
        order_date = ""
        for i, ln in enumerate(lines[:15]):
            dm = re.match(r"^(\d{2}/\d{2}/\d{2,4})$", ln)
            if dm:
                order_date = self._normalize_date(dm.group(1))
                break

        # --- Find the MAILER label block (MAILER:, MAILER OFFER:, ..., OFFER CATEGORY:) ---
        mailer_label_idx = -1
        for i, ln in enumerate(lines):
            if ln.upper() == "MAILER:":
                mailer_label_idx = i
                break

        # --- Find OFFER CATEGORY: or CATEGORY: (end of first label block) ---
        offer_cat_idx = -1
        if mailer_label_idx >= 0:
            for i in range(mailer_label_idx, min(mailer_label_idx + 8, len(lines))):
                if lines[i].upper() in ("OFFER CATEGORY:", "CATEGORY:"):
                    offer_cat_idx = i
                    break

        # --- Values for MAILER block appear right after OFFER CATEGORY: / CATEGORY: ---
        # Also handle inline "Mailer: VALUE" format (e.g. from email body)
        mailer_name = ""
        mailer_offer = ""
        key_code = ""
        inline_mailer = re.search(r"^Mailer:\s*(.+)$", text, re.IGNORECASE | re.MULTILINE)
        if inline_mailer:
            mailer_name = inline_mailer.group(1).strip()
        if offer_cat_idx >= 0:
            val_start = offer_cat_idx + 1
            vals = []
            for j in range(val_start, min(val_start + 10, len(lines))):
                if lines[j].endswith(":") and not re.match(r"^\d", lines[j]):
                    break
                vals.append(lines[j])

            # Some formats prefix values with a numeric category code; skip it if present
            offset = 1 if (vals and re.match(r"^\d+$", vals[0])) else 0
            if len(vals) > offset:
                mailer_name = vals[offset]
            if len(vals) > offset + 2:
                key_code = vals[offset + 2]

        # --- Mailer PO = BROKER ORDER # (e.g., 129214), NOT the DL number ---
        # DL number goes in manager_order_number and title only
        mailer_po = ""

        # --- BROKER ORDER # extraction ---
        # Find BROKER:, BROKER ORDER #:, WANTED BY: label block
        broker_order_idx = -1
        for i, ln in enumerate(lines):
            if ln.upper() in ("BROKER ORDER #:", "BROKER ORDER:", "BROKER ORDER#:"):
                broker_order_idx = i
                break

        # Find WANTED BY: label
        wanted_by_idx = -1
        if broker_order_idx >= 0:
            for i in range(broker_order_idx, min(broker_order_idx + 5, len(lines))):
                if lines[i].upper().startswith("WANTED BY"):
                    wanted_by_idx = i
                    break

        # Values for BROKER block appear later, anchored by MAIL DATE label
        mail_date = ""
        ship_by_date = ""

        # Find "MAIL DATE" label (standalone, not "MAIL DATE:")
        mail_date_label_idx = -1
        for i, ln in enumerate(lines):
            if ln.upper().rstrip(":") == "MAIL DATE":
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
                dm = re.match(r"^(\d{2}/\d{2}/\d{2,4})$", ln)
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
        list_name_idx = -1
        for i, ln in enumerate(lines):
            if ln.upper() == "LIST:" or ln.upper().startswith("LIST:"):
                rest = re.sub(r"(?i)LIST:", "", ln).strip()
                if rest:
                    list_name = rest
                    list_name_idx = i
                    break
                # Value should be the next significant line
                for j in range(i + 1, min(i + 3, len(lines))):
                    if lines[j].upper().startswith("PRICE:"):
                        continue
                    if len(lines[j]) > 3 and not lines[j].endswith(":"):
                        list_name = lines[j]
                        list_name_idx = j
                        break
                break

        # --- Selection criteria: unlabeled line after list name (e.g. "18 MONTHS $10-$99.99") ---
        segment_criteria = ""
        if list_name_idx >= 0:
            for j in range(list_name_idx + 1, min(list_name_idx + 4, len(lines))):
                ln = lines[j]
                if ln.upper().startswith("PRICE:"):
                    continue
                if ln.endswith(":") or re.match(r"^\$[\d,]", ln) or re.match(r"^\d+\.\d{2}", ln):
                    break
                if len(ln) > 3:
                    segment_criteria = ln
                    break

        # --- RENTAL QTY ---
        requested_quantity = 0
        availability_rule = "Nth"
        qty_m = re.search(r"RENTAL\s*QTY:[\s\S]*?([\d,]{3,})", text, re.IGNORECASE)
        if qty_m:
            requested_quantity = int(qty_m.group(1).replace(",", ""))
        if re.search(r"All\s+available", text, re.IGNORECASE):
            availability_rule = "All Available"

        # --- List manager = broker (KAP) ---
        list_manager = "KAP"

        # --- Contact info: KAP's own rep appears as "Please contact NAME at Email: EMAIL" ---
        # Fallback: any @keyacquisition.com email in the text (e.g. email-only orders)
        requestor_name = ""
        requestor_email = ""
        m = re.search(r"Please contact\s+(.+?)\s+at\s+Email:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
        if m:
            requestor_name = m.group(1).strip()
            requestor_email = m.group(2).strip()
        if not requestor_email:
            # Find any non-noreply @keyacquisition.com address (e.g. email-only orders)
            for m in re.finditer(r"([\w.+-]+@keyacquisition(?:partners)?\.com)", text, re.IGNORECASE):
                addr = m.group(1).strip()
                if not addr.lower().startswith("no-reply") and not addr.lower().startswith("noreply"):
                    requestor_email = addr
                    break

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
        # Fall back to explicit Selects: label if unlabeled line wasn't found
        if not segment_criteria:
            segment_criteria = self._find(text, r"(?:Selects?|Segment):[ \t]*([^\n]+)")

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
