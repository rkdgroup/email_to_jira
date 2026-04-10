"""Parser for RKD Group and AMLC broker PDF orders (columnar Service Bureau format)."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class RkdGroupParser(BaseBrokerParser):
    broker_key: str = "rkd_group"

    def _parse_columnar(self, text: str):
        """
        Parse the two-column Service Bureau layout used by both RKD and AMLC.
        Returns a dict of extracted values.
        """
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        result = {}

        # --- Service Bureau No. / Purchase Order No. (line 1) ---
        result["manager_order_number"] = ""
        for ln in lines[:8]:
            if re.match(r"^\d{5,6}$", ln):
                result["manager_order_number"] = ln
                break

        # --- Find the Mail Date: label (end of first label block) ---
        mail_date_label_idx = -1
        for i, ln in enumerate(lines):
            if ln == "Mail Date:":
                mail_date_label_idx = i
                break

        # --- Mailer name: the non-date text line immediately before "Mailer:" label ---
        # In the columnar layout, right-column values appear BEFORE their left-column labels.
        # So the value for "Mailer:" is the last meaningful line before that label.
        result["mailer_name"] = ""
        result["mail_date"] = ""
        mailer_label_idx = -1
        for i, ln in enumerate(lines):
            if ln == "Mailer:" or ln.startswith("Mailer:"):
                mailer_label_idx = i
                break

        if mailer_label_idx > 0:
            for j in range(mailer_label_idx - 1, max(0, mailer_label_idx - 6), -1):
                candidate = lines[j]
                dm = re.match(r"^(\d{1,2}/\d{1,2}/\d{2,4})$", candidate)
                if dm:
                    if not result["mail_date"]:
                        result["mail_date"] = self._normalize_date(dm.group(1))
                    continue
                if (len(candidate) > 3 and not candidate.endswith(":") and
                        not re.match(r"^\d", candidate) and
                        not re.match(r"^[A-Z][a-z]+\s+[A-Z][a-z]+$", candidate)):
                    # Skip if it looks like a person name (contact), not an org
                    result["mailer_name"] = candidate
                    break

        # --- Quantity: before "Quantity:" label ---
        result["requested_quantity"] = 0
        result["availability_rule"] = "Nth"
        for i, ln in enumerate(lines):
            if ln == "Quantity:":
                for j in range(max(0, i - 5), i):
                    m = re.match(r"^([\d,]+)$", lines[j])
                    if m and int(m.group(1).replace(",", "")) >= 50:
                        result["requested_quantity"] = int(m.group(1).replace(",", ""))
                        break
                break
        if re.search(r"ALL\s+AVAILABLE", text, re.IGNORECASE):
            result["availability_rule"] = "All Available"

        # --- Client P.O.: AFTER "Ext:" label ---
        # In the columnar layout, the Client P.O. value appears right AFTER "Ext:"
        # e.g., Ext: (line 33) -> M8744 (line 34)
        result["mailer_po"] = ""
        ext_idx = -1
        for i, ln in enumerate(lines):
            if ln == "Ext:" or ln.startswith("Ext:"):
                ext_idx = i
                break
        if ext_idx >= 0:
            # Look AFTER Ext: for the PO value
            for j in range(ext_idx + 1, min(ext_idx + 5, len(lines))):
                candidate = lines[j]
                if (re.match(r"^[A-Z]\d{3,6}$", candidate) or
                        re.match(r"^\d{4,6}$", candidate)):
                    result["mailer_po"] = candidate
                    break

        # --- List name: the text line immediately before "Way Bill #:" ---
        # e.g., line 36=NATIONAL FOUNDATION FOR CANCER RESEAR, line 37=Way Bill #:
        # Or for AMLC: line 35=Tafoya for Senate (MN), line 36=Way Bill #:
        # Wait - in AMLC 667772: Tafoya for Senate (MN) IS the mailer, not the list.
        # And the list is Viguerie's Statewide Campaign Donor Superfile (line 21, right after labels).
        # So the line before Way Bill #: is actually the LIST name (the list being rented/exchanged).
        result["list_name"] = ""
        way_bill_idx = -1
        for i, ln in enumerate(lines):
            if ln == "Way Bill #:":
                way_bill_idx = i
                break
        if way_bill_idx > 0:
            for j in range(way_bill_idx - 1, max(way_bill_idx - 5, ext_idx if ext_idx >= 0 else 0), -1):
                candidate = lines[j]
                if (len(candidate) > 5 and not re.match(r"^\d+$", candidate) and
                        not candidate.endswith(":") and
                        not re.match(r"^[A-Z]\d{3,6}$", candidate)):
                    result["list_name"] = candidate
                    break

        # --- Key code: before "Key(s):" label ---
        # Search in reverse from closest line to Key(s): label
        result["key_code"] = ""
        _KEY_EXCLUDE = {
            "Managed", "Active", "Fax", "E-Mail", "FTP Transfer",
            "Call With Count before Shipping", "Nth Cross Section", "M", "/",
        }
        for i, ln in enumerate(lines):
            if re.match(r"Key\(?s?\)?:", ln, re.IGNORECASE):
                for j in range(i - 1, max(0, i - 4), -1):
                    candidate = lines[j]
                    if (not candidate.endswith(":") and len(candidate) >= 2 and
                            candidate not in _KEY_EXCLUDE and
                            not re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", candidate) and
                            not re.match(r"^[\d,]+$", candidate) and
                            not re.match(r"^\d+\.?\d*%$", candidate) and
                            not re.search(r"all\s+available", candidate, re.IGNORECASE)):
                        result["key_code"] = candidate
                        break
                break

        # --- Ship By date (Want By): date near "Offer:" ---
        result["ship_by_date"] = ""
        for i, ln in enumerate(lines):
            if ln == "Offer:" or ln.startswith("Offer:"):
                for j in range(max(0, i - 5), i):
                    dm = re.match(r"^(\d{1,2}/\d{1,2}/\d{2,4})$", lines[j])
                    if dm:
                        result["ship_by_date"] = self._normalize_date(dm.group(1))
                break

        # --- Contact info ---
        result["requestor_name"] = ""
        result["requestor_email"] = ""
        # Name appears before the first "Email:" label after the contact section
        # In RKD: line 53=Brittany Crabtree, line 54=Bcrabtree@rkdgroup.com, line 55=Email:
        # In AMLC 667772: line 53=Marty Anderson, line 54=marty@amlclists.com, line 55=Email:
        for i, ln in enumerate(lines):
            if ln == "Email:" and i > 20:
                for j in range(max(0, i - 3), i):
                    m_email = re.search(r"([\w.+-]+@[\w.-]+\.\w+)", lines[j])
                    if m_email:
                        result["requestor_email"] = m_email.group(1)
                        # Name is the line before the email
                        if j > 0 and re.match(r"^[A-Z][a-z]", lines[j - 1]):
                            result["requestor_name"] = lines[j - 1]
                        break
                break

        # --- Ship To email ---
        result["ship_to_email"] = ""
        m = re.search(r"Email\s+file\s+to:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
        if m:
            result["ship_to_email"] = m.group(1)
        if not result["ship_to_email"]:
            m = re.search(r"Email\s+to:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
            if m:
                result["ship_to_email"] = m.group(1)

        # --- Shipping ---
        result["shipping_method"] = "Email" if re.search(r"E-?Mail", text) else ""
        if not result["shipping_method"] and re.search(r"\bFTP\b", text):
            result["shipping_method"] = "FTP"

        result["shipping_instructions"] = (
            f"CC: {result['requestor_email']}" if result["requestor_email"] else ""
        )

        # --- Omission ---
        result["omission_description"] = ""
        m = re.search(r"(?:[Oo]mit|OMIT)[ \t:]+(.+?)(?:\n|$)", text)
        if m:
            result["omission_description"] = m.group(0).strip()

        # --- Special seed instructions ---
        result["special_seed_instructions"] = self._extract_special_seed_instructions(text)

        # --- Segment criteria ---
        # In the columnar layout, Segment: label has no inline value.
        # The actual criteria appears as the first content line after Way Bill #: / Attn To: labels.
        result["segment_criteria"] = ""
        if way_bill_idx >= 0:
            for j in range(way_bill_idx + 1, min(way_bill_idx + 8, len(lines))):
                candidate = lines[j]
                if (not candidate.endswith(":") and len(candidate) > 4 and
                        re.search(r"\$|\bmonth|\bdonor|\bomit|\bselect|\+", candidate, re.IGNORECASE)):
                    result["segment_criteria"] = candidate
                    break

        # --- Other fees: auto-detect State Omits ---
        result["other_fees"] = self._detect_state_omits(result["omission_description"])

        return result

    def parse(self, text: str) -> ParseResult:
        r = self._parse_columnar(text)
        return ParseResult(
            source=f"rule:{self.broker_key}",
            confidence=CONFIDENCE_RULE_BASED,
            mailer_name=r["mailer_name"],
            mailer_po=r["mailer_po"],
            list_name=r["list_name"],
            list_manager="RKD",
            requested_quantity=r["requested_quantity"],
            manager_order_number=r["manager_order_number"],
            mail_date=r["mail_date"],
            ship_by_date=r["ship_by_date"],
            requestor_name=r["requestor_name"],
            requestor_email=r["requestor_email"],
            ship_to_email=r["ship_to_email"],
            key_code=r["key_code"],
            availability_rule=r["availability_rule"],
            file_format=self._detect_file_format(text),
            shipping_method=r["shipping_method"],
            shipping_instructions=r["shipping_instructions"],
            omission_description=r["omission_description"],
            other_fees=r["other_fees"],
            special_seed_instructions=r["special_seed_instructions"],
            segment_criteria=r["segment_criteria"],
        )


class AmlcParser(RkdGroupParser):
    """AMLC orders use the same Service Bureau format as RKD Group."""
    broker_key: str = "amlc"

    def parse(self, text: str) -> ParseResult:
        r = self._parse_columnar(text)
        return ParseResult(
            source=f"rule:{self.broker_key}",
            confidence=CONFIDENCE_RULE_BASED,
            mailer_name=r["mailer_name"],
            mailer_po=r["mailer_po"],
            list_name=r["list_name"],
            list_manager="AMLC",
            requested_quantity=r["requested_quantity"],
            manager_order_number=r["manager_order_number"],
            mail_date=r["mail_date"],
            ship_by_date=r["ship_by_date"],
            requestor_name=r["requestor_name"],
            requestor_email=r["requestor_email"],
            ship_to_email=r["ship_to_email"],
            key_code=r["key_code"],
            availability_rule=r["availability_rule"],
            file_format=self._detect_file_format(text),
            shipping_method=r["shipping_method"],
            shipping_instructions=r["shipping_instructions"],
            omission_description=r["omission_description"],
            other_fees=r["other_fees"],
            special_seed_instructions=r["special_seed_instructions"],
            segment_criteria=r["segment_criteria"],
        )
