import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class RkdGroupParser(BaseBrokerParser):
    broker_key: str = "rkd_group"

    def _parse_columnar(self, text: str):
        """
        Parse the two-column Service Bureau layout used by both RKD and AMLC.
        Includes fixes for table-based text extraction (quotes/commas).
        """
        lines = [ln.strip().replace('"', '') for ln in text.split("\n") if ln.strip()]
        result = {}

        # --- Service Bureau No. / Purchase Order No. ---
        result["manager_order_number"] = ""
        for ln in lines[:10]:
            m = re.search(r"(\d{5,6})", ln)
            if m:
                result["manager_order_number"] = m.group(1)
                break

        # --- Order Status (Rental vs Exchange) ---
        result["order_status"] = ""
        for i, ln in enumerate(lines):
            clean_ln = ln.replace(',', '').strip()
            if clean_ln == "Status:":
                # Look forward for values (table/CSV layout)
                forward_vals = []
                for j in range(i + 1, min(len(lines), i + 4)):
                    candidate = lines[j].replace(',', '').strip()
                    if candidate.endswith(":") or not candidate:
                        break
                    forward_vals.append(candidate)

                status_str = " ".join(forward_vals)
                if re.search(r"\b(rental|exchange)\b", status_str, re.IGNORECASE):
                    result["order_status"] = status_str
                    break

                # Fallback: Look backward (traditional columnar layout)
                for j in range(i - 1, max(0, i - 4), -1):
                    candidate = lines[j].replace(',', '').strip()
                    if candidate and not candidate.endswith(":") and len(candidate) > 2:
                        result["order_status"] = candidate
                        break
                break

        # --- Mail Date ---
        result["mail_date"] = ""
        for i, ln in enumerate(lines):
            if "Mail Date" in ln:
                for j in range(i - 2, i + 5):
                    if 0 <= j < len(lines):
                        dm = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", lines[j])
                        if dm:
                            result["mail_date"] = self._normalize_date(dm.group(1))
                            break
                if result["mail_date"]:
                    break

        # --- Ship By date (Want By / Offer date) ---
        result["ship_by_date"] = ""
        for i, ln in enumerate(lines):
            if ln == "Offer:" or ln.startswith("Offer:"):
                for j in range(max(0, i - 5), i):
                    dm = re.match(r"^(\d{1,2}/\d{1,2}/\d{2,4})$", lines[j])
                    if dm:
                        result["ship_by_date"] = self._normalize_date(dm.group(1))
                break

        # --- Mailer name ---
        result["mailer_name"] = ""
        mailer_label_idx = -1
        for i, ln in enumerate(lines):
            if ln.startswith("Mailer:"):
                mailer_label_idx = i
                break

        _STATUS_WORDS = {"rental", "exchange", "active", "inactive", "managed"}
        _MAILER_SKIP = {"american mailing lists corporation management", "data management, inc."}

        if mailer_label_idx >= 0:
            # First check forward (table layout)
            for j in range(mailer_label_idx + 1, mailer_label_idx + 4):
                if j < len(lines):
                    candidate = lines[j].strip(",")
                    if (len(candidate) > 3 and not candidate.endswith(":") and
                            not re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", candidate) and
                            not re.match(r"^\d", candidate) and
                            candidate.lower() not in _STATUS_WORDS and
                            candidate.lower() not in _MAILER_SKIP):
                        result["mailer_name"] = candidate
                        break
            # Fallback backward
            if not result["mailer_name"]:
                for j in range(mailer_label_idx - 1, max(0, mailer_label_idx - 5), -1):
                    candidate = lines[j].strip(",")
                    if (len(candidate) > 3 and not candidate.endswith(":") and
                            not re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", candidate) and
                            not re.match(r"^\d", candidate) and
                            candidate.lower() not in _STATUS_WORDS and
                            candidate.lower() not in _MAILER_SKIP):
                        result["mailer_name"] = candidate
                        break

        # Last-resort fallback: value immediately before "Way Bill #:" label
        # In AMLC table format the actual client/mailer name appears there.
        if not result["mailer_name"]:
            for i, ln in enumerate(lines):
                if ln == "Way Bill #:":
                    for j in range(i - 1, max(0, i - 5), -1):
                        candidate = lines[j]
                        if (len(candidate) > 5 and not candidate.endswith(":") and
                                not re.match(r"^\d", candidate) and
                                not re.match(r"^[A-Z]-?\d+", candidate) and
                                candidate.lower() not in _MAILER_SKIP):
                            result["mailer_name"] = candidate
                        break
                    break

        # --- Quantity ---
        result["requested_quantity"] = 0
        result["availability_rule"] = "Nth"
        for i, ln in enumerate(lines):
            if "Quantity:" in ln:
                for j in range(max(0, i - 2), min(len(lines), i + 5)):
                    m = re.search(r"([\d,]+)", lines[j])
                    if m:
                        val = int(m.group(1).replace(",", ""))
                        if val >= 50:
                            result["requested_quantity"] = val
                            break
                break
        if re.search(r"ALL\s+AVAILABLE", text, re.IGNORECASE):
            result["availability_rule"] = "All Available"

        # --- Client P.O. ---
        result["mailer_po"] = ""
        for i, ln in enumerate(lines):
            if "Client P.O.:" in ln:
                for j in range(i, i + 3):
                    if j < len(lines):
                        candidate = lines[j].replace("Client P.O.:", "").strip(", ")
                        if re.match(r"^[A-Z0-9-]{4,10}$", candidate):
                            result["mailer_po"] = candidate
                            break
                break

        # --- List Name ---
        result["list_name"] = ""
        for i, ln in enumerate(lines):
            if ln.startswith("List:"):
                # Check forward (table layout)
                for j in range(i + 1, i + 4):
                    if j < len(lines):
                        candidate = lines[j].strip(", ")
                        if len(candidate) > 5 and not candidate.endswith(":"):
                            result["list_name"] = candidate
                            break
                # Fallback backward
                if not result["list_name"]:
                    for j in range(i - 1, max(0, i - 4), -1):
                        candidate = lines[j].strip(", ")
                        if len(candidate) > 5 and not candidate.endswith(":"):
                            result["list_name"] = candidate
                            break
                break

        # --- Key Code ---
        _KEY_EXCLUDE = {
            "Managed", "Active", "Fax", "E-Mail", "FTP Transfer",
            "Call With Count before Shipping", "Nth Cross Section", "M", "/",
        }
        result["key_code"] = ""
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

        # --- Segment Criteria ---
        result["segment_criteria"] = ""
        way_bill_idx = -1
        for i, ln in enumerate(lines):
            if ln == "Way Bill #:":
                way_bill_idx = i
                break
        if way_bill_idx >= 0:
            for j in range(way_bill_idx + 1, min(way_bill_idx + 8, len(lines))):
                candidate = lines[j]
                if (not candidate.endswith(":") and len(candidate) > 4 and
                        re.search(r"\$|\bmonth|\bdonor|\bomit|\bselect|\+", candidate, re.IGNORECASE)):
                    result["segment_criteria"] = candidate
                    break

        # --- Requestor Contact ---
        # Generic: find email near an "Email:" label (works for both RKD and AMLC formats).
        result["requestor_name"] = ""
        result["requestor_email"] = ""
        for i, ln in enumerate(lines):
            if ln == "Email:" and i > 20:
                for j in range(max(0, i - 3), i):
                    m_email = re.search(r"([\w.+-]+@[\w.-]+\.\w+)", lines[j])
                    if m_email:
                        result["requestor_email"] = m_email.group(1)
                        if j > 0 and re.match(r"^[A-Z][a-z]", lines[j - 1]):
                            result["requestor_name"] = lines[j - 1]
                        break
                break

        # --- Ship To email ---
        result["ship_to_email"] = ""
        m_ship = re.search(r"Email\s+(?:file\s+)?to:\s*([\w.+-]+@[\w.-]+\.\w+)", text, re.IGNORECASE)
        if m_ship:
            result["ship_to_email"] = m_ship.group(1)

        # --- Shipping method ---
        if re.search(r"\bFTP\b", text, re.IGNORECASE):
            result["shipping_method"] = "FTP"
        elif re.search(r"E-?Mail", text, re.IGNORECASE):
            result["shipping_method"] = "Email"
        else:
            result["shipping_method"] = ""

        # --- Omission ---
        result["omission_description"] = ""
        m_omit = re.search(r"(?:[Oo]mit|OMIT)[ \t:]+(.+?)(?:\n\n|\r\n\r\n|$)", text, re.DOTALL)
        if m_omit:
            result["omission_description"] = m_omit.group(0).strip()

        return result

    def parse(self, text: str) -> ParseResult:
        r = self._parse_columnar(text)
        requestor_email = r["requestor_email"]
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
            requestor_email=requestor_email,
            ship_to_email=r["ship_to_email"],
            key_code=r["key_code"],
            availability_rule=r["availability_rule"],
            file_format=self._detect_file_format(text),
            shipping_method=r["shipping_method"],
            shipping_instructions=f"CC: {requestor_email}" if requestor_email else "",
            omission_description=r["omission_description"],
            other_fees=self._detect_state_omits(r["omission_description"]),
            special_seed_instructions=self._extract_special_seed_instructions(text),
            segment_criteria=r["segment_criteria"],
        )


class AmlcParser(RkdGroupParser):
    broker_key: str = "amlc"

    def parse(self, text: str) -> ParseResult:
        r = self._parse_columnar(text)

        order_status = r.get("order_status", "")
        list_name = r.get("list_name", "")

        is_rental = bool(re.search(r"\brental\b", order_status, re.IGNORECASE))
        if not is_rental:
            is_rental = bool(re.search(r"\bVIGUERIE\b", list_name, re.IGNORECASE))

        billable_account = "T11" if is_rental else ""
        requestor_email = r["requestor_email"]

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
            requestor_email=requestor_email,
            ship_to_email=r["ship_to_email"],
            key_code=r["key_code"],
            availability_rule=r["availability_rule"],
            file_format=self._detect_file_format(text),
            shipping_method=r["shipping_method"],
            shipping_instructions=f"CC: {requestor_email}" if requestor_email else "",
            omission_description=r["omission_description"],
            other_fees=self._detect_state_omits(r["omission_description"]),
            special_seed_instructions=self._extract_special_seed_instructions(text),
            segment_criteria=r["segment_criteria"],
            billable_account=billable_account,
        )
