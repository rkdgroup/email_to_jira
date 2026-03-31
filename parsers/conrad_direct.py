"""Parser for Conrad Direct broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class ConradDirectParser(BaseBrokerParser):
    broker_key: str = "conrad_direct"

    def _clean_conrad_text(self, text: str) -> str:
        """Cleans excess whitespace and line breaks common in Conrad PDF tables."""
        if not text:
            return ""
        # Replace newlines with spaces and collapse multiple spaces
        text = re.sub(r'\s*\n\s*', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    def parse(self, text: str) -> ParseResult:
        """Parse Conrad Direct purchase order PDF text."""

        # 1. Basic Identifiers
        po_number = self._find(text, r"PURCHASE\s*ORDER\s*NO:\s*(\S+)")
        order_date_raw = self._find(text, r"PURCHASE\s*ORDER\s*NO:\s*\S+\s+(\d{1,2}/\d{1,2}/\d{2,4})")
        order_date = self._normalize_date(order_date_raw)

        # 2. Multi-line Header Fields
        # Captures until the next ALL-CAPS header (e.g., OFFER:, PACKAGE:)
        mailer_raw = self._find(text, r"MAILER:\s*([\s\S]+?)(?=\n\s*[A-Z]{3,}:|$)")
        mailer_name = self._clean_conrad_text(mailer_raw)

        # Defined missing list_manager (usually the recipient of the PO)
        list_manager = self._find(text, r"To:\s*([\s\S]+?)(?=\n\s*[A-Z]{3,}:|$)") or "Conrad Direct"

        # 3. Dates and POs
        brok_mail_po = self._find(text, r"BROK/MAIL\s*PO:\s*(\S+)")
        mail_date_raw = self._find(text, r"MAIL\s*DATE:\s*(\d{1,2}/\d{1,2}/\d{2,4})")
        mail_date = self._normalize_date(mail_date_raw)
        ship_by_raw = self._find(text, r"NEEDED\s*BY:?\s*(\d{1,2}/\d{1,2}/\d{2,4})")
        ship_by_date = self._normalize_date(ship_by_raw)

        # 4. List and Quantity Extraction
        list_raw = self._find(text, r"LIST:\s*([\s\S]+?)(?=\n\s*[\d,]+\s+|$)")
        list_name = self._clean_conrad_text(list_raw)

        requested_quantity = 0
        segment = ""
        # Look for the specific pattern of Qty followed by Segment text
        qty_pattern = r"LIST:[\s\S]+?\n\s*([\d,]+)\s+([\s\S]+?)(?=\n\s*(?:Base Price|[A-Z]{3,}:)|$)"
        qty_match = re.search(qty_pattern, text)
        if qty_match:
            requested_quantity = int(qty_match.group(1).replace(",", ""))
            segment = self._clean_conrad_text(qty_match.group(2))

        # 5. Availability Rule
        availability_rule = "Nth"
        if re.search(r"\*FULL\s*RUN\*|ALL\s+AVAILABLE", text, re.IGNORECASE):
            availability_rule = "All Available"

        # 6. Key Code and Contact
        # Conrad often puts key codes in "Special Instructions" or "Material"
        key_code = ""
        key_match = re.search(r"(?:PO#\s+\S+\s+(?:And|&)\s+)([\w\s#]+)(?=\n|$)", text, re.IGNORECASE)
        if key_match:
            key_code = self._clean_conrad_text(key_match.group(1))

        requestor_email = ""
        requestor_name = ""
        contact_match = re.search(r"CONTACT:\s*([^\n]+)\n.*([\w.+-]+@[\w.-]+\.\w+)", text)
        if contact_match:
            requestor_name = contact_match.group(1).strip()
            requestor_email = contact_match.group(2).strip()

        # 7. Shipping and Omissions
        ship_to_email = self._find(text, r"Please\s*Email\s*Names\s*To:\s*([\w.+-]+@[\w.-]+\.\w+)")
        omission_description = self._clean_conrad_text(
            self._find(text, r"((?:Please\s+)?[Oo]mit\s+[\s\S]+?)(?=\n\s*[A-Z]{3,}:|\n\n|$)")
        )

        return ParseResult(
            source=f"rule:{self.broker_key}",
            confidence=CONFIDENCE_RULE_BASED,
            mailer_name=mailer_name,
            mailer_po=brok_mail_po or po_number,
            list_name=list_name,
            list_manager=self._clean_conrad_text(list_manager),
            requested_quantity=requested_quantity,
            manager_order_number=po_number,
            mail_date=mail_date,
            ship_by_date=ship_by_date,
            requestor_name=requestor_name,
            requestor_email=requestor_email,
            ship_to_email=ship_to_email,
            key_code=key_code,
            availability_rule=availability_rule,
            shipping_instructions=f"CC: {requestor_email}" if requestor_email else "",
            omission_description=omission_description,
            segment_criteria=segment,
        )