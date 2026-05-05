"""Parser for We Are Moore broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult

_REQUESTOR_NAME  = "MICHELLE NAY"
_REQUESTOR_EMAIL = "MNAY@WEAREMOORE.COM"


class WeAreMooreParser(BaseBrokerParser):
    broker_key: str = "we_are_moore"

    def parse(self, text: str) -> ParseResult:
        # Manager Order # = Order#
        manager_order_number = (
            self._find(text, r"Order\s*#[:\s]+(\S+)")
            or self._find(text, r"Order\s+Number[:\s]+(\S+)")
        )

        # Mailer PO = Ship Label number (W-prefix)
        mailer_po = (
            self._find(text, r"Ship\s+Label\s*#?[:\s]+(W\d+\S*)")
            or self._find(text, r"\b(W\d{4,}\w*)\b")
        )

        # Mailer Name
        mailer_name = (
            self._find(text, r"Mailer[:\s]+([^\n]+)")
            or self._find(text, r"Client[:\s]+([^\n]+)")
        )

        # List Name
        list_name = (
            self._find(text, r"List\s+Name[:\s]+([^\n]+)")
            or self._find(text, r"List[:\s]+([^\n]+)")
        )

        # Quantity and availability
        qty_raw = (
            self._find(text, r"Quantity[:\s]+([\d,]+(?:\s+or\s+all\s+available)?)", group=1)
            or self._find(text, r"Qty[:\s]+([\d,]+(?:\s+or\s+all\s+available)?)", group=1)
        )
        if qty_raw:
            requested_quantity, availability_rule = self._find_quantity(text, r"(?:Quantity|Qty)[:\s]+([\d,]+(?:\s+or\s+all\s+available)?)")
        else:
            requested_quantity = 0
            availability_rule = "Nth"

        if re.search(r"Full\s+Run|All\s+Available", text, re.IGNORECASE):
            availability_rule = "All Available"

        # Dates
        mail_date  = self._find_date(text, r"Mail\s+Date[:\s]+(\d{1,2}/\d{1,2}/\d{2,4})")
        ship_by_date = (
            self._find_date(text, r"(?:Ship\s+(?:By|Date)|Due\s+Date|Needed\s+By)[:\s]+(\d{1,2}/\d{1,2}/\d{2,4})")
        )

        # Ship-to email
        ship_to_email = self._find(text, r"(?:Email|Send\s+To|Deliver\s+To)[:\s]+([\w.+-]+@[\w.-]+\.\w+)")

        # Shipping method
        shipping_method = self._map_shipping_method(
            self._find(text, r"(?:Delivery|Ship(?:ping)?\s+Method|Via)[:\s]+([^\n]+)")
        )
        if not shipping_method:
            if re.search(r"\bFTP\b", text):
                shipping_method = "FTP"
            elif re.search(r"\bemail\b", text, re.IGNORECASE):
                shipping_method = "Email"

        # File format
        file_format = self._detect_file_format(text)

        # Key code
        key_code = self._find(text, r"Key\s+Code[:\s]+([^\n]+)")

        # Omissions
        omission_description = self._find(text, r"(?:Omit|Suppress)[:\s]+([^\n]+(?:\n(?!\s*[A-Z]{2,}:)[^\n]+)*)")
        other_fees = self._detect_state_omits(omission_description)

        # Segment criteria
        segment_criteria = self._find(text, r"(?:Select(?:s|ion)?|Segment)[:\s]+([^\n]+)")

        # Special seed instructions
        special_seed_instructions = self._extract_special_seed_instructions(text)

        return ParseResult(
            source=f"rule:{self.broker_key}",
            confidence=CONFIDENCE_RULE_BASED,
            mailer_name=mailer_name.strip() if mailer_name else "",
            mailer_po=mailer_po.strip() if mailer_po else "",
            list_name=list_name.strip() if list_name else "",
            list_manager="WE ARE MOORE",
            requested_quantity=requested_quantity,
            manager_order_number=manager_order_number.strip() if manager_order_number else "",
            mail_date=mail_date,
            ship_by_date=ship_by_date,
            requestor_name=_REQUESTOR_NAME,
            requestor_email=_REQUESTOR_EMAIL,
            ship_to_email=ship_to_email,
            key_code=key_code,
            availability_rule=availability_rule,
            file_format=file_format,
            shipping_method=shipping_method,
            shipping_instructions=f"CC: {_REQUESTOR_EMAIL}",
            omission_description=omission_description,
            other_fees=other_fees,
            special_seed_instructions=special_seed_instructions,
            segment_criteria=segment_criteria,
        )
