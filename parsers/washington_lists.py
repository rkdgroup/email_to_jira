"""Parser for Washington Lists broker PDF orders."""

import re
from parsers.base import BaseBrokerParser, CONFIDENCE_RULE_BASED
from parse_result import ParseResult


class WashingtonListsParser(BaseBrokerParser):
    broker_key: str = "washington_lists"

    def parse(self, text: str) -> ParseResult:
        """Parse Washington Lists rental/exchange order PDF text."""
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

        # --- Washington Lists Format ---
        # The PDF has a two-column layout:
        #   LEFT column: labels (Mailer, Offer, Mail Date, List, Segment, Format, Quantity, ...)
        #   RIGHT column: values (ANIMAL WELFARE, FUNDRAISING, 5/18/2026, ...)
        # After extraction, labels appear first, then values in the same order.
        #
        # Find the "Mailer" label (not "Mailer/List Broker...") in the data section

        # --- Find the label block start ---
        # The "Client" line starts the data section, followed by client address
        # Then the label block: Mailer, Offer, Mail Date, List, Segment, Format, Quantity
        label_start = -1
        for i, ln in enumerate(lines):
            # Look for standalone "Mailer" label (not the legal text "Mailer/List Broker...")
            if ln == "Mailer" and i > 30:
                label_start = i
                break

        if label_start < 0:
            # Fallback: look for it after the PLANO address block
            for i, ln in enumerate(lines):
                if "PLANO" in ln or "DALLAS" in ln:
                    for j in range(i + 1, min(i + 10, len(lines))):
                        if lines[j] == "Mailer":
                            label_start = j
                            break
                    break

        # --- Count labels to find where values start ---
        # Labels: Mailer, Offer, Mail Date, List, Segment, Format XXX, Quantity XXX, Select XXX, Shipping Fee
        # Some labels have values inline (e.g. "Format FTP", "Quantity 6650 All")
        label_count = 0
        label_end = label_start
        inline_values = {}

        # Known label names in Washington Lists
        known_labels = {"Mailer", "Offer", "Mail Date", "List", "Segment"}
        known_prefixes = ("Format", "Quantity", "Select", "Shipping")

        if label_start >= 0:
            for i in range(label_start, min(label_start + 15, len(lines))):
                ln = lines[i]
                if ln in known_labels:
                    label_count += 1
                    label_end = i
                elif any(ln.startswith(p) for p in known_prefixes):
                    # Check for inline values
                    if ln.startswith("Quantity"):
                        rest = ln.replace("Quantity", "").strip()
                        if rest:
                            inline_values["Quantity"] = rest
                    elif ln.startswith("Format"):
                        rest = ln.replace("Format", "").strip()
                        if rest:
                            inline_values["Format"] = rest
                    elif ln.startswith("Select"):
                        rest = ln.replace("Select", "").strip()
                        if rest:
                            inline_values["Select"] = rest
                    label_count += 1
                    label_end = i
                else:
                    break

            # Values start after the last label
            val_start = label_end + 1
            val_lines = lines[val_start:]

            # Map: values are in same order as labels
            # Label order: Mailer, Offer, Mail Date, List, Segment, ...
            # Value order: same (e.g., ANIMAL WELFARE, FUNDRAISING, 5/18/2026, ...)

        # --- Extract values ---
        mailer_name = ""
        offer = ""
        mail_date = ""
        list_name = ""
        segment = ""

        if label_start >= 0:
            # The first value after labels is Mailer value
            val_idx = 0
            label_order = []
            for i in range(label_start, label_end + 1):
                ln = lines[i]
                if ln == "Mailer":
                    label_order.append("Mailer")
                elif ln == "Offer":
                    label_order.append("Offer")
                elif ln == "Mail Date":
                    label_order.append("Mail Date")
                elif ln == "List":
                    label_order.append("List")
                elif ln == "Segment":
                    label_order.append("Segment")
                elif ln.startswith("Format"):
                    label_order.append("Format")
                elif ln.startswith("Quantity"):
                    label_order.append("Quantity")
                elif ln.startswith("Select"):
                    label_order.append("Select")
                elif ln.startswith("Shipping"):
                    label_order.append("Shipping")

            # Only pure labels (without inline values) need values from val_lines
            pure_labels = [l for l in label_order if l not in inline_values and
                           l not in ("Format", "Quantity", "Select", "Shipping")]

            val_lines_section = lines[label_end + 1:]
            for idx, label in enumerate(pure_labels):
                if idx < len(val_lines_section):
                    val = val_lines_section[idx]
                    if label == "Mailer":
                        mailer_name = val
                    elif label == "Offer":
                        offer = val
                    elif label == "Mail Date":
                        mail_date = self._normalize_date(val)
                    elif label == "List":
                        list_name = val
                    elif label == "Segment":
                        segment = val

        # --- Quantity ---
        requested_quantity = 0
        availability_rule = "Nth"
        if "Quantity" in inline_values:
            qty_text = inline_values["Quantity"]
            m = re.search(r"([\d,]+)", qty_text)
            if m:
                requested_quantity = int(m.group(1).replace(",", ""))
            if re.search(r"\ball\b", qty_text, re.IGNORECASE):
                availability_rule = "All Available"

        # --- Want By / Ship To ---
        ship_by_date = ""
        ship_to_email = ""
        for ln in lines:
            if ln.startswith("Want By"):
                rest = ln.replace("Want By", "").strip()
                dm = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", rest)
                if dm:
                    ship_by_date = self._normalize_date(dm.group(1))
            if ln.startswith("Ship To"):
                rest = ln.replace("Ship To", "").strip()
                m = re.search(r"([\w.+-]+@[\w.-]+\.\w+)", rest)
                if m:
                    ship_to_email = m.group(1)

        # --- Key code ---
        key_code = ""
        select_match = re.search(r"SELECT\s*ELEMENT:\s*(\S+)", text)
        if select_match:
            key_code = select_match.group(1)

        # --- Client Reference (mailer_po) ---
        mailer_po = ""
        # At the bottom: Order Number, Order Date, Client Reference, Contact, ...
        # Values follow in order
        for i, ln in enumerate(lines):
            if ln == "Client Reference":
                # Value is a few lines after
                for j in range(i + 1, min(i + 8, len(lines))):
                    if re.match(r"^\d{4,}$", lines[j]):
                        mailer_po = lines[j]
                        break
                break

        # --- Order Number ---
        manager_order_number = ""
        for i, ln in enumerate(lines):
            if ln == "Order Number":
                for j in range(i + 1, min(i + 8, len(lines))):
                    if re.match(r"^D\d{2}-\d+$", lines[j]):
                        manager_order_number = lines[j]
                        break
                break
        if not manager_order_number:
            m = re.search(r"(D\d{2}-\d+)", text)
            if m:
                manager_order_number = m.group(1)

        # --- Contact info ---
        # Bottom section has labels then values in order:
        # Order Number, Order Date, Client Reference, Contact, Contact Phone, Contact Email
        # D02-108563, 3/6/2026, 18856, REGGIE, (703)749-3127, rgwira@washingtonlists.com
        contact_name = ""
        contact_email = ""
        # Find "Contact Email" label and get its corresponding value
        contact_email_idx = -1
        contact_idx = -1
        for i, ln in enumerate(lines):
            if ln == "Contact Email":
                contact_email_idx = i
            elif ln == "Contact" and "Phone" not in ln and "Email" not in ln:
                contact_idx = i

        # Find the value for Contact Email by looking at the same offset from Order Number
        order_number_idx = -1
        for i, ln in enumerate(lines):
            if ln == "Order Number":
                order_number_idx = i
                break

        if order_number_idx >= 0 and contact_email_idx >= 0:
            # Labels are at order_number_idx to contact_email_idx
            # Values start after contact_email_idx
            label_count_bottom = contact_email_idx - order_number_idx + 1
            val_start_bottom = contact_email_idx + 1
            # Contact offset from Order Number
            if contact_idx >= 0:
                contact_offset = contact_idx - order_number_idx
                email_offset = contact_email_idx - order_number_idx
                if val_start_bottom + contact_offset < len(lines):
                    contact_name = lines[val_start_bottom + contact_offset]
                if val_start_bottom + email_offset < len(lines):
                    contact_email = lines[val_start_bottom + email_offset]

        requestor_name = contact_name
        requestor_email = contact_email

        # --- Shipping notification email ---
        ship_notify_email = ""
        notify_match = re.search(
            r"EMAIL\s+SHIPPING\s+NOTIFICATION\s+TO[:\s]+([\w.+-]+@[\w.-]+\.\w+)",
            text, re.IGNORECASE
        )
        if notify_match:
            ship_notify_email = notify_match.group(1)
        if not ship_to_email and ship_notify_email:
            ship_to_email = ship_notify_email

        # --- List manager ---
        list_manager = "WASHINGTON LISTS"

        # --- Shipping method ---
        ship_via = ""
        for ln in lines:
            m = re.search(r"via\s+(FTP|Email|E-mail)", ln, re.IGNORECASE)
            if m:
                ship_via = m.group(1)
                break
        if not ship_via and "Format" in inline_values:
            ship_via = inline_values["Format"]
        shipping_method = self._map_shipping_method(ship_via)

        # --- Shipping instructions = CC: requestor_email ---
        shipping_instructions = f"CC: {requestor_email}" if requestor_email else ""

        # --- File format ---
        file_format = ""

        # --- Omission ---
        omission_description = ""
        omit_match = re.search(r"OMIT[ \t:]+(.+?)(?:\n|$)", text, re.IGNORECASE)
        if omit_match:
            omission_description = omit_match.group(1).strip()

        # --- Other fees: auto-detect State Omits ---
        other_fees = self._detect_state_omits(omission_description)

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
            segment_criteria=segment,
        )

