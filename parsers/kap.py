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
        # DL and DM are the same slot: the line under "KAP Order:" holds DL### on most
        # orders and DM### on others (DSLF-1092 DM009, DSLF-1100 DM022 both parsed blank
        # while a DL-only regex was here, which sent the mailer_po into the title via
        # ParseResult's fallback and left Seed Tracking Number empty). Measured over 22
        # KAP order PDFs that line carried only those two prefixes.
        manager_order_number = ""
        m = re.search(r"(D[LM]\d+)", text)
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

        # --- Mailer PO = BROKER ORDER # (e.g. 129214, E12316, or alphanumeric CJZ47),
        # NOT the DL number (which goes in manager_order_number and the title only). ---
        # In KAP's two-column layout the values for the Broker block appear together
        # after the "MAIL DATE" label, in the order:
        #   broker_name, [broker_sb], BROKER_ORDER#, wanted_by_date, mail_date
        # The order # is the LAST digit-bearing token before the first date: this skips
        # the broker name (no digits) and a leading S/B (which precedes the order #), and
        # unlike a digits-only match it also captures alphanumeric order #s like CJZ47
        # (DSLF-862, DL786) that would otherwise leave Mailer PO blank.
        mailer_po = ""
        mail_date = ""
        ship_by_date = ""

        # Find "MAIL DATE" label (standalone, not "MAIL DATE:")
        mail_date_label_idx = -1
        for i, ln in enumerate(lines):
            if ln.upper().rstrip(":") == "MAIL DATE":
                mail_date_label_idx = i
                break

        if mail_date_label_idx >= 0:
            broker_vals = lines[mail_date_label_idx + 1:]
            dates_found = []
            for ln in broker_vals[:10]:
                if re.match(r"^(\d{2}/\d{2}/\d{2,4})$", ln):
                    dates_found.append(ln)
                    continue
                if ln.endswith(":"):
                    break  # reached the next label block — stop scanning
                # Broker order #: a value carrying a digit that appears before the first
                # date. Keep the last such value so a leading S/B is overwritten by the
                # order # that follows it. Internal spaces are allowed because brokers do
                # write the order # with one ("CRU 924-105" on DL995, which a single-token
                # match left blank); the length and word-count caps keep a prose line out.
                if (not dates_found and re.search(r"\d", ln) and len(ln) <= 30
                        and re.match(r"^[A-Za-z0-9][A-Za-z0-9 #/.-]*$", ln)
                        and len(ln.split()) <= 3):
                    mailer_po = ln.strip()

            # First date after MAIL DATE label = WANTED BY (ship_by_date), second = MAIL DATE
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
                # Stop at the price column, but only on a line that is *nothing but* a
                # price ("$ 95.00 /M", "$0.00/M", "$100.00"). A criterion may legitimately
                # open with a dollar floor — "$10+ LAST 12 MO" (DL995) was being read as a
                # price and the whole selection dropped.
                if (ln.endswith(":") or re.match(r"^\d+\.\d{2}", ln)
                        or re.fullmatch(r"\$\s*[\d,]+(?:\.\d{2})?\s*(?:/\s*M)?", ln)):
                    break
                if len(ln) > 3:
                    segment_criteria = ln
                    break

        # --- QTY + AVAILABILITY ---
        # Both live in one block, labelled "Rental Qty:" on a rental and "Exch Qty:" on an
        # exchange (DL997 is an exchange, which left DSLF-1071's quantity blank). The
        # availability is the block's own "All available" / "Nth select" value: reading it
        # from the whole page matched the KAP boilerplate "Please provide the all available
        # quantity before shipping for approval.", which turned DL997's 5,000 Nth select
        # into All Available. Same trap as the FTP boilerplate in c7e9751 — a sentence
        # offering something is not the order asking for it.
        # The quantity is a line holding nothing but the number, so a price inside the
        # window ("$100.00") cannot be read as the quantity.
        requested_quantity = 0
        availability_rule = "Nth"
        qty_m = re.search(r"(?:RENTAL|EXCH(?:ANGE)?)\s*QTY:([\s\S]{0,160})", text, re.IGNORECASE)
        if qty_m:
            qty_block = qty_m.group(1)
            num_m = re.search(r"(?m)^[ \t]*([\d,]{3,})[ \t]*$", qty_block)
            avail_scope = qty_block
            if num_m:
                requested_quantity = int(num_m.group(1).replace(",", ""))
                # The availability value is printed directly under the number, so stop
                # there rather than letting the window run on into the page's prose.
                avail_scope = "\n".join(qty_block[num_m.end():].splitlines()[:4])
            if re.search(r"All\s+available", avail_scope, re.IGNORECASE):
                availability_rule = "All Available"

        # --- List manager = broker (KAP) ---
        list_manager = "KAP"

        # --- Contact info: KAP's own rep appears as "Please contact NAME at Email: EMAIL" ---
        # Fallback: any @keyacquisition.com email in the text (e.g. email-only orders)
        requestor_name = ""
        requestor_email = ""
        # Three variants of the same sentence, all seen on live orders:
        #   "Please contact Robin Wojack at Email: rwojack@keyacquisition.com"
        #   "Please contact Robin Wojack @ Email:  rwojack@..."   (DSLF-1152)
        #   "contact Robin Wojack at Email: rwojack@..."          (DSLF-1141, no "Please")
        # An "at"-only, Please-required pattern missed the latter two, leaving Requestor
        # Name blank while the keyacquisition fallback below still found the address.
        #
        # The captured address is pinned to KAP's own domain rather than any address. That
        # is the requestor rule this parser already states further down -- the requestor is
        # the LIST MANAGER's contact, never the mailer's -- and it is what makes dropping
        # "Please" safe: DSLF-1141 also carries "contact eftaccountsetup@igxfer.com" for
        # FTP setup, which a domain-agnostic pattern could have taken instead.
        m = re.search(r"(?:Please\s+)?contact\s+(.+?)\s*(?:\bat\b|@)\s*(?:Email:)?\s*"
                      r"([\w.+-]+@keyacquisition(?:partners)?\.com)", text, re.IGNORECASE)
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
        # KAP house requestor when the order names no KAP contact (all recent KAP tickets
        # use Jenny Gomez; DSLF-936/DL870 had only Data Axle addresses and came out blank).
        # Never accept a non-KAP address (e.g. @data-axle.com) as the requestor.
        if not requestor_email:
            requestor_name = "Jenny Gomez"
            requestor_email = "jgomez@keyacquisition.com"

        # --- Ship To block ---
        # ONLY the Ship To block decides the destination, the delivery method and the format.
        # A broker, list manager or contact from a processing house named elsewhere on the
        # order is irrelevant: DSLF-1022 was brokered by Data-Axle and shipped by plain
        # email, and reading the wider page turned it into an FTP order.
        _ship_block = ""
        _m_block = re.search(r"Ship\s*-?\s*To\s*:(.{0,260})", text, re.IGNORECASE | re.DOTALL)
        if _m_block:
            _ship_block = _m_block.group(1)

        # --- Ship to email ---
        # Data Axle FTP orders name incoming.files@data-axle.com explicitly, and that is a
        # genuine destination rather than a contact. Otherwise take the address printed
        # inside the Ship To block. The page-wide "Email:" fallback is LAST because the
        # first such match is the mailer/broker contact — that is how the Data-Axle rep's
        # address reached ship_to_email on DSLF-1022 and the mailer's own contact reached it
        # on DSLF-1029. See also DSLF-802.
        # Everything from the Ship To label to the end of the page. _ship_block is capped at
        # 260 chars, which is right for reading the Via/format tokens that sit immediately
        # after the labels, but the destination can be much further down in prose — on
        # DSLF-1152 it was ~15 lines past the label. Searching for an address must never
        # look ABOVE this point: the first "Email:" on a KAP order is the mailer/broker
        # contact, and reading the whole page is precisely how BCRABTREE@RKDGROUP.COM (the
        # broker's own rep) became the ship-to on DSLF-1152, LKA on DSLF-1022 and the
        # mailer's contact on DSLF-1029.
        _ship_tail = text[_m_block.start():] if _m_block else ""

        ship_to_email = ""
        m = re.search(r"(incoming\.files@data-axle\.com)", text, re.IGNORECASE)
        if not m:
            m = re.search(r"([\w.+-]+@[\w.-]+\.\w+)", _ship_block)
        if not m:
            # "Email to:", "send an email to X and Y", "email the file to X". The FIRST
            # address is the destination; a second one after "and" is the broker being
            # copied, which belongs in Shipping Instructions rather than Ship To.
            m = re.search(r"e-?mail\s+(?:the\s+\w+\s+)?to:?[^\n@]*?([\w.+-]+@[\w.-]+\.\w+)",
                          _ship_tail, re.IGNORECASE)
        if not m:
            m = re.search(r"Email:\s*([\w.+-]+@[\w.-]+\.\w+)", _ship_tail, re.IGNORECASE)
        if m:
            ship_to_email = m.group(1)

        # --- Shipping method ---
        # Read the Via / Ship To value, not the page. KAP boilerplate says "if you are
        # unable to email records, please place on your FTP site" on orders that are plain
        # email, so a bare "FTP" anywhere in the document is not evidence of an FTP order.
        # Take whichever delivery token appears FIRST in the block: the Ship To values sit
        # immediately after the labels, ahead of any prose further down.
        shipping_method = ""
        _m_via = re.search(r"\b(F\.?\s*T\.?\s*P\.?|E-?\s*MAIL|IN\s*-?\s*HOUSE)\b",
                           _ship_block, re.IGNORECASE)
        if _m_via:
            _tok = re.sub(r"[^A-Z]", "", _m_via.group(1).upper())
            shipping_method = {"FTP": "FTP", "EMAIL": "Email"}.get(_tok, "Other")
        elif re.search(r"\bE-?mail\b", text, re.IGNORECASE):
            # No Ship To block found at all — fall back to the old page-wide read, but only
            # for Email. Never infer FTP from the page.
            shipping_method = "Email"

        # Saturn Corp destination (order instructs loading to the Saturn FileShare) is always
        # ASCII Fixed via FTP, even though the ship-to email here is only a notify address.
        file_format = ""
        if self._is_saturn_order(text):
            file_format = "ASCII Fixed"
            shipping_method = "FTP"

        # FTP orders: the ship-to is a notification address, not an email delivery.
        if shipping_method == "FTP" and ship_to_email and not ship_to_email.upper().startswith("FTP NOTIFY:"):
            ship_to_email = f"FTP NOTIFY: {ship_to_email}"

        # --- Upload destination ---
        # An FTP order that names no address inside the Ship To block states its
        # destination in prose instead: DL995 reads "upload file to:" with the URL on the
        # next line, and the ticket ended up carrying a notify address and no destination.
        # Shipping Instructions is a single-line text field, so only the target goes here.
        upload_target = ""
        m_up = re.search(r"(?is)\b(?:upload|post|send)[ \t]+(?:the[ \t]+)?file[ \t]+to[ \t]*:[ \t\r\n]*(\S+)",
                         text)
        if m_up:
            upload_target = m_up.group(1).strip().rstrip(".,;")

        shipping_instructions = f"CC: {requestor_email}" if requestor_email else ""
        if upload_target:
            _upload = f"UPLOAD TO: {upload_target}"
            shipping_instructions = f"{shipping_instructions} | {_upload}" if shipping_instructions else _upload

        # --- Omission ---
        # Collect the real omit/suppress directives (deduped, in order). The old
        # single-anchor approach grabbed whatever line first contained "Omit" — which on
        # DL870 was the SELECTION line ("12mo ... WITH Omit States - SEE BELOW") and on
        # other orders left a stray "executed." paragraph prefix. Capture instead:
        #   1. Starred instruction lines: "*** Omit States AK, AL, ..." (drives State Omits)
        #   2. Standalone "Omit ...:" / "Omit ..." directive lines (e.g. "Omit: ALL RECORDS ..."),
        #      excluding the pointer "Omit States - SEE BELOW"
        #   3. The embedded boilerplate directive, captured from "Omit all" so the wrapped
        #      paragraph prefix (e.g. "executed.") is dropped.
        omit_lines: list[str] = []

        def _add_omit(s: str) -> None:
            s = re.sub(r"(?i)^omit[ \t]*:[ \t]*", "", s.strip().lstrip("*").strip()).strip()
            if s and s not in omit_lines:
                omit_lines.append(s)

        for m in re.finditer(r"(?m)^[ \t]*\*+[ \t]*(Omit\b.+?)[ \t]*$", text):
            _add_omit(m.group(1))
        for ln in lines:
            if re.match(r"(?i)^omit[ \t:]+\S", ln) and not re.search(r"(?i)\bsee below\b", ln):
                _add_omit(ln)
        #   4. Directives that never say "omit": DL984 carries "Please exclude states MN,
        #      MS, and NC" on its own line, which reached neither field.
        for ln in lines:
            if re.match(r"(?i)^(?:please[ \t]+)?exclude[ \t]+\S", ln):
                _add_omit(ln)
        m = re.search(r"(Omit\s+all\s+APO\b[^\n.]*)", text, re.IGNORECASE)
        if m:
            _add_omit(m.group(1))
        omission_description = "\n".join(omit_lines)

        # --- Other fees: auto-detect State Omits (6+ states/zips/SCFs) ---
        other_fees = self._detect_state_omits(omission_description)

        # --- Segment criteria ---
        # Fall back to explicit Selects: label if unlabeled line wasn't found
        if not segment_criteria:
            segment_criteria = self._find(text, r"(?:Selects?|Segment):[ \t]*([^\n]+)")

        # Drop offer-date noise mis-grabbed as the key from a wrapped offer line, e.g.
        # offer "Lutheran Hour Ministries November 2026" leaves key_code "2026" or
        # "September 2026". A bare month name alone is left as-is (could be a real code).
        _MONTH = (r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
                  r"jul(?:y)?|aug(?:ust)?|sep(?:t)?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)")
        if re.fullmatch(rf"(?i)(?:{_MONTH}\s+)?(?:19|20)\d{{2}}", (key_code or "").strip()):
            key_code = ""

        # Summary is deliberately left unset: ParseResult.__post_init__ builds the
        # LIST NAME - MAILER NAME - MANAGER ORDER NUMBER title every other broker uses.
        # Setting it here suppressed that and titled 64 KAP tickets "P.O. {DL#} {list}".
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
