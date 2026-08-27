"""
SELECT PDF text parser.

What survived the removal of the rule-based QC checker: the regexes that read a SELECT
report's own printed values. This is NOT quality control — all QC is now the LLM's job in
`qc_llm.py`, which reads the PDF directly. These functions exist for the two callers that
need a specific number off a SELECT rather than a judgement about it:

  qty_approval_scanner.py  TOTAL RECORDS SELECTED, as the quantity fallback
  qc_llm.py                find_select_attachment(), to pick which PDF to send

Do not grow a comparison back in here. A value read off the report belongs in a prompt as
evidence, not in an if-statement that decides PASS or FAIL.
"""

import re
import logging
from pathlib import Path

_ROOT = Path(__file__).parent
log = logging.getLogger(__name__)

def _normalize_date(raw: str) -> str:
    """Convert M/D/YY or M/D/YYYY to YYYY-MM-DD. Returns '' on failure."""
    raw = raw.strip()
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{2,4})$', raw)
    if m:
        month, day, year = m.groups()
        if len(year) == 2:
            year = f"20{year}"
        return f"{year}-{int(month):02d}-{int(day):02d}"
    return ""



_CRITERIA_LINE = re.compile(r'^\s*CRITERIA\b[\s.:]*\d', re.IGNORECASE)

# Omit-type header keywords. ADSTRA spells them either "OMIT <TYPE>" or "<TYPE> OM."
# and abbreviates FLAG as FLG — match both forms so a variant header is not skipped.
_OMIT_FLAG_HDR  = r'(?:OMIT\s+FL(?:AG|G)S?|FL(?:AG|G)S?\s+OM)'
_OMIT_STATE_HDR = r'(?:OMIT\s+STATES?|STATES?\s+OM)'
_OMIT_ZIP_HDR   = r'(?:OMIT\s+ZIPS?|ZIPS?\s+OM)'


# Include-set line from the account-history table at the top of the SELECT, e.g.
#   "  INCLUDE BY ACCOUNT #:    30,311   S00N11DIH1   009999   10-99.99 L3M   07'01  19:14:52 07/07/2026"
# Groups: count, member, job id#, then TITLE + run-time tail (split by _RUNTIME_TAIL).
# The TITLE carries the universe the select was drawn from ("10-99.99 L3M") — the
# second, independent statement of the order's criteria alongside the REPORT line.
# The bare "DE BY ACCOUNT" alternative catches a truncated INCLUDE (seen in extracted
# text); the lookbehind keeps it from firing on the tail of the full word, and neither
# form matches the "SUPPRESS BY ACCOUNT #:" lines that share this table.
_INCLUDE_LINE = re.compile(
    r'(?:INCLUDE|(?<![A-Z])DE)\s+BY\s+ACCOUNT\s*#?\s*:\s*([\d,]+)\s+(\S+)\s+(\S+)\s+(.+?)\s*$',
    re.IGNORECASE)

# Trailing "07'01   19:14:52 07/07/2026" (the leading MM'DD stamp is not always present).
_RUNTIME_TAIL = re.compile(
    r"\s*(?:\d{1,2}'\d{2}\s+)?\d{1,2}:\d{2}:\d{2}\s+\d{1,2}/\d{1,2}/\d{2,4}\s*$")



def _criteria_tokens(s: str) -> tuple[list[str], list[str]]:
    """
    Pull ($-amount, L#M) criteria tokens out of a criteria string.

    Handles all three forms seen in the wild:
      "$10-99.99"  (KAP range, $ present)   -> ["$10-99.99"]
      "10-99.99"   (INCLUDE title, bare)    -> ["$10-99.99"]
      "$5+" / "05+"                          -> ["$5+"]

    A BARE range must have decimals on the high side. Without that guard a
    SUPPRESS title like "4-6 LINE ADDRESSES" parses as the money range "$4-6".

    Thresholds have two traps, both seen on live selects:
      * "05+ L03 60+" — the trailing "60+" is the 60 PLUS ASSOCIATION mailer code,
        not $60. The dollar criterion is stated first, so when nothing in the
        string carries a '$' only the FIRST bare threshold is taken; when any
        threshold IS '$'-prefixed the bare ones are mailer shorthand and dropped.
      * "0.01+" — decimals were being truncated to "$1+". A floor of a cent is
        "no minimum" rather than a criterion, so it is not emitted at all.
    """
    def _dnum(x: str) -> str:
        return str(int(float(x))) if float(x) == int(float(x)) else x

    dollar = [f"${_dnum(lo)}-{_dnum(hi)}"
              for lo, hi in re.findall(r'\$\s*(\d+(?:\.\d+)?)\s*-\s*\$?\s*(\d+(?:\.\d+)?)', s)]
    dollar += [f"${_dnum(lo)}-{_dnum(hi)}"
               for lo, hi in re.findall(r'(?<![\d.$])(\d+)\s*-\s*(\d+\.\d+)(?![\d.])', s)]

    thresholds = re.findall(r'(\$)?\s*(\d+(?:\.\d+)?)\s*\+', s)
    explicit   = [v for sign, v in thresholds if sign]
    chosen     = explicit if explicit else [v for _s, v in thresholds][:1]
    dollar    += [f"${_dnum(v)}+" for v in chosen if float(v) > 0.01]

    period = [f"L{int(n)}M" for n in re.findall(r'(?<![A-Za-z])L(\d+)M?', s, re.IGNORECASE)]

    # de-dupe, preserve order
    return list(dict.fromkeys(dollar)), list(dict.fromkeys(period))



_SELECT_FORMAT_MAP = {
    'ASCII COMMA DELIMITED': 'ASCII Delimited',
    'ASCII FIXED LENGTH':    'ASCII Fixed',
    'ASCII FIXED':           'ASCII Fixed',
    'EXCEL':                 'Excel',
}


def _iter_criteria_blocks(text: str):
    """
    Yield (header, value_lines) for every 'CRITERIA ...: N <label>' block.

    header      = the full criteria header line, e.g.
                  "CRITERIA ...:  6  OMIT FLAG $   EXCLUDED . 15 RECORDS ..."
    value_lines = the non-empty lines under it, up to the next CRITERIA header
                  (or end of text).

    Exposing every block (not just the first match) lets callers UNION an omit
    type that ADSTRA splits across several criteria — e.g. a general
    "OMIT FLAGS" block plus a dedicated "OMIT FLAG $" block on a later page.
    """
    header = None
    body: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if _CRITERIA_LINE.match(stripped):
            if header is not None:
                yield header, body
            header, body = stripped, []
        elif header is not None and stripped:
            body.append(stripped)
    if header is not None:
        yield header, body


def _collect_criteria_blocks(text: str, criteria_keyword: str) -> list[str]:
    """
    Value lines from EVERY criteria block whose header matches criteria_keyword,
    unioned in document order. Generalises the old first-match-only collector so
    an omit type spread across multiple CRITERIA blocks is fully captured.
    """
    lines: list[str] = []
    for header, body in _iter_criteria_blocks(text):
        if re.search(criteria_keyword, header, re.IGNORECASE):
            lines.extend(body)
    return lines



def find_select_attachment(attachments: list) -> tuple[dict | None, list[str]]:
    """
    Find the SELECT PDF from a list of Jira attachment dicts.
    Returns (attachment_or_None, warnings) where warnings is a list of
    human-readable strings to surface in the QC comment.
    """
    SELECT_RE = re.compile(r'(?<![A-Z])SELECT(?![A-Z])', re.IGNORECASE)
    matches = [a for a in attachments
               if SELECT_RE.search(a.get("filename", ""))
               and a.get("filename", "").lower().endswith(".pdf")]
    warnings: list[str] = []
    if not matches:
        return None, warnings
    if len(matches) > 1:
        names = [a["filename"] for a in matches]
        log.warning("Multiple SELECT PDFs found: %s — using most recent", names)
        matches.sort(key=lambda a: a.get("created", ""), reverse=True)
        others = ", ".join(a["filename"] for a in matches[1:])
        warnings.append(
            f"Multiple SELECT PDFs found — used most recent: {matches[0]['filename']}; "
            f"ignored: {others}"
        )
    return matches[0], warnings


# ---------------------------------------------------------------------------
# SELECT PDF parser
# ---------------------------------------------------------------------------

def _parse_shipping_info(text: str) -> dict:
    """Delivery destination from the NOTES block on the SELECT's last page.

        Email:  TO: <email>  and  CC: <email>
        FTP:    FILENAME: <name>.ZIP

    The filename is built from the Mailer PO and carries it verbatim, spaces included —
    "FILENAME: CRU 924-105.ZIP" on DSLF-1070. Matching it as a single whitespace-free
    token missed that, so QC reported "Shipping info not found" on an order whose SELECT
    states it plainly and skipped the shipping cross-checks without failing anything.

    Lives out here rather than inside parse_select_pdf so it is reachable without a PDF.
    """
    _email_re = r'[\w.\-]+@[\w.\-]+'
    m_to = re.search(r'\bTO\s*:\s*(' + _email_re + r')', text, re.IGNORECASE)
    m_cc = re.search(r'\bCC\s*:\s*(' + _email_re + r')', text, re.IGNORECASE)
    # Stay on the label's own line and stop at the first ".ZIP", so a bare FILENAME:
    # label cannot reach down the page for a value that is not there.
    m_fn = re.search(r'\bFILENAME\s*:[ \t]*([^\r\n]*?\.ZIP)\b', text, re.IGNORECASE)

    if m_to:
        return {"shipping_method": "Email",
                "ship_to_email":   m_to.group(1).strip().upper(),
                "cc_email":        m_cc.group(1).strip().upper() if m_cc else "",
                "ftp_filename":    "",
                "parse_errors":    []}
    if m_fn:
        return {"shipping_method": "FTP",
                "ship_to_email":   "",
                "cc_email":        "",
                "ftp_filename":    re.sub(r"\s+", " ", m_fn.group(1)).strip().upper(),
                "parse_errors":    []}
    return {"shipping_method": "",
            "ship_to_email":   "",
            "cc_email":        "",
            "ftp_filename":    "",
            "parse_errors":    ["Shipping info (TO:/CC:/FILENAME:) not found in SELECT PDF"]}


def parse_select_pdf(pdf_path: str) -> dict:
    """
    Extract QC-relevant fields from a SELECT PDF.
    Returns dict with keys: job_number, client_db, customer_name,
    manager_order, total_records, mailing_date, seed_db, flags,
    state_omits, zip_omits, parse_errors.
    """
    from tools_pdf import extract_pdf_text

    text = extract_pdf_text(pdf_path)
    if text.startswith("[ERROR"):
        return {"parse_errors": [f"PDF extraction failed: {text}"]}
    if text.startswith("[WARNING:LOW_TEXT]"):
        log.warning("Low text in SELECT PDF: %s", pdf_path)
        text = text[len("[WARNING:LOW_TEXT]"):].strip()

    result: dict = {"parse_errors": []}

    # Job line: JOB : W459261189 K40 D  ACCOUNT LIST FOR : KIDS WISH DATA MAIL INC.
    m = re.search(
        r'JOB\s*:\s*(\S+)\s+([\w\s]+?)\s+ACCOUNT\s+LIST\s+FOR\s*:\s*(.+)',
        text
    )
    if m:
        result["job_number"]    = m.group(1).strip()
        result["client_db"]     = re.sub(r'\s+', '', m.group(2)).upper()
        result["customer_name"] = m.group(3).strip()
    else:
        result["job_number"]    = ""
        result["client_db"]     = ""
        result["customer_name"] = ""
        result["parse_errors"].append("JOB line not found (client_db, job_number, customer_name)")

    # Manager order + criteria suffix from REPORT: P.O.# J0094 $5+L3M FLAG
    m = re.search(r'REPORT\s*:\s*P\.O\.#\s*([A-Z0-9]+)\s*(.*)', text, re.IGNORECASE)
    if m:
        result["manager_order"]  = m.group(1).strip()
        # The REPORT line runs to the page footer, so the raw tail carries
        # "PAGE :    1" and the column padding before it. Trim both — this value is
        # quoted verbatim into the QC comment a human reads.
        suffix = re.split(r'\s{2,}PAGE\s*:', m.group(2), 1)[0].strip()
        result["criteria_suffix"] = suffix
        # Dollar ranges ("$10-99.99", KAP style) and thresholds ("$5+" / "05+"), plus
        # time periods "L12M"/"L3M"/"L03" — see _criteria_tokens.
        result["dollar_criteria"], result["period_criteria"] = _criteria_tokens(suffix)
    else:
        result["manager_order"]    = ""
        result["criteria_suffix"]  = ""
        result["dollar_criteria"]  = []
        result["period_criteria"]  = []
        result["parse_errors"].append("REPORT/P.O.# line not found (manager_order, criteria)")

    # Total records selected. Track "found" separately from the value: a missing line
    # and a genuine 0 both used to collapse to 0, and the caller must tell them apart
    # (both fail, but for different reasons).
    m = re.search(r'TOTAL\s+RECORDS\s+SELECTED[\s.]*\s*([\d,]+)', text, re.IGNORECASE)
    if m:
        result["total_records"]       = int(m.group(1).replace(',', ''))
        result["total_records_found"] = True
    else:
        result["total_records"]       = 0
        result["total_records_found"] = False
        result["parse_errors"].append("TOTAL RECORDS SELECTED line not found")

    # Include set(s) — the universe the select was drawn from, from the account-history
    # table: "INCLUDE BY ACCOUNT #: 30,311  S00N11DIH1  009999  10-99.99 L3M  07'01 ...".
    # The TITLE ("10-99.99 L3M") states the order's criteria independently of the
    # REPORT line, so a mismatch against the ticket is a real QC finding.
    result["include_sets"] = []
    for line in text.splitlines():
        im = _INCLUDE_LINE.search(line.strip())
        if not im:
            continue
        title = _RUNTIME_TAIL.sub('', im.group(4)).strip()
        i_dollar, i_period = _criteria_tokens(title)
        result["include_sets"].append({
            "count":  int(im.group(1).replace(',', '')),
            "member": im.group(2).strip().upper(),
            "job_id": im.group(3).strip(),
            "title":  title,
            "dollar": i_dollar,
            "period": i_period,
        })
    if not result["include_sets"]:
        result["parse_errors"].append("INCLUDE BY ACCOUNT # line not found (include-set check skipped)")

    # Mailing date: Mailing Date...: 3/05/2026
    m = re.search(r'Mailing\s+Date[\s.]*:\s*(\d{1,2}/\d{1,2}/\d{2,4})', text, re.IGNORECASE)
    if m:
        result["mailing_date"] = _normalize_date(m.group(1))
    else:
        result["mailing_date"] = ""
        result["parse_errors"].append("Mailing Date line not found")

    # Seed database: SEED RECORDS INCLUDED FROM LIST: K40 S
    m = re.search(
        r'SEED\s+RECORDS\s+INCLUDED\s+FROM\s+LIST\s*:\s*([\w\s]+?)(?:\n|\s{2,}|\Z)',
        text, re.IGNORECASE
    )
    if m:
        result["seed_db"] = re.sub(r'\s+', '', m.group(1)).upper()
    else:
        result["seed_db"] = ""
        result["parse_errors"].append("SEED RECORDS INCLUDED FROM LIST line not found")

    # Flag omits — ADSTRA can split flag omits across MULTIPLE criteria blocks and
    # spell the header either way:
    #   "CRITERIA ...: 1  OMIT FLAGS"    -> FLAGS = !, OR = D, OR = N, ...
    #   "CRITERIA ...: 6  OMIT FLAG $"   -> FLAGS = $   (e.g. the DMA-pander $ suppress)
    # Union every flag-omit block: the flag named in the header itself
    # ("OMIT FLAG $") plus each "FLAGS/OR = X" value line inside it.
    result["flags"] = set()
    found_flag_block = False
    for _hdr, _body in _iter_criteria_blocks(text):
        if not re.search(_OMIT_FLAG_HDR, _hdr, re.IGNORECASE):
            continue
        found_flag_block = True
        # Flag named directly in the header, e.g. "OMIT FLAG $" (guard against
        # swallowing the following word like "OMIT FLAGS  EXCLUDED").
        _hm = re.search(_OMIT_FLAG_HDR + r'\s+([A-Z0-9!\$])(?![A-Z0-9])', _hdr, re.IGNORECASE)
        if _hm:
            result["flags"].add(_hm.group(1))
        for _fl in _body:
            # First value line is "FLAGS  :  = !" (colon before =); the rest are "OR = X".
            # Allow any non-'=' chars between the keyword and '=' so the leading flag is caught.
            _fm = re.match(r'(?:FLAGS?|OR)\b[^=\n]*=\s*([A-Z0-9!\$])', _fl, re.IGNORECASE)
            if _fm:
                result["flags"].add(_fm.group(1))
    if not found_flag_block:
        result["parse_errors"].append("OMIT FLAGS criteria block not found (flag omits check skipped)")

    # State omits — every OMIT STATES criteria block (order-specific, not standard
    # territory block); union in case it's split across criteria.
    state_lines = _collect_criteria_blocks(text, _OMIT_STATE_HDR)
    result["omit_states"] = set()
    for _sl in state_lines:
        _sm = re.match(r'(?:STATE|OR)\s*=\s*([A-Z]{2})\b', _sl, re.IGNORECASE)
        if _sm:
            result["omit_states"].add(_sm.group(1).upper())

    # Zip omits — every OMIT ZIPS criteria block; union in case it's split.
    zip_lines = _collect_criteria_blocks(text, _OMIT_ZIP_HDR)
    result["omit_zips"] = set()
    for _zl in zip_lines:
        _zm = re.match(r'(?:ZIP\s*CODE|OR)\s*=\s*(\d{5})', _zl, re.IGNORECASE)
        if _zm:
            result["omit_zips"].add(_zm.group(1))

    # File format — from REPORT PROGRAMS section (page 2 typically)
    # e.g. "ASCII COMMA DELIMITED W/WRKDTA", "ASCII FIXED LENGTH", "EXCEL"
    # Also: "<N> TAPE DON'T TOP LOAD" = ASCII Fixed
    m = re.search(
        r'(ASCII\s+COMMA\s+DELIMITED|ASCII\s+FIXED(?:\s+LENGTH)?|EXCEL)',
        text, re.IGNORECASE
    )
    if m:
        raw_fmt = re.sub(r'\s+', ' ', m.group(1).strip().upper())
        result["file_format"] = _SELECT_FORMAT_MAP.get(raw_fmt, "Other")
    elif re.search(r"TAPE\s+DON'?T\s+TOP\s+LOAD", text, re.IGNORECASE):
        result["file_format"] = "ASCII Fixed"
    else:
        result["file_format"] = ""
        result["parse_errors"].append("File format (ASCII/EXCEL) line not found")

    # Shipping info from the NOTES section of the last page
    _shipping = _parse_shipping_info(text)
    result["parse_errors"].extend(_shipping.pop("parse_errors"))
    result.update(_shipping)

    return result


# ---------------------------------------------------------------------------
# QC comparison engine
# ---------------------------------------------------------------------------

