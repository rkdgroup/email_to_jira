"""
QC Checker for DSLF List Rental Pipeline.

Downloads the SELECT PDF attached to a 'Needs QC' Jira ticket, parses key
fields, compares them against the ticket's requirements, and posts a
structured pass/fail report comment. The ticket is never transitioned —
it stays in 'Needs QC' regardless of the result.

Usage:
    python qc_checker.py                    # scan all Need QC tickets
    python qc_checker.py DSLF-123          # single ticket
    python qc_checker.py --dry-run         # no writes to Jira
    python qc_checker.py DSLF-123 --dry-run
"""

import os
import re
import sys
import logging
import shutil
import tempfile
import argparse
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

QC_PASS_THRESHOLD = 4
HARD_REQUIRED     = {"Client Database", "Manager Order #"}
NEED_QC_STATUS    = "Needs QC"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _extract_adf_text(adf) -> str:
    """Recursively extract plain text from a Jira ADF dict."""
    if not isinstance(adf, dict):
        return str(adf) if adf else ""
    texts = []

    def _recurse(node):
        if isinstance(node, dict):
            if node.get("type") == "text":
                texts.append(node.get("text", ""))
            for child in node.get("content", []):
                _recurse(child)
        elif isinstance(node, list):
            for item in node:
                _recurse(item)

    _recurse(adf)
    return " ".join(t for t in texts if t)


_US_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
    'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC',
}

_CRITERIA_LINE = re.compile(r'^\s*CRITERIA\s*\.+\s*:\s*\d+', re.IGNORECASE)


_SELECT_FORMAT_MAP = {
    'ASCII COMMA DELIMITED': 'ASCII Delimited',
    'ASCII FIXED LENGTH':    'ASCII Fixed',
    'ASCII FIXED':           'ASCII Fixed',
    'EXCEL':                 'Excel',
}

_NAME_STOPWORDS = {
    'LIST', 'DATA', 'FILE', 'MAIL', 'MAILING',
    'DONOR', 'DONORS', 'NAME', 'NAMES', 'DIRECT',
    'PROGRAM', 'PROGRAMS', 'FUND', 'FUNDS',
}


def _clean_select_name(raw: str) -> str:
    """Strip trailing company footer and expand acronyms in parens for fuzzy matching."""
    raw = re.sub(r'\s*DATA\s+MAIL\s+INC\.?\s*$', '', raw.strip(), flags=re.IGNORECASE)
    raw = re.sub(r'\((\w+)\)', r' \1 ', raw)  # (ECAD) → ECAD
    return raw.strip()


def _name_words(s: str) -> set:
    """Return significant words (4+ chars, not stopwords) from a name string, uppercased."""
    s = re.sub(r'[^A-Z0-9\s]', ' ', s.upper())
    return {w for w in s.split() if len(w) >= 4 and w not in _NAME_STOPWORDS}


def _fuzzy_name_match(s_words: set, t_words: set) -> list[str]:
    """
    Match words between two sets via exact match or prefix overlap.
    Prefix: one word starts with the other (covers abbreviations and truncations).
    Returns list of match labels for display.
    """
    matches = []
    used_t: set = set()
    for sw in sorted(s_words):
        if sw in t_words:
            matches.append(sw)
            used_t.add(sw)
        else:
            for tw in sorted(t_words - used_t):
                if sw.startswith(tw) or tw.startswith(sw):
                    matches.append(f"{sw}/{tw}")
                    used_t.add(tw)
                    break
    return matches


def _collect_criteria_block(text: str, criteria_keyword: str) -> list[str]:
    """
    Find the CRITERIA block whose header matches criteria_keyword and return its
    non-empty value lines.  Stops when the next 'CRITERIA ...: N' line begins.
    """
    in_block = False
    result: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if _CRITERIA_LINE.match(stripped):
            if in_block:
                break          # next criteria started
            if re.search(criteria_keyword, stripped, re.IGNORECASE):
                in_block = True
        elif in_block and stripped:
            result.append(stripped)
    return result


def _load_adstra_flag_omits() -> dict:
    """
    seed_database → expected flag-omit set, from config/adstra_omit_database.yaml.
    Mirrors parse_pipeline's loader (first entry wins on duplicate seeds) but
    splits compound entries like "!$" into single characters for set compare.
    """
    yaml_path = _ROOT / "config" / "adstra_omit_database.yaml"
    if not yaml_path.exists():
        return {}
    try:
        import yaml
        with open(yaml_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        log.warning("Could not load %s: %s", yaml_path.name, e)
        return {}
    result: dict = {}
    for entry in data.get("adstra_database", []):
        seed_db = str(entry.get("seed_database", "")).upper()
        flags   = entry.get("flags", [])
        if seed_db and flags and seed_db not in result:
            result[seed_db] = {ch for fl in flags for ch in str(fl)}
    return result


_ADSTRA_FLAG_OMITS = _load_adstra_flag_omits()


def _is_saturn(select_data: dict, ticket_fields: dict) -> bool:
    """Saturn Corp orders are always FTP + ASCII Fixed regardless of the order form."""
    return ("saturn" in select_data.get("ship_to_email", "").lower()
            or "saturn" in (ticket_fields.get("ship_to_email") or "").lower())


# Fixed-format processing houses (CREAT 4300 TAPE, DON'T TOP LOAD): files sent to these
# addresses are ALWAYS fixed-length ASCII. Kept in sync with tools_jira._FIXED_FORMAT_EMAILS.
_FIXED_FORMAT_EMAILS = (
    "data@trylondm.com", "data@talonmm.com", "data@rkdgroup.com",
    "tisdata@trinitydirect.net", "tapelibrarian@directmail.com",
)


def _is_fixed_format(select_data: dict, ticket_fields: dict) -> bool:
    """Ship-to routed to a fixed-format house is always ASCII Fixed (like Saturn, but Email)."""
    s = (select_data.get("ship_to_email") or "").lower()
    t = (ticket_fields.get("ship_to_email") or "").lower()
    return any(a in s or a in t for a in _FIXED_FORMAT_EMAILS)


def _extract_ticket_flags(omission_adf) -> set:
    """Parse 'FLAG OMITS: D, N, R, $, A, X, !' from omission description ADF."""
    text = _extract_adf_text(omission_adf)
    if not text:
        return set()
    m = re.search(r'FLAG\s+OMITS\s*:\s*([^\n]+)', text, re.IGNORECASE)
    if not m:
        return set()
    flag_str = m.group(1).strip()
    return set(re.findall(r'(?<![A-Z0-9])([A-Z0-9!\$])(?![A-Z0-9])', flag_str))


def _extract_ticket_states(omission_adf) -> set:
    """
    Extract 2-letter US state codes from the ticket's omission description ADF.

    A bare \\b[A-Z]{2}\\b scan turns uppercase prose words (OR, IN, ME, OK...)
    into phantom states, so codes only count when they appear in state context:
      A. a comma-delimited run of 2+ codes ("AK, HI, OR" — Oregon kept)
      B. a list following a STATES keyword — where OR/AND between codes
         are conjunctions, not Oregon ("OMIT STATES: AK OR HI" -> {AK, HI})
      C. directly after OMIT ("OMIT AK")
    """
    text = _extract_adf_text(omission_adf)
    if not text:
        return set()

    def _codes_from_span(span: str) -> set:
        span = re.sub(r'\bAND\b', ',', span)
        tokens = set(re.findall(r'\b([A-Z]{2})\b', span))
        if 'OR' in tokens:
            # OR space-separated between codes is a conjunction ("AK OR HI");
            # it only counts as Oregon when comma-delimited or standing alone
            if not re.search(r'(?:^|,)\s*OR\s*(?:,|$)|,\s*OR\b|\bOR\s*,', span):
                tokens.discard('OR')
        return tokens

    found: set = set()

    # Rule A: comma-delimited runs of 2+ codes, with optional "OR/AND XX" tail
    # ("AK, HI OR ME" — OR is the conjunction, ME is the last item)
    for m in re.finditer(
            r'\b[A-Z]{2}(?:\s*,\s*[A-Z]{2}\b)+(?:\s*,?\s*\b(?:OR|AND)\s+[A-Z]{2}\b)?',
            text):
        found |= _codes_from_span(m.group(0))

    # Rule B: list after a STATES keyword ("OMIT STATES: ...", "STATE OMITS - ...");
    # codes separated by spaces, commas, slashes, ampersands, or OR/AND
    for m in re.finditer(
            r'STATES?\b(?:\s+(?:OMITS?|TO|EXCLUDED?|EXCLUSIONS?))*[^A-Za-z0-9\n]{0,20}'
            r'((?:[A-Z]{2}\b(?:\s*(?:,|/|&|\bOR\b|\bAND\b)\s*|\s+))*[A-Z]{2}\b)',
            text):
        found |= _codes_from_span(m.group(1))

    # Rule C: "OMIT XX" directly, including short chains ("OMIT AK AND HI")
    for m in re.finditer(
            r'\bOMIT\s+([A-Z]{2}\b(?:\s*(?:,|&|/|\bAND\b|\bOR\b)\s*[A-Z]{2}\b)*)',
            text):
        found |= _codes_from_span(m.group(1))

    return found & _US_STATES


def _extract_ticket_zips(omission_adf) -> set:
    """
    Extract 5-digit zip codes from the ticket's omission description ADF.
    Only attempted when the text mentions zips at all — otherwise standalone
    5-digit numbers ("25000 LIFETIME", "$25000") become phantom zips.
    """
    text = _extract_adf_text(omission_adf)
    if not text or not re.search(r'\bZIP(?:\s*CODE)?S?\b', text, re.IGNORECASE):
        return set()
    return set(re.findall(r'(?<![\d$.,])(\d{5})(?!\s*[\d+])', text))


# ---------------------------------------------------------------------------
# SELECT PDF identification
# ---------------------------------------------------------------------------

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
        suffix                   = m.group(2).strip()
        result["criteria_suffix"] = suffix
        # Dollar amounts: "$5+" or "05+" (zero-padded, no $ sign) — normalize to "$5+"
        _digit_re = re.compile(r'\d+')
        result["dollar_criteria"] = [
            f"${int(_digit_re.search(t).group())}+"
            for t in re.findall(r'\$?\d+\+', suffix)
        ]
        # Time periods: "L3M" or "L03" (zero-padded, trailing M optional) — normalize to "L3M"
        result["period_criteria"] = [
            f"L{int(n)}M"
            for n in re.findall(r'\bL(\d+)M?\b', suffix, re.IGNORECASE)
        ]
    else:
        result["manager_order"]    = ""
        result["criteria_suffix"]  = ""
        result["dollar_criteria"]  = []
        result["period_criteria"]  = []
        result["parse_errors"].append("REPORT/P.O.# line not found (manager_order, criteria)")

    # Total records selected
    m = re.search(r'TOTAL\s+RECORDS\s+SELECTED[\s.]*\s*([\d,]+)', text, re.IGNORECASE)
    if m:
        result["total_records"] = int(m.group(1).replace(',', ''))
    else:
        result["total_records"] = 0
        result["parse_errors"].append("TOTAL RECORDS SELECTED line not found")

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

    # Flag omits — collect every value line in the OMIT FLAGS criteria block,
    # which ends when the next CRITERIA line begins.
    flag_lines = _collect_criteria_block(text, r'OMIT\s+FLAGS\b')
    if flag_lines:
        result["flags"] = set()
        for _fl in flag_lines:
            # First flag line is "FLAGS  :  = !" (colon before =); the rest are "OR = X".
            # Allow any non-'=' chars between the keyword and '=' so the leading flag is caught.
            _fm = re.match(r'(?:FLAGS|OR)\b[^=\n]*=\s*([A-Z0-9!\$])', _fl, re.IGNORECASE)
            if _fm:
                result["flags"].add(_fm.group(1))
    else:
        result["flags"] = set()
        result["parse_errors"].append("OMIT FLAGS criteria block not found (flag omits check skipped)")

    # State omits — OMIT STATES criteria block (order-specific, not standard territory block)
    state_lines = _collect_criteria_block(text, r'OMIT\s+STATES?\b')
    result["omit_states"] = set()
    for _sl in state_lines:
        _sm = re.match(r'(?:STATE|OR)\s*=\s*([A-Z]{2})\b', _sl, re.IGNORECASE)
        if _sm:
            result["omit_states"].add(_sm.group(1).upper())

    # Zip omits — OMIT ZIPS criteria block
    zip_lines = _collect_criteria_block(text, r'OMIT\s+ZIPS?\b')
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

    # Shipping info from NOTES section of last page:
    #   Email: TO:<email> and CC: <email>
    #   FTP:   FILENAME: <name>.ZIP
    _email_re = r'[\w.\-]+@[\w.\-]+'
    m_to = re.search(r'\bTO\s*:\s*(' + _email_re + r')', text, re.IGNORECASE)
    m_cc = re.search(r'\bCC\s*:\s*(' + _email_re + r')', text, re.IGNORECASE)
    m_fn = re.search(r'\bFILENAME\s*:\s*(\S+\.ZIP)', text, re.IGNORECASE)

    if m_to:
        result["shipping_method"] = "Email"
        result["ship_to_email"]   = m_to.group(1).strip().upper()
        result["cc_email"]        = m_cc.group(1).strip().upper() if m_cc else ""
        result["ftp_filename"]    = ""
    elif m_fn:
        result["shipping_method"] = "FTP"
        result["ftp_filename"]    = m_fn.group(1).strip().upper()
        result["ship_to_email"]   = ""
        result["cc_email"]        = ""
    else:
        result["shipping_method"] = ""
        result["ship_to_email"]   = ""
        result["cc_email"]        = ""
        result["ftp_filename"]    = ""
        result["parse_errors"].append("Shipping info (TO:/CC:/FILENAME:) not found in SELECT PDF")

    return result


# ---------------------------------------------------------------------------
# QC comparison engine
# ---------------------------------------------------------------------------

def run_qc_checks(select_data: dict, ticket_fields: dict) -> dict:
    """Compare SELECT PDF data against ticket fields. Returns check results."""
    checks = []
    hard_fails = []

    def _check(status, label, detail):
        # WARN = something that can't be measured / doesn't apply. Skip it entirely
        # so only PASS/FAIL surface (no WARN rows, no dilution of the check total).
        if status == "WARN":
            return
        checks.append((status, label, detail))
        if status == "FAIL" and label in HARD_REQUIRED:
            hard_fails.append(label)

    # 1. Client Database (HARD)
    s_db = select_data.get("client_db", "")
    t_db = ticket_fields.get("client_db", "")
    if s_db and t_db and s_db == t_db:
        _check("PASS", "Client Database", f"{s_db} matches {t_db}")
    elif not s_db:
        _check("FAIL", "Client Database", "Could not parse client DB from SELECT PDF")
    elif not t_db:
        _check("FAIL", "Client Database", f"SELECT has {s_db!r} but ticket has no Client DB set")
    else:
        _check("FAIL", "Client Database", f"SELECT has {s_db!r} but ticket has {t_db!r}")

    # 2. Manager Order # (HARD)
    s_ord = select_data.get("manager_order", "")
    t_ord = ticket_fields.get("manager_order", "")
    if s_ord and t_ord and s_ord == t_ord:
        _check("PASS", "Manager Order #", f"{s_ord} matches {t_ord}")
    elif not s_ord:
        _check("FAIL", "Manager Order #", "Could not parse manager order from SELECT PDF")
    elif not t_ord:
        _check("FAIL", "Manager Order #", f"SELECT has {s_ord!r} but ticket has no Manager Order # set")
    else:
        _check("FAIL", "Manager Order #", f"SELECT has {s_ord!r} but ticket has {t_ord!r}")

    # 3. List Name — fuzzy word-overlap match
    s_name = select_data.get("customer_name", "")
    t_list = ticket_fields.get("list_name", "")
    if not s_name:
        _check("WARN", "List Name", "Could not parse account name from SELECT PDF")
    elif not t_list:
        _check("WARN", "List Name", f"SELECT has {s_name!r} but ticket has no List Name set")
    else:
        s_words = _name_words(_clean_select_name(s_name))
        t_words = _name_words(t_list)
        matches = _fuzzy_name_match(s_words, t_words)
        if matches:
            _check("PASS", "List Name", f"Matched on: {matches}")
        else:
            _check("FAIL", "List Name",
                   f"No match — SELECT: {s_name!r}, ticket: {t_list!r}")

    # 4. Records selected — logic differs by availability rule:
    #   All Available : quantity is whatever the SELECT returns — the requested qty
    #                   is only an estimate, so DON'T check it (skip the comparison).
    #   Nth           : count must not exceed the requested maximum
    total_sel  = select_data.get("total_records", 0)
    req_qty    = int(ticket_fields.get("requested_qty", 0) or 0)  # Jira returns floats (3000.0)
    avail_rule = (ticket_fields.get("availability_rule") or "").strip().lower()
    is_all_avail = "all" in avail_rule  # matches "All Available"

    if is_all_avail:
        # All Available: no quantity check — report the count for information only.
        msg = f"{total_sel:,} records (All Available — quantity not checked)" if total_sel \
              else "All Available — quantity not checked"
        _check("PASS", "Records Selected", msg)
    elif total_sel == 0:
        _check("FAIL", "Records Selected", "Could not parse total records from SELECT PDF")
    elif req_qty == 0:
        _check("WARN", "Records Selected",
               f"SELECT has {total_sel:,} records — ticket has no Requested Qty set")
    else:
        # Nth: count must not exceed the requested maximum
        if total_sel <= req_qty:
            _check("PASS", "Records Selected",
                   f"{total_sel:,} <= max {req_qty:,} (Nth)")
        else:
            _check("FAIL", "Records Selected",
                   f"SELECT has {total_sel:,} which exceeds Nth maximum of {req_qty:,}")

    # 5. Selection criteria — $-amount and L#M tokens from REPORT line vs ticket description
    s_dollar = select_data.get("dollar_criteria", [])
    s_period = select_data.get("period_criteria", [])
    desc_text = _extract_adf_text(ticket_fields.get("description_adf")).upper()

    if not s_dollar and not s_period:
        _check("WARN", "Selection Criteria",
               f"No $-amount or L#M tokens in SELECT REPORT line "
               f"({select_data.get('criteria_suffix', '') or 'no suffix'})")
    else:
        missing_dollar = []  # FAIL — $ amount not found at all
        missing_period = []  # WARN — period missing or different in description

        for dc in s_dollar:
            amount = re.escape(dc.replace('$', '').rstrip('+'))
            # digit boundaries so "$5+" is not satisfied by "$15+" or "$50+"
            pat = (rf'(?<![\d.,])\$?\s*{amount}(?:\.0+)?\s*\+'
                   rf'|\$\s*{amount}(?:\.0+)?(?![\d.])')
            if not re.search(pat, desc_text):
                missing_dollar.append(dc)

        for pc in s_period:
            n = re.match(r'L(\d+)M', pc).group(1)
            # Exact period with digit boundary ("3M" must not match "13M")
            if re.search(rf'(?<!\d){n}\s*M(?:ONTHS?)?\b', desc_text):
                continue
            other = re.search(r'(?<!\d)(\d+)\s*M(?:ONTHS?)?\b', desc_text)
            missing_period.append(
                (pc, other.group(0).strip() if other else None))

        if not missing_dollar and not missing_period:
            _check("PASS", "Selection Criteria",
                   f"Criteria found in description: {', '.join(s_dollar + s_period)}")
        elif missing_dollar:
            _check("FAIL", "Selection Criteria",
                   f"$ amount missing from description: {', '.join(missing_dollar)} "
                   f"(SELECT: {select_data.get('criteria_suffix', '')})")
        else:
            details = [
                f"SELECT specifies {pc} but description mentions {other!r}"
                if other else f"no time period in description for {pc}"
                for pc, other in missing_period
            ]
            _check("WARN", "Selection Criteria",
                   f"{'; '.join(details)} "
                   f"(SELECT: {select_data.get('criteria_suffix', '')})")

    # 6. Seed database
    s_seed = select_data.get("seed_db", "")
    t_seed = ticket_fields.get("seed_db", "")
    if not s_seed:
        _check("FAIL", "Seed Database", "Could not parse seed DB from SELECT PDF")
    elif not t_seed:
        _check("WARN", "Seed Database", f"SELECT has {s_seed!r} but ticket has no Seed DB set")
    elif s_seed == t_seed:
        _check("PASS", "Seed Database", f"{s_seed} matches {t_seed}")
    else:
        _check("FAIL", "Seed Database", f"SELECT has {s_seed!r} but ticket has {t_seed!r}")

    # 7. File Format
    s_fmt = select_data.get("file_format", "")
    t_fmt = ticket_fields.get("file_format", "")
    saturn = _is_saturn(select_data, ticket_fields)
    if saturn:
        # Saturn Corp is ALWAYS ASCII Fixed regardless of what the SELECT/ticket says.
        if t_fmt == "ASCII Fixed":
            _check("PASS", "File Format", "ASCII Fixed (Saturn Corp — always ASCII Fixed)")
        elif not t_fmt:
            _check("WARN", "File Format",
                   "Saturn Corp order — ticket should be ASCII Fixed but File Format is unset")
        else:
            _check("FAIL", "File Format",
                   f"Saturn Corp order must be 'ASCII Fixed' but ticket has {t_fmt!r}")
    elif _is_fixed_format(select_data, ticket_fields):
        # Fixed-format houses (CREAT 4300 TAPE) are ALWAYS ASCII Fixed regardless of SELECT.
        if t_fmt == "ASCII Fixed":
            _check("PASS", "File Format", "ASCII Fixed (fixed-format address — always ASCII Fixed)")
        elif not t_fmt:
            _check("WARN", "File Format",
                   "Fixed-format address — ticket should be ASCII Fixed but File Format is unset")
        else:
            _check("FAIL", "File Format",
                   f"Fixed-format address must be 'ASCII Fixed' but ticket has {t_fmt!r}")
    elif not s_fmt:
        _check("WARN", "File Format", "Could not parse file format from SELECT PDF")
    elif not t_fmt:
        _check("WARN", "File Format",
               f"SELECT indicates {s_fmt!r} but ticket has no File Format set")
    elif s_fmt == t_fmt:
        _check("PASS", "File Format", f"{s_fmt} matches ticket")
    else:
        _check("FAIL", "File Format",
               f"SELECT has {s_fmt!r} but ticket has {t_fmt!r}")

    # 8. Flag omits
    s_flags = select_data.get("flags", set())
    t_flags = _extract_ticket_flags(ticket_fields.get("omission_adf"))

    if not s_flags and not t_flags:
        _check("WARN", "Flag Omits", "Neither SELECT PDF nor ticket has flag omit data")
    elif not s_flags:
        _check("WARN", "Flag Omits", "Could not parse flags from SELECT PDF")
    elif not t_flags:
        _check("WARN", "Flag Omits",
               f"SELECT has {sorted(s_flags)} — ticket omission has no FLAG OMITS line")
    else:
        extra   = s_flags - t_flags
        missing = t_flags - s_flags
        if not extra and not missing:
            _check("PASS", "Flag Omits", f"Both have {sorted(s_flags)}")
        elif extra and not missing:
            _check("WARN", "Flag Omits",
                   f"SELECT has {sorted(s_flags)} — ticket has {sorted(t_flags)} "
                   f"({','.join(sorted(extra))} extra in SELECT)")
        else:
            _check("FAIL", "Flag Omits",
                   f"Mismatch — SELECT: {sorted(s_flags)}, ticket: {sorted(t_flags)}")

    # 8b. ADSTRA expected flags — third source: known defaults per seed DB
    # from config/adstra_omit_database.yaml. WARN-only by design.
    t_seed_key = (ticket_fields.get("seed_db") or "").strip().upper()
    expected_flags = _ADSTRA_FLAG_OMITS.get(t_seed_key)
    if expected_flags:
        if not s_flags:
            _check("WARN", "ADSTRA Flag Omits",
                   f"ADSTRA expects {sorted(expected_flags)} for {t_seed_key} — "
                   f"no flags parsed from SELECT")
        elif s_flags == expected_flags:
            _check("PASS", "ADSTRA Flag Omits",
                   f"SELECT flags match ADSTRA defaults for {t_seed_key}: "
                   f"{sorted(expected_flags)}")
        else:
            missing = expected_flags - s_flags
            extra   = s_flags - expected_flags
            parts = []
            if missing:
                parts.append(f"missing {sorted(missing)}")
            if extra:
                parts.append(f"extra {sorted(extra)}")
            _check("WARN", "ADSTRA Flag Omits",
                   f"SELECT has {sorted(s_flags)} but ADSTRA defaults for "
                   f"{t_seed_key} are {sorted(expected_flags)} ({'; '.join(parts)})")

    # 9. State omits
    s_states = select_data.get("omit_states", set())
    t_states = _extract_ticket_states(ticket_fields.get("omission_adf"))

    if not s_states and not t_states:
        _check("WARN", "State Omits", "No state omit data in SELECT or ticket")
    elif not t_states:
        _check("WARN", "State Omits",
               f"SELECT omitted {sorted(s_states)} — ticket has no states in omission description")
    elif not s_states:
        _check("WARN", "State Omits",
               f"Ticket specifies {sorted(t_states)} but no OMIT STATES block in SELECT")
    else:
        missing = t_states - s_states
        if not missing:
            _check("PASS", "State Omits",
                   f"All required states omitted: {sorted(s_states)}")
        else:
            _check("FAIL", "State Omits",
                   f"Missing from SELECT: {sorted(missing)} "
                   f"(SELECT has {sorted(s_states)}, ticket has {sorted(t_states)})")

    # 10. Zip omits (only checked when either side has zip data)
    s_zips = select_data.get("omit_zips", set())
    t_zips = _extract_ticket_zips(ticket_fields.get("omission_adf"))

    if t_zips or s_zips:
        if not t_zips:
            _check("WARN", "Zip Omits",
                   f"SELECT omitted {len(s_zips)} zip(s) — ticket has none specified")
        elif not s_zips:
            _check("FAIL", "Zip Omits",
                   f"Ticket specifies {sorted(t_zips)} but no OMIT ZIPS block in SELECT")
        else:
            missing = t_zips - s_zips
            if not missing:
                _check("PASS", "Zip Omits",
                       f"All required zips omitted ({len(s_zips)} total)")
            else:
                _check("FAIL", "Zip Omits",
                       f"Missing zips: {sorted(missing)} "
                       f"(SELECT has {sorted(s_zips)}, ticket has {sorted(t_zips)})")

    # 11. Shipping Method
    s_method = select_data.get("shipping_method", "")
    t_method = ticket_fields.get("shipping_method", "")
    if saturn:
        # CONVERT@SATURNCORP.COM parses as a TO: email but is really an FTP upload.
        if t_method.lower() == "ftp":
            _check("PASS", "Shipping Method", "FTP (Saturn Corp — always shipped via FTP)")
        elif not t_method:
            _check("WARN", "Shipping Method",
                   "Saturn Corp order — ticket should be FTP but Shipping Method is unset")
        else:
            _check("FAIL", "Shipping Method",
                   f"Saturn Corp order must ship via FTP but ticket has {t_method!r}")
    elif not s_method:
        _check("WARN", "Shipping Method",
               "Could not determine shipping method from SELECT PDF")
    elif not t_method:
        _check("WARN", "Shipping Method",
               f"SELECT indicates {s_method!r} but ticket has no Shipping Method set")
    elif s_method.lower() == t_method.lower():
        _check("PASS", "Shipping Method", f"{s_method} matches ticket")
    else:
        _check("FAIL", "Shipping Method",
               f"SELECT indicates {s_method!r} but ticket has {t_method!r}")

    # 12. Ship To Email (Email method only)
    if s_method == "Email":
        s_to = select_data.get("ship_to_email", "")
        t_to = ticket_fields.get("ship_to_email", "").upper()
        if not s_to:
            _check("WARN", "Ship To Email",
                   "Could not parse TO: email from SELECT PDF")
        elif not t_to:
            _check("WARN", "Ship To Email",
                   f"SELECT has TO: {s_to} but ticket has no Ship To Email set")
        elif s_to == t_to:
            _check("PASS", "Ship To Email", f"{s_to} matches ticket")
        else:
            _check("FAIL", "Ship To Email",
                   f"SELECT has {s_to!r} but ticket has {t_to!r}")

    # 13. Shipping CC (Email method only)
    if s_method == "Email":
        s_cc = select_data.get("cc_email", "")
        t_instr = ticket_fields.get("shipping_instructions", "").upper()
        if not s_cc:
            _check("WARN", "Shipping CC",
                   "Could not parse CC: email from SELECT PDF")
        elif not t_instr:
            _check("WARN", "Shipping CC",
                   f"SELECT has CC: {s_cc} but ticket has no Shipping Instructions set")
        elif s_cc in t_instr:
            _check("PASS", "Shipping CC",
                   f"CC: {s_cc} found in Shipping Instructions")
        else:
            _check("FAIL", "Shipping CC",
                   f"SELECT CC: {s_cc!r} not in ticket Shipping Instructions {t_instr!r}")

    # 14. FTP Notify (FTP method only) — ticket Ship To Email should hold the
    # notification address as "FTP NOTIFY: email@domain.com". WARN-only.
    if s_method == "FTP" or t_method.lower() == "ftp":
        t_to    = (ticket_fields.get("ship_to_email") or "").strip()
        ftp_fn  = select_data.get("ftp_filename", "")
        fn_note = f" (SELECT filename: {ftp_fn})" if ftp_fn else ""
        if not t_to:
            _check("WARN", "FTP Notify",
                   f"FTP order — ticket Ship To Email is empty, expected "
                   f"'FTP NOTIFY: email@domain.com'{fn_note}")
        elif re.match(r'(?i)^\s*FTP\s+NOTIFY\s*:\s*[\w.\-]+@[\w.\-]+', t_to):
            _check("PASS", "FTP Notify",
                   f"Ship To Email has FTP notify address: {t_to}{fn_note}")
        else:
            _check("WARN", "FTP Notify",
                   f"FTP order — ticket Ship To Email is {t_to!r}, expected "
                   f"'FTP NOTIFY: email@domain.com'{fn_note}")

    pass_count  = sum(1 for s, _, _ in checks if s == "PASS")
    overall_pass = (pass_count >= QC_PASS_THRESHOLD) and (not hard_fails)

    return {
        "checks":       checks,
        "pass_count":   pass_count,
        "total_checks": len(checks),
        "hard_fails":   hard_fails,
        "overall_pass": overall_pass,
    }


# ---------------------------------------------------------------------------
# Comment formatter
# ---------------------------------------------------------------------------

def format_qc_comment(ticket_key: str, select_filename: str,
                      qc_result: dict, parse_errors: list,
                      select_warnings: list | None = None) -> str:
    """Build the plain-text Jira comment for the QC result."""
    checks     = qc_result["checks"]
    pass_count = qc_result["pass_count"]
    total      = qc_result["total_checks"]
    overall    = "QC PASSED" if qc_result["overall_pass"] else "QC FAILED"
    hard_fails = qc_result["hard_fails"]

    lines = [
        f"QC CHECK RESULTS — {ticket_key}",
        "=" * 36,
        f"SELECT FILE: {select_filename}",
        "",
    ]
    for status, label, detail in checks:
        lines.append(f"{status:<4}  {label:<22} {detail}")

    lines.append("")
    lines.append(f"RESULT: {overall} ({pass_count}/{total} checks passed)")

    if hard_fails:
        lines.append(f"HARD FAIL: {', '.join(hard_fails)} must match")

    all_warnings = list(select_warnings or []) + list(parse_errors or [])
    if all_warnings:
        lines.append("")
        lines.append("WARNINGS:")
        for w in all_warnings:
            lines.append(f"  - {w}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Single-ticket orchestrator
# ---------------------------------------------------------------------------

def process_ticket_qc(ticket_key: str, dry_run: bool = False) -> dict:
    """Run full QC pipeline for one ticket. Returns result dict."""
    from tools_jira import (get_ticket_qc_fields, download_attachment,
                             add_comment_to_ticket)

    log.info("QC check: %s%s", ticket_key, " [DRY RUN]" if dry_run else "")

    # Fetch all fields (includes attachments)
    fields = get_ticket_qc_fields(ticket_key)
    if "error" in fields:
        return {"ticket_key": ticket_key, "error": fields["error"]}

    # Find SELECT PDF
    select_att, select_warnings = find_select_attachment(fields["attachments"])
    if not select_att:
        msg = f"No SELECT PDF attachment found on {ticket_key}"
        log.warning(msg)
        if not dry_run:
            cr = add_comment_to_ticket(
                ticket_key,
                f"QC SKIPPED — {msg}\n\nPlease attach the SELECT PDF and re-run QC.",
                code_block=True,
            )
            if "error" in cr:
                log.error("Could not post QC-SKIPPED comment to %s: %s",
                          ticket_key, cr["error"])
        return {"ticket_key": ticket_key, "error": msg}

    select_filename = select_att["filename"]
    log.info("SELECT PDF: %s", select_filename)

    tmp_dir = tempfile.mkdtemp(prefix="dslf_qc_")
    tmp_path = os.path.join(tmp_dir, select_filename)
    try:
        # Download
        try:
            download_attachment(select_att["content"], tmp_path)
        except Exception as e:
            return {"ticket_key": ticket_key, "error": f"Download failed: {e}"}

        # Parse
        select_data  = parse_select_pdf(tmp_path)
        parse_errors = select_data.pop("parse_errors", [])

        if not any(select_data.values()):
            return {"ticket_key": ticket_key,
                    "error": "SELECT PDF parsing returned no usable data"}

        # Compare
        qc_result = run_qc_checks(select_data, fields)

        # Format + print
        comment = format_qc_comment(ticket_key, select_filename, qc_result,
                                    parse_errors, select_warnings)
        print(f"\n{comment}\n")

        if dry_run:
            log.info("[DRY RUN] %s: QC %s — would post report comment (no transition)",
                     ticket_key,
                     "PASSED" if qc_result["overall_pass"] else "FAILED")
            return {
                "ticket_key":      ticket_key,
                "overall_pass":    qc_result["overall_pass"],
                "pass_count":      qc_result["pass_count"],
                "total_checks":    qc_result["total_checks"],
                "select_filename": select_filename,
                "comment":         comment,
                "dry_run":         True,
            }

        # Post comment only — never transition (ticket stays in Needs QC regardless of result)
        cr = add_comment_to_ticket(ticket_key, comment, code_block=True)
        if "error" in cr:
            log.error("Could not post QC comment to %s: %s", ticket_key, cr["error"])
        else:
            log.info("%s: QC %s — report posted, ticket stays in %r",
                     ticket_key,
                     "PASSED" if qc_result["overall_pass"] else "FAILED",
                     NEED_QC_STATUS)

        return {
            "ticket_key":      ticket_key,
            "overall_pass":    qc_result["overall_pass"],
            "pass_count":      qc_result["pass_count"],
            "total_checks":    qc_result["total_checks"],
            "select_filename": select_filename,
            "comment":         comment,
        }

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Batch scanner
# ---------------------------------------------------------------------------

_QC_COMMENT_PREFIXES = ("QC CHECK RESULTS", "QC SKIPPED")
_RERUN_GRACE_SECONDS = 120  # ignore ticket updates within 2 min of QC comment (comment post itself updates the ticket)


def _last_qc_comment_time(ticket_key: str) -> str | None:
    """Return the ISO timestamp of the most recent QC comment, or None if none exists."""
    from tools_jira import get_issue_comments
    for c in get_issue_comments(ticket_key):
        body = _extract_adf_text(c.get("body", ""))
        if body.startswith(_QC_COMMENT_PREFIXES):
            return c.get("created", "")
    return None


def _updated_after_qc(ticket_updated: str, qc_created: str) -> bool:
    """Return True if the ticket was meaningfully updated after the QC comment was posted."""
    from datetime import datetime
    try:
        # Jira format: 2026-06-11T08:15:30.123-0400 — compare TZ-aware
        fmt = "%Y-%m-%dT%H:%M:%S.%f%z"
        t_ticket = datetime.strptime(ticket_updated, fmt)
        t_qc     = datetime.strptime(qc_created, fmt)
    except Exception:
        try:
            # fallback: naive compare (both timestamps share the Jira TZ)
            fmt = "%Y-%m-%dT%H:%M:%S"
            t_ticket = datetime.strptime(ticket_updated[:19], fmt)
            t_qc     = datetime.strptime(qc_created[:19], fmt)
        except Exception:
            log.warning("Could not parse timestamps (updated=%r, qc=%r) — "
                        "treating as no change since last QC",
                        ticket_updated, qc_created)
            return False
    return (t_ticket - t_qc).total_seconds() > _RERUN_GRACE_SECONDS


def scan_need_qc_tickets(dry_run: bool = False) -> list:
    """Scan 'Need QC' tickets. Skips tickets with a QC comment unless changes were made after it."""
    from tools_jira import search_issues_paged

    jql = f'project = DSLF AND status = "{NEED_QC_STATUS}" ORDER BY created ASC'
    log.info("Scanning: %s", jql)

    all_issues = search_issues_paged(jql, "summary,status,updated")
    log.info("Found %d ticket(s) in %r", len(all_issues), NEED_QC_STATUS)

    results = []
    for issue in all_issues:
        key             = issue["key"]
        ticket_updated  = issue["fields"].get("updated", "")
        last_qc_time    = _last_qc_comment_time(key)

        if last_qc_time:
            if _updated_after_qc(ticket_updated, last_qc_time):
                log.info("%s: changes made after last QC — re-running QC", key)
            else:
                log.info("%s: no changes since last QC — skipping", key)
                continue

        results.append(process_ticket_qc(key, dry_run=dry_run))

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="QC checker for DSLF list rental SELECT PDFs"
    )
    parser.add_argument(
        "ticket_key", nargs="?", default=None,
        help="Ticket key (e.g. DSLF-123). Omit to scan all unprocessed 'Need QC' tickets."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and compare but do not post comments or transition tickets."
    )
    parser.add_argument(
        "--watch", metavar="MINUTES", type=int, nargs="?", const=5,
        help="Keep running, scanning for new 'Need QC' tickets every MINUTES (default 5)."
    )
    args = parser.parse_args()

    if not os.getenv("JIRA_API_TOKEN") and not args.dry_run:
        print("ERROR: JIRA_API_TOKEN not set in .env")
        sys.exit(1)

    if args.ticket_key:
        result = process_ticket_qc(args.ticket_key, dry_run=args.dry_run)
        if "error" in result:
            print(f"ERROR: {result['error']}")
            sys.exit(1)
        label   = "[DRY RUN] " if args.dry_run else ""
        overall = "PASSED" if result.get("overall_pass") else "FAILED"
        print(f"\n{label}{args.ticket_key}: QC {overall} "
              f"({result.get('pass_count', 0)}/{result.get('total_checks', 0)})")
    else:
        import time

        def _run_scan():
            results = scan_need_qc_tickets(dry_run=args.dry_run)
            if results:
                print(f"\n{'Ticket':<12} {'Result':<10} {'Checks':<10} {'SELECT File'}")
                print("-" * 70)
                for r in results:
                    if "error" in r:
                        print(f"{r['ticket_key']:<12} {'ERROR':<10} {'':10} {r['error'][:40]}")
                    else:
                        overall = "PASSED" if r.get("overall_pass") else "FAILED"
                        checks  = f"{r.get('pass_count', 0)}/{r.get('total_checks', 0)}"
                        fname   = r.get("select_filename", "")[:30]
                        print(f"{r['ticket_key']:<12} {overall:<10} {checks:<10} {fname}")

                passed = sum(1 for r in results if r.get("overall_pass"))
                failed = sum(1 for r in results
                             if not r.get("overall_pass") and "error" not in r)
                errors = sum(1 for r in results if "error" in r)
                print(f"\nSummary: {passed} passed, {failed} failed, {errors} errors "
                      f"({len(results)} total)")
            return results

        _run_scan()

        if args.watch:
            interval = args.watch * 60
            log.info("Watch mode: scanning every %d minute(s). Ctrl-C to stop.", args.watch)
            try:
                while True:
                    time.sleep(interval)
                    log.info("Watch: polling for new Need QC tickets...")
                    _run_scan()
            except KeyboardInterrupt:
                log.info("Watch mode stopped.")


if __name__ == "__main__":
    main()
