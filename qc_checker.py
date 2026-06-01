"""
QC Checker for DSLF List Rental Pipeline.

Downloads the SELECT PDF attached to a 'Need QC' Jira ticket, parses key
fields, compares them against the ticket's requirements, posts a structured
pass/fail comment, and transitions the ticket to 'QC Passed' if it passes.

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
QC_PASSED_STATUS  = "QC Passed"
NEED_QC_STATUS    = "Need QC"


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


# ---------------------------------------------------------------------------
# SELECT PDF identification
# ---------------------------------------------------------------------------

def find_select_attachment(attachments: list) -> dict | None:
    """Find the SELECT PDF from a list of Jira attachment dicts."""
    SELECT_RE = re.compile(r'(?<![A-Z])SELECT(?![A-Z])', re.IGNORECASE)
    matches = [a for a in attachments
               if SELECT_RE.search(a.get("filename", ""))
               and a.get("filename", "").lower().endswith(".pdf")]
    if not matches:
        return None
    if len(matches) > 1:
        log.warning("Multiple SELECT PDFs: %s — using most recent",
                    [a["filename"] for a in matches])
        matches.sort(key=lambda a: a.get("created", ""), reverse=True)
    return matches[0]


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

    # Manager order from REPORT: P.O.# J0094 ...
    m = re.search(r'REPORT\s*:\s*P\.O\.#\s*([A-Z0-9]+)', text, re.IGNORECASE)
    if m:
        result["manager_order"] = m.group(1).strip()
    else:
        result["manager_order"] = ""
        result["parse_errors"].append("REPORT/P.O.# line not found (manager_order)")

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

    # Flag omits from OMIT FLAGS criteria block
    m = re.search(r'FLAGS\s*=\s*(.+?)(?:\n|$)', text)
    if m:
        flag_raw = m.group(1).strip()
        result["flags"] = set(re.findall(r'(?:^|[\s/])([A-Z0-9!\$])(?:\s+[A-Z]|\s*$|\s*/)', flag_raw))
        if not result["flags"]:
            # fallback: single-char tokens separated by DMA-style description words
            result["flags"] = set(re.findall(r'\b([A-Z!\$])\s+[A-Z]{2,}', flag_raw))
    else:
        result["flags"] = set()
        result["parse_errors"].append("FLAGS= line not found (flag omits check skipped)")

    # State / zip omits (informational)
    state_parts = re.findall(r'OMIT\s+STATE\s*=\s*(.+?)(?:\n|/\s*OMIT|\Z)', text, re.IGNORECASE)
    result["state_omits"] = " / ".join(p.strip() for p in state_parts)

    zip_parts = re.findall(r'OMIT\s+ZIP\s*=\s*(.+?)(?:\n|\Z)', text, re.IGNORECASE)
    result["zip_omits"] = " / ".join(p.strip() for p in zip_parts)

    return result


# ---------------------------------------------------------------------------
# QC comparison engine
# ---------------------------------------------------------------------------

def run_qc_checks(select_data: dict, ticket_fields: dict) -> dict:
    """Compare SELECT PDF data against ticket fields. Returns check results."""
    checks = []
    hard_fails = []

    def _check(status, label, detail):
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

    # 3. Records selected >= requested quantity
    total_sel = select_data.get("total_records", 0)
    req_qty   = ticket_fields.get("requested_qty", 0)
    if total_sel == 0:
        _check("FAIL", "Records Selected", "Could not parse total records from SELECT PDF")
    elif req_qty == 0:
        _check("WARN", "Records Selected",
               f"SELECT has {total_sel:,} records — ticket has no Requested Qty set")
    elif total_sel >= req_qty:
        _check("PASS", "Records Selected", f"{total_sel:,} >= requested {req_qty:,}")
    else:
        _check("FAIL", "Records Selected",
               f"SELECT has {total_sel:,} but ticket requests {req_qty:,}")

    # 4. Mailing date
    s_dt = select_data.get("mailing_date", "")
    t_dt = ticket_fields.get("mail_date", "")
    if not s_dt:
        _check("FAIL", "Mailing Date", "Could not parse mailing date from SELECT PDF")
    elif not t_dt:
        _check("WARN", "Mailing Date", f"SELECT has {s_dt!r} but ticket has no Mail Date set")
    elif s_dt == t_dt:
        _check("PASS", "Mailing Date", f"{s_dt} matches {t_dt}")
    else:
        _check("FAIL", "Mailing Date", f"SELECT has {s_dt!r} but ticket has {t_dt!r}")

    # 5. Seed database
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

    # 6. Flag omits
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
                      qc_result: dict, parse_errors: list) -> str:
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

    if parse_errors:
        lines.append("")
        lines.append("PARSE WARNINGS:")
        for e in parse_errors:
            lines.append(f"  - {e}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Single-ticket orchestrator
# ---------------------------------------------------------------------------

def process_ticket_qc(ticket_key: str, dry_run: bool = False) -> dict:
    """Run full QC pipeline for one ticket. Returns result dict."""
    from tools_jira import (get_ticket_qc_fields, download_attachment,
                             add_comment_to_ticket, transition_ticket)

    log.info("QC check: %s%s", ticket_key, " [DRY RUN]" if dry_run else "")

    # Fetch all fields (includes attachments)
    fields = get_ticket_qc_fields(ticket_key)
    if "error" in fields:
        return {"ticket_key": ticket_key, "error": fields["error"]}

    # Find SELECT PDF
    select_att = find_select_attachment(fields["attachments"])
    if not select_att:
        msg = f"No SELECT PDF attachment found on {ticket_key}"
        log.warning(msg)
        if not dry_run:
            add_comment_to_ticket(
                ticket_key,
                f"QC SKIPPED — {msg}\n\nPlease attach the SELECT PDF and re-run QC."
            )
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

        if not any(v for k, v in select_data.items() if k != "parse_errors"):
            return {"ticket_key": ticket_key,
                    "error": "SELECT PDF parsing returned no usable data"}

        # Compare
        qc_result = run_qc_checks(select_data, fields)

        # Format + print
        comment = format_qc_comment(ticket_key, select_filename, qc_result, parse_errors)
        print(f"\n{comment}\n")

        if dry_run:
            action = (f"transition to {QC_PASSED_STATUS!r}"
                      if qc_result["overall_pass"] else f"leave at {NEED_QC_STATUS!r} (QC Failed)")
            log.info("[DRY RUN] Would post comment and %s", action)
            return {
                "ticket_key":      ticket_key,
                "overall_pass":    qc_result["overall_pass"],
                "pass_count":      qc_result["pass_count"],
                "total_checks":    qc_result["total_checks"],
                "select_filename": select_filename,
                "comment":         comment,
                "dry_run":         True,
            }

        # Post comment
        cr = add_comment_to_ticket(ticket_key, comment)
        if "error" in cr:
            log.error("Could not post QC comment to %s: %s", ticket_key, cr["error"])

        # Transition if passed
        if qc_result["overall_pass"]:
            tr = transition_ticket(ticket_key, QC_PASSED_STATUS)
            if "error" in tr:
                log.warning("Could not transition %s to %r: %s",
                             ticket_key, QC_PASSED_STATUS, tr["error"])
        else:
            log.info("%s: QC FAILED — leaving at %r with comment", ticket_key, NEED_QC_STATUS)

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

def scan_need_qc_tickets(dry_run: bool = False) -> list:
    """Scan all 'Need QC' tickets and run QC on each."""
    import requests as _req
    from requests.auth import HTTPBasicAuth as _Auth

    base_url = os.getenv("JIRA_BASE_URL", "https://rkdgroup.atlassian.net")
    auth     = _Auth(os.getenv("JIRA_EMAIL"), os.getenv("JIRA_API_TOKEN"))
    jql      = f'project = DSLF AND status = "{NEED_QC_STATUS}" ORDER BY created ASC'

    log.info("Scanning: %s", jql)

    all_keys = []
    start, batch = 0, 50
    while True:
        resp = _req.get(
            f"{base_url}/rest/api/3/search", auth=auth,
            headers={"Accept": "application/json"},
            params={"jql": jql, "startAt": start, "maxResults": batch,
                    "fields": "summary,status"},
            timeout=15,
        )
        if resp.status_code != 200:
            log.error("Search failed: %s %s", resp.status_code, resp.text[:200])
            break
        data   = resp.json()
        issues = data.get("issues", [])
        all_keys.extend(i["key"] for i in issues)
        if start + batch >= data.get("total", 0):
            break
        start += batch

    log.info("Found %d ticket(s) in %r", len(all_keys), NEED_QC_STATUS)

    results = []
    for key in all_keys:
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
        help="Ticket key (e.g. DSLF-123). Omit to scan all 'Need QC' tickets."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and compare but do not post comments or transition tickets."
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
        results = scan_need_qc_tickets(dry_run=args.dry_run)

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


if __name__ == "__main__":
    main()
