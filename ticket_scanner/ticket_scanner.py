"""
DSLF Ticket Scanner — periodically scans new DSLF tickets and generates audit reports.

Usage:
    python ticket_scanner.py              # run once
    python ticket_scanner.py --loop 30   # run every 30 minutes
    python ticket_scanner.py --reset     # clear saved state and scan all tickets
"""

import os
import re
import sys
import json
import time
import logging
import argparse
import requests
from datetime import datetime
from pathlib import Path
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Load .env from parent project directory
load_dotenv(Path(__file__).parent.parent / ".env")

# --- Config ---
JIRA_BASE_URL = os.getenv("JIRA_BASE_URL", "https://rkdgroup.atlassian.net")
JIRA_EMAIL    = os.getenv("JIRA_EMAIL")
JIRA_TOKEN    = os.getenv("JIRA_API_TOKEN")
PROJECT_KEY   = "DSLF"
STATE_FILE             = Path(__file__).parent / "scanner_state.json"
REPORTS_DIR            = Path(__file__).parent / "reports"
LEARNED_PATTERNS_FILE  = Path(__file__).parent / "learned_patterns.json"

FIELDS = [
    "summary", "created",
    "customfield_12191",  # Billable Account
    "customfield_12155",  # Client Database
    "customfield_12156",  # Seed Database
    "customfield_12192",  # Manager Order Number
    "customfield_12193",  # Mailer PO
    "customfield_12194",  # Mailer Name
    "customfield_12231",  # List Manager
    "customfield_12234",  # List Name
    "customfield_12271",  # Requested Quantity
    "customfield_12273",  # Availability Rule
    "customfield_12232",  # Requestor Name
    "customfield_12233",  # Requestor Email
    "customfield_12275",  # Ship To Email
    "customfield_12276",  # Shipping Method
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# --- State ---

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_ticket_number": 0, "last_scan": None}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def ticket_number(key: str) -> int:
    """Extract numeric part from ticket key e.g. DSLF-98 → 98."""
    m = re.search(r"-(\d+)$", key)
    return int(m.group(1)) if m else 0


# --- Jira ---

def _auth():
    return HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN)


def fetch_new_tickets(after_number: int) -> list[dict]:
    """Fetch all DSLF tickets with issue number > after_number, oldest first."""
    all_issues = []
    start = 0
    batch = 50

    # JQL: project = DSLF AND issue > DSLF-{N} ORDER BY created ASC
    if after_number > 0:
        jql = f'project = {PROJECT_KEY} AND issue > "{PROJECT_KEY}-{after_number}" ORDER BY created ASC'
    else:
        jql = f"project = {PROJECT_KEY} ORDER BY created ASC"

    while True:
        params = {
            "jql": jql,
            "startAt": start,
            "maxResults": batch,
            "fields": ",".join(FIELDS),
        }
        resp = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=_auth(),
            headers={"Accept": "application/json"},
            params=params,
            timeout=20,
        )
        if resp.status_code != 200:
            log.error("Jira search failed: %s %s", resp.status_code, resp.text[:200])
            break

        data = resp.json()
        issues = data.get("issues", [])
        all_issues.extend(issues)

        if start + batch >= data.get("total", 0):
            break
        start += batch

    return all_issues


# --- Audit ---

def _val(issue: dict, field: str):
    return issue["fields"].get(field)


def _select_val(issue: dict, field: str) -> str:
    v = _val(issue, field)
    return v.get("value", "") if isinstance(v, dict) else ""


def audit_ticket(issue: dict) -> list[str]:
    """
    Return a list of issue strings for this ticket.
    Only flags genuine data errors — not cosmetic/optional fields.
    """
    problems = []
    key = issue["key"]

    # Required text fields
    required = {
        "Mailer PO":           "customfield_12193",
        "Manager Order #":     "customfield_12192",
        "Mailer Name":         "customfield_12194",
        "List Name":           "customfield_12234",
        "List Manager":        "customfield_12231",
        "Requestor Name":      "customfield_12232",
        "Requestor Email":     "customfield_12233",
        "Ship To Email":       "customfield_12275",
    }
    for label, field in required.items():
        v = _val(issue, field)
        if not v or (isinstance(v, str) and not v.strip()):
            problems.append(f"Missing {label}")

    # Quantity
    qty = _val(issue, "customfield_12271")
    if not qty or qty == 0:
        problems.append("Missing or zero Requested Quantity")

    # Availability Rule
    avail = _select_val(issue, "customfield_12273")
    if not avail:
        problems.append("Missing Availability Rule")

    # Shipping Method
    ship = _select_val(issue, "customfield_12276")
    if not ship:
        problems.append("Missing Shipping Method")

    # Billable Account vs Client Database root mismatch
    billable = _select_val(issue, "customfield_12191")   # e.g. "F65"
    client_db = _select_val(issue, "customfield_12155")  # e.g. "C21D"
    if billable and client_db:
        # Client DB root = strip trailing letter(s)
        client_root = re.sub(r"[A-Za-z]+$", "", client_db)  # "C21D" → "C21"
        if client_root and billable != client_root:
            problems.append(
                f"Billable Account mismatch: '{billable}' but Client DB is '{client_db}' (expected '{client_root}')"
            )

    return problems


# --- Report ---

def generate_report(scanned: list[dict], audit_results: dict[str, list[str]]) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    clean = [k for k, v in audit_results.items() if not v]
    flagged = {k: v for k, v in audit_results.items() if v}

    lines = [
        "=" * 60,
        f"DSLF TICKET SCAN REPORT",
        f"Generated : {now}",
        f"Tickets scanned: {len(scanned)}",
        f"Clean: {len(clean)}   Flagged: {len(flagged)}",
        "=" * 60,
    ]

    if flagged:
        lines.append("\nFLAGGED TICKETS:")
        for key, issues in flagged.items():
            summary = next((t["fields"]["summary"] for t in scanned if t["key"] == key), "")
            lines.append(f"\n  {key} — {summary}")
            for issue in issues:
                lines.append(f"    • {issue}")
    else:
        lines.append("\nNo issues found.")

    if clean:
        lines.append(f"\nCLEAN TICKETS: {', '.join(clean)}")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


def save_report(content: str) -> Path:
    REPORTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"scan_{timestamp}.txt"
    path.write_text(content)
    return path


# --- Learn from reporter ---

def fetch_tickets_by_reporter(reporter_display_name: str) -> list[dict]:
    """Fetch all DSLF tickets created by a specific reporter, oldest first."""
    all_issues = []
    start = 0
    batch = 50
    jql = f'project = {PROJECT_KEY} AND reporter = "{reporter_display_name}" ORDER BY created ASC'

    while True:
        params = {
            "jql": jql,
            "startAt": start,
            "maxResults": batch,
            "fields": ",".join(FIELDS),
        }
        resp = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=_auth(),
            headers={"Accept": "application/json"},
            params=params,
            timeout=20,
        )
        if resp.status_code != 200:
            log.error("Jira search failed: %s %s", resp.status_code, resp.text[:200])
            break

        data = resp.json()
        issues = data.get("issues", [])
        all_issues.extend(issues)

        if start + batch >= data.get("total", 0):
            break
        start += batch

    return all_issues


def learn_from_tickets(issues: list[dict]) -> int:
    """
    Extract List Name → DB code mappings from tickets and save to learned_patterns.json.
    Keys by List Name (the donor list being rented) — stable, consistent with YAML structure.
    Falls back to Mailer Name if List Name is empty.
    Skips tickets missing Billable Account or Client Database.
    Returns count of patterns saved.
    """
    patterns = {}
    if LEARNED_PATTERNS_FILE.exists():
        try:
            patterns = json.loads(LEARNED_PATTERNS_FILE.read_text())
        except Exception:
            patterns = {}

    learned = 0
    for issue in issues:
        key    = issue["key"]
        fields = issue["fields"]

        # Key by List Name (which list is being rented — determines DB code)
        # Fall back to Mailer Name only if List Name is absent
        list_name   = (fields.get("customfield_12234") or "").strip().upper()
        mailer_name = (fields.get("customfield_12194") or "").strip().upper()
        pattern_key = list_name or mailer_name
        if not pattern_key:
            continue

        billable  = _select_val(issue, "customfield_12191")
        client_db = _select_val(issue, "customfield_12155")
        seed_db   = _select_val(issue, "customfield_12156")

        if not billable or not client_db:
            log.debug("  %s skipped — missing Billable or Client DB", key)
            continue

        list_manager    = (fields.get("customfield_12231") or "").strip()
        requestor_name  = (fields.get("customfield_12232") or "").strip()
        requestor_email = (fields.get("customfield_12233") or "").strip()
        ship_to_email   = (fields.get("customfield_12275") or "").strip()
        shipping_method = _select_val(issue, "customfield_12276")

        entry = patterns.setdefault(pattern_key, {
            "billable_account": "",
            "client_db": "",
            "seed_db": "",
            "list_manager": "",
            "mailer_name": "",
            "requestor_name": "",
            "requestor_email": "",
            "ship_to_email": "",
            "shipping_method": "",
            "seen_in": [],
        })

        # Latest ticket wins for DB codes (list→DB mapping is stable)
        entry["billable_account"] = billable
        entry["client_db"]        = client_db
        if seed_db:
            entry["seed_db"] = seed_db
        if list_manager:
            entry["list_manager"] = list_manager
        if mailer_name:
            entry["mailer_name"] = mailer_name
        if requestor_name:
            entry["requestor_name"] = requestor_name
        if requestor_email:
            entry["requestor_email"] = requestor_email
        if ship_to_email:
            entry["ship_to_email"] = ship_to_email
        if shipping_method:
            entry["shipping_method"] = shipping_method
        if key not in entry["seen_in"]:
            entry["seen_in"].append(key)

        learned += 1
        log.info("  Learned: %s → %s / %s / %s", pattern_key, billable, client_db, seed_db)

    LEARNED_PATTERNS_FILE.write_text(json.dumps(patterns, indent=2))
    return learned


def run_learn(reporter: str) -> None:
    log.info("Fetching tickets by reporter: %r", reporter)
    issues = fetch_tickets_by_reporter(reporter)
    if not issues:
        log.warning("No tickets found for reporter %r", reporter)
        return
    log.info("Found %d ticket(s) — extracting patterns...", len(issues))
    count = learn_from_tickets(issues)
    log.info("Done. %d pattern(s) saved to %s", count, LEARNED_PATTERNS_FILE)


# --- Main ---

def run_scan():
    state = load_state()
    last_number = state["last_ticket_number"]

    log.info("Scanning DSLF tickets after #%d ...", last_number)
    issues = fetch_new_tickets(last_number)

    if not issues:
        log.info("No new tickets found.")
        state["last_scan"] = datetime.now().isoformat()
        save_state(state)
        return

    log.info("Found %d new ticket(s).", len(issues))

    audit_results = {}
    for issue in issues:
        key = issue["key"]
        problems = audit_ticket(issue)
        audit_results[key] = problems
        status = "FLAGGED" if problems else "OK"
        log.info("  %s [%s] %s", key, status, issue["fields"].get("summary", ""))

    report = generate_report(issues, audit_results)
    report_path = save_report(report)
    log.info("Report saved: %s", report_path)
    print("\n" + report)

    # Update state to the highest ticket number seen
    max_number = max(ticket_number(i["key"]) for i in issues)
    state["last_ticket_number"] = max_number
    state["last_scan"] = datetime.now().isoformat()
    save_state(state)


def main():
    parser = argparse.ArgumentParser(description="DSLF ticket scanner")
    parser.add_argument("--loop", type=int, metavar="MINUTES",
                        help="Run repeatedly every N minutes")
    parser.add_argument("--reset", action="store_true",
                        help="Clear saved state and scan all tickets")
    parser.add_argument("--learn", action="store_true",
                        help="Learn field patterns from a reporter's tickets")
    parser.add_argument("--reporter", default="Lee Ann Hazelwood",
                        help="Reporter display name to learn from (default: Lee Ann Hazelwood)")
    args = parser.parse_args()

    if args.learn:
        run_learn(args.reporter)
        return

    if args.reset:
        STATE_FILE.unlink(missing_ok=True)
        log.info("State reset — will scan all tickets.")

    if args.loop:
        log.info("Scheduler started — running every %d minute(s). Ctrl+C to stop.", args.loop)
        while True:
            try:
                run_scan()
            except Exception as e:
                log.error("Scan failed: %s", e)
            log.info("Next scan in %d minute(s).", args.loop)
            time.sleep(args.loop * 60)
    else:
        run_scan()


if __name__ == "__main__":
    main()
