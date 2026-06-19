"""
Qty Approval Queue Scanner
1. Scans service-account inbox for unread "NPA/QTY APPROVAL/<order#>" emails.
   - Parses approved qty from body (format: "<order#> = <qty>")
   - Updates Requested Quantity on the matching Jira ticket
   - Transitions ticket out of "Waiting on Qty Approval"
   - Marks the email as read so it is not reprocessed
2. For tickets still waiting (no approval email found), downloads the SELECT PDF
   and reads TOTAL RECORDS SELECTED as a fallback qty.
3. Builds a report and emails it.

Usage:
    python qty_approval_scanner.py
    python qty_approval_scanner.py --output report.txt
    python qty_approval_scanner.py --no-email-scan   # skip inbox scan, PDF only
"""

import os
import re
import sys
import argparse
import tempfile
import requests
from collections import defaultdict
from dotenv import load_dotenv
from pathlib import Path
from requests.auth import HTTPBasicAuth

load_dotenv(Path(__file__).parent / ".env")

sys.path.insert(0, str(Path(__file__).parent))
from tools_pdf import extract_pdf_text
from qc_checker import parse_select_pdf

JIRA_BASE_URL  = os.getenv("JIRA_BASE_URL", "https://rkdgroup.atlassian.net")
JIRA_EMAIL     = os.getenv("JIRA_EMAIL")
JIRA_TOKEN     = os.getenv("JIRA_API_TOKEN")
MS_CLIENT_ID   = os.getenv("MS_CLIENT_ID", "")
MS_TENANT_ID   = os.getenv("MS_TENANT_ID", "common")
MS_CLIENT_SECRET    = os.getenv("MS_CLIENT_SECRET", "")
MS_SERVICE_ACCOUNT  = os.getenv("MS_SERVICE_ACCOUNT", "")
MS_SERVICE_PASSWORD = os.getenv("MS_SERVICE_PASSWORD", "")
GRAPH_BASE     = "https://graph.microsoft.com/v1.0"
STATUS         = "Waiting on Qty Approval"
PROJECT        = "DSLF"

DEFAULT_EMAIL_TO = "smondal@data-management.com"
DEFAULT_EMAIL_CC = "smondal@data-management.com"

FIELDS = [
    "summary",
    "attachment",
    "customfield_12192",  # Manager Order Number
    "customfield_12193",  # Mailer PO
    "customfield_12194",  # Mailer Name
    "customfield_12234",  # List Name
    "customfield_12271",  # Requested Quantity (fallback)
]

# Matches: "NPA/QTY APPROVAL/J2044", "QTY APPROVAL J2044", etc.
_SUBJECT_RE = re.compile(r'QTY\s*APPROVAL[/\s]+([A-Z0-9\-]+)', re.IGNORECASE)
# Matches body line: "J2044 = 3570" or "J2044=3,570"
_BODY_QTY_RE = re.compile(r'\b[A-Z0-9\-]+=\s*([\d,]+)', re.IGNORECASE)


def _auth():
    return HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN)


def _get_ms_token() -> str:
    import msal
    app = msal.ConfidentialClientApplication(
        MS_CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{MS_TENANT_ID}",
        client_credential=MS_CLIENT_SECRET,
    )
    result = app.acquire_token_by_username_password(
        username=MS_SERVICE_ACCOUNT,
        password=MS_SERVICE_PASSWORD,
        scopes=["Mail.ReadWrite", "Mail.Send"],
    )
    if "access_token" not in result:
        sys.exit(f"MS auth failed: {result.get('error_description', result)}")
    return result["access_token"]


def send_email(to: str, subject: str, body: str, cc: str = "") -> None:
    token = _get_ms_token()
    msg = {
        "subject": subject,
        "body": {"contentType": "Text", "content": body},
        "toRecipients": [{"emailAddress": {"address": a.strip()}}
                         for a in to.split(",")],
        "replyTo": [{"emailAddress": {"address": "smondal@data-management.com"}}],
    }
    if cc:
        msg["ccRecipients"] = [{"emailAddress": {"address": a.strip()}}
                               for a in cc.split(",")]
    payload = {"message": msg, "saveToSentItems": True}
    resp = requests.post(
        f"{GRAPH_BASE}/users/{MS_SERVICE_ACCOUNT}/sendMail",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload, timeout=30,
    )
    resp.raise_for_status()
    print(f"Email sent to {to}")


# ---------------------------------------------------------------------------
# Email scanning for qty approvals
# ---------------------------------------------------------------------------

def scan_approval_emails() -> dict[str, int]:
    """
    Scan the service-account inbox for unread NPA/QTY APPROVAL emails.

    Subject format:  NPA/QTY APPROVAL/J2044
    Body format:     J2044 = 3570

    Returns {manager_order_number (uppercased): approved_qty}.
    Marks matched emails as read so they are not reprocessed on the next run.
    Returns {} silently if MS credentials are not configured.
    """
    if not MS_CLIENT_ID or not MS_SERVICE_ACCOUNT:
        return {}

    try:
        token = _get_ms_token()
    except SystemExit as exc:
        print(f"Warning: MS auth unavailable — skipping inbox scan. ({exc})")
        return {}

    headers_auth = {"Authorization": f"Bearer {token}"}

    # Search for unread messages containing QTY APPROVAL in the subject.
    # Graph $search uses KQL; "subject:" prefix scopes to the subject field.
    url = f"{GRAPH_BASE}/users/{MS_SERVICE_ACCOUNT}/messages"
    params = {
        "$search": '"subject:QTY APPROVAL"',
        "$select": "id,subject,body,isRead",
        "$top": 100,
    }

    try:
        resp = requests.get(url, headers=headers_auth, params=params, timeout=15)
        resp.raise_for_status()
    except Exception as exc:
        print(f"Warning: Could not scan approval emails: {exc}")
        return {}

    approvals: dict[str, int] = {}
    to_mark_read: list[str] = []

    for msg in resp.json().get("value", []):
        subject = msg.get("subject", "")
        m = _SUBJECT_RE.search(subject)
        if not m:
            continue

        order_num = m.group(1).strip().upper()

        # Strip HTML tags from body
        raw_body = msg.get("body", {}).get("content", "")
        body_text = re.sub(r'<[^>]+>', ' ', raw_body)
        body_text = re.sub(r'&[a-z]+;', ' ', body_text)

        qty_m = _BODY_QTY_RE.search(body_text)
        if not qty_m:
            continue

        qty = int(qty_m.group(1).replace(',', ''))

        # Keep the first (most recent, since Graph returns newest first) match per order
        if order_num not in approvals:
            approvals[order_num] = qty
            if not msg.get("isRead"):
                to_mark_read.append(msg["id"])

    # Mark processed emails as read
    for msg_id in to_mark_read:
        try:
            requests.patch(
                f"{GRAPH_BASE}/users/{MS_SERVICE_ACCOUNT}/messages/{msg_id}",
                headers={**headers_auth, "Content-Type": "application/json"},
                json={"isRead": True},
                timeout=10,
            )
        except Exception:
            pass  # non-fatal

    if approvals:
        print(f"  Found {len(approvals)} approval email(s): {', '.join(approvals.keys())}")

    return approvals


# ---------------------------------------------------------------------------
# Applying an approval to a Jira ticket
# ---------------------------------------------------------------------------

def _transition_ticket(key: str) -> bool:
    """
    Transition a ticket out of Waiting on Qty Approval.
    Tries transitions whose name contains 'approv' or whose target status
    is 'in progress' / 'ready'. Falls back to the first non-waiting transition.
    Returns True if a transition was applied.
    """
    resp = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{key}/transitions",
        auth=_auth(), timeout=15,
    )
    if not resp.ok:
        return False

    transitions = resp.json().get("transitions", [])
    target_id = None

    for t in transitions:
        name      = t.get("name", "").lower()
        to_status = (t.get("to") or {}).get("name", "").lower()
        if "approv" in name or "in progress" in to_status or "ready" in name:
            target_id = t["id"]
            break

    if not target_id:
        for t in transitions:
            to_status = (t.get("to") or {}).get("name", "").lower()
            if "waiting" not in to_status and "qty" not in to_status:
                target_id = t["id"]
                break

    if not target_id:
        return False

    r = requests.post(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{key}/transitions",
        auth=_auth(),
        headers={"Content-Type": "application/json"},
        json={"transition": {"id": target_id}},
        timeout=15,
    )
    return r.ok


def apply_qty_approval(ticket: dict, qty: int) -> str:
    """
    Update Requested Quantity (customfield_12271) and transition the ticket.
    Returns one of: "updated+transitioned", "updated", "failed".
    """
    key = ticket["key"]

    # Update quantity field
    resp = requests.put(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{key}",
        auth=_auth(),
        headers={"Content-Type": "application/json"},
        json={"fields": {"customfield_12271": qty}},
        timeout=15,
    )
    if not resp.ok:
        print(f"  ERROR updating {key}: {resp.status_code} {resp.text[:120]}")
        return "failed"

    transitioned = _transition_ticket(key)
    return "updated+transitioned" if transitioned else "updated"


# ---------------------------------------------------------------------------
# Queue fetching and SELECT PDF enrichment (unchanged)
# ---------------------------------------------------------------------------

def _get_select_qty(attachments: list) -> tuple[int | None, str]:
    select_att = next(
        (a for a in attachments if "SELECT" in a.get("filename", "").upper()
         and a.get("filename", "").upper().endswith(".PDF")),
        None,
    )
    if not select_att:
        return None, ""

    filename    = select_att["filename"]
    content_url = select_att["content"]
    tmp_path    = os.path.join(tempfile.mkdtemp(prefix="dslf_qty_"), filename)

    try:
        resp = requests.get(content_url, auth=_auth(),
                            headers={"Accept": "*/*"},
                            stream=True, timeout=60)
        resp.raise_for_status()
        with open(tmp_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)
    except Exception:
        return None, filename

    data  = parse_select_pdf(tmp_path)
    count = data.get("total_records") or None
    return count, filename


def fetch_queue() -> list[dict]:
    jql    = f'project = {PROJECT} AND status = "{STATUS}" ORDER BY created ASC'
    url    = f"{JIRA_BASE_URL}/rest/api/3/search/jql"
    tickets = []
    token  = None

    while True:
        params = {"jql": jql, "fields": ",".join(FIELDS), "maxResults": 100}
        if token:
            params["nextPageToken"] = token
        resp = requests.get(url, auth=_auth(),
                            headers={"Accept": "application/json"},
                            params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for issue in data.get("issues", []):
            f = issue.get("fields", {})
            tickets.append({
                "key":           issue["key"],
                "manager_order": f.get("customfield_12192") or "",
                "mailer_po":     f.get("customfield_12193") or "",
                "mailer_name":   f.get("customfield_12194") or "",
                "list_name":     f.get("customfield_12234") or "",
                "req_qty":       f.get("customfield_12271"),
                "attachments":   f.get("attachment") or [],
                "url":           f"{JIRA_BASE_URL}/browse/{issue['key']}",
            })

        pi = data.get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        token = pi.get("endCursor")

    return tickets


def enrich_with_select_qty(tickets: list[dict]) -> None:
    total = len(tickets)
    for i, t in enumerate(tickets, 1):
        print(f"  Fetching SELECT PDF {i}/{total}: {t['key']} ...", end="\r")
        qty, fname = _get_select_qty(t["attachments"])
        t["select_qty"]      = qty
        t["select_filename"] = fname
    if total:
        print(" " * 60, end="\r")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _fmt_qty(t: dict) -> str:
    qty = t.get("select_qty") or t.get("req_qty")
    if qty is None:
        return ""
    return f"  qty={int(qty):,}"


def _abbrev_list_name(name: str) -> str:
    """'3-NCF NATL CAREGIVING FND' → 'NCF',  'BFF- ALZHEIMERS...' → 'BFF', bare codes pass through."""
    m = re.match(r'^3-\s*([A-Z0-9]+)\b', name)
    if m:
        return m.group(1)
    m = re.match(r'^([A-Z]{2,6})\s*-', name)
    if m:
        return m.group(1)
    return name


def _load_mailer_abbrevs() -> dict[str, str]:
    path = Path(__file__).parent / "dslf_list_and_mailer_names.txt"
    result = {}
    in_section = False
    pat = re.compile(r'^\s{2}(.+?)\s{2,}\((\*?)([^=)][^)]*)\)\s*$')
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if "MAILER NAMES" in line and "unique" in line:
                    in_section = True
                    continue
                if not in_section:
                    continue
                m = pat.match(line)
                if m:
                    result[m.group(1).strip().upper()] = m.group(3).strip()
    except FileNotFoundError:
        pass
    return result

_MAILER_ABBREVS = _load_mailer_abbrevs()


def _abbrev_mailer(name: str) -> str:
    return _MAILER_ABBREVS.get(name.strip().upper(), name)



def build_report(waiting: list[dict], processed: list[dict]) -> str:
    from datetime import date
    today = date.today().strftime("%B %d, %Y")
    total = len(waiting) + len(processed)

    lines = [
        "Hi Bobbi,",
        "",
        f"Total tickets pending: {total}",
        "-" * 40,
        "",
    ]

    # --- Auto-approved this run ---
    if processed:
        lines += ["AUTO-APPROVED", "-" * 40]
        for t in processed:
            qty_str  = f"{t['approved_qty']:,}"
            status   = "transitioned" if "transitioned" in t.get("result", "") else "qty updated"
            abbrev   = _abbrev_mailer(t.get("mailer_name", ""))
            lines.append(f"  {abbrev:<15}  {t['manager_order']:<10}  {qty_str:>7}  ({status})")
        lines.append("")

    # --- Still waiting ---
    if waiting:
        lines += [f"PENDING APPROVAL  ({len(waiting)} ticket(s))", "-" * 40, ""]

    by_mailer: dict[str, list[dict]] = defaultdict(list)
    for t in waiting:
        by_mailer[t["mailer_name"]].append(t)

    groups      = {m: ts for m, ts in by_mailer.items() if len(ts) > 1}
    individuals = {m: ts for m, ts in by_mailer.items() if len(ts) == 1}

    if groups:
        lines.append("  Groups")
        for mailer in sorted(groups):
            ticket_list = sorted(groups[mailer], key=lambda x: x["manager_order"])
            lines.append(f"    {_abbrev_mailer(mailer)}  ({len(ticket_list)} orders)")
            for t in ticket_list:
                qty = t.get("select_qty") or t.get("req_qty")
                qty_str = f"{int(qty):,}" if qty is not None else "-"
                lines.append(f"      {t['manager_order']:<10}  {qty_str:>7}")
            lines.append("")

    if individuals:
        lines.append("  Individual Orders")
        for mailer in sorted(individuals):
            t = individuals[mailer][0]
            qty = t.get("select_qty") or t.get("req_qty")
            qty_str = f"{int(qty):,}" if qty is not None else "-"
            list_abbrev = _abbrev_list_name(t.get("list_name", ""))
            lines.append(f"      {list_abbrev:<10}  {t['manager_order']:<10}  {qty_str:>7}")
        lines.append("")

    if not waiting:
        lines.append("  No tickets pending.")

    lines += [
        "",
        "Thanks and regards,",
        "Suvam Mondal",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scan Qty Approval queue")
    parser.add_argument("--output",         help="Write report to this file (default: stdout)")
    parser.add_argument("--email",          default=DEFAULT_EMAIL_TO,
                        help=f"Send report to this address (default: {DEFAULT_EMAIL_TO})")
    parser.add_argument("--subject",        default="DSLF Qty Approval Queue",
                        help="Email subject")
    parser.add_argument("--cc",             default=DEFAULT_EMAIL_CC,
                        help=f"CC address(es), comma-separated (default: {DEFAULT_EMAIL_CC})")
    parser.add_argument("--no-email-scan",  action="store_true",
                        help="Skip inbox scan; use SELECT PDFs only")
    args = parser.parse_args()

    if not JIRA_EMAIL or not JIRA_TOKEN:
        sys.exit("ERROR: JIRA_EMAIL and JIRA_API_TOKEN must be set in .env")

    # 1. Scan inbox for approval emails
    approvals: dict[str, int] = {}
    if not args.no_email_scan:
        print("Scanning inbox for qty approval emails...")
        approvals = scan_approval_emails()

    # 2. Fetch queue
    print("Fetching queue...")
    tickets = fetch_queue()
    print(f"Found {len(tickets)} ticket(s) in '{STATUS}'.")

    # 3. Apply any email approvals
    processed: list[dict] = []
    waiting:   list[dict] = []

    for t in tickets:
        order = (t["manager_order"] or "").strip().upper()
        if order and order in approvals:
            qty    = approvals[order]
            result = apply_qty_approval(t, qty)
            processed.append({**t, "approved_qty": qty, "result": result})
            status_str = "OK" if "updated" in result else "FAILED"
            print(f"  {t['key']}  {order}  qty={qty:,}  [{status_str}]")
        else:
            waiting.append(t)

    # 4. Enrich remaining tickets with SELECT PDF qty
    if waiting:
        print(f"Downloading SELECT PDFs for {len(waiting)} remaining ticket(s)...")
        enrich_with_select_qty(waiting)

    # 5. Build and deliver report
    report = build_report(waiting, processed)

    all_orders = [t["manager_order"] for t in processed + waiting if t.get("manager_order")]
    subject = "QTY APPROVAL/" + "/".join(all_orders) if all_orders else args.subject

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(report + "\n")
        print(f"Report written to {args.output}")

    send_email(args.email, subject, report, cc=args.cc)

    if not args.output:
        print(report)


if __name__ == "__main__":
    main()
