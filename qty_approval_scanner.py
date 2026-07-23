"""
Qty Approval Queue Scanner
1. Scans service-account inbox for unread "NPA/QTY APPROVAL/<order#>" emails.
   - Parses approved qty from body (format: "<order#> = <qty>")
   - Updates Requested Quantity on the matching Jira ticket
   - Never transitions the ticket — it stays in "Waiting on Qty Approval"
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

DEFAULT_EMAIL_TO = "ADoyle@data-management.com"
#DEFAULT_EMAIL_TO = "smondal@data-management.com"
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
        "replyTo": [{"emailAddress": {"address": "ADoyle@data-management.com"}}],
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
    Body format:     J2044 = 3570   (the '=' is required; spaces around it optional)

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

        # Require "<order#> = <qty>": the order number from the subject, an '='
        # sign (spaces optional on either side), then the approved quantity.
        # Anchoring to order_num guarantees the qty belongs to this order and
        # not some unrelated "X=N" line in the body.
        qty_pat = re.compile(rf'\b{re.escape(order_num)}\s*=\s*([\d,]+)', re.IGNORECASE)
        qty_m = qty_pat.search(body_text)
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

def apply_qty_approval(ticket: dict, qty: int) -> str:
    """
    Update Requested Quantity (customfield_12271) only.

    The ticket is NEVER transitioned — it stays in 'Waiting on Qty Approval'
    regardless of the result. Returns one of: "updated", "failed".
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

    return "updated"


# ---------------------------------------------------------------------------
# Queue fetching and SELECT PDF enrichment (unchanged)
# ---------------------------------------------------------------------------

def _get_select_qty(attachments: list) -> tuple[int | None, str]:
    select_atts = [a for a in attachments
                   if "SELECT" in a.get("filename", "").upper()
                   and a.get("filename", "").upper().endswith(".PDF")]
    if not select_atts:
        return None, ""
    # Multiple SELECT PDFs (list re-selected) → use the most recently uploaded,
    # matching qc_checker. The first attachment can be a stale earlier count.
    select_att = max(select_atts, key=lambda a: a.get("created", ""))

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


def _load_list_short_codes() -> tuple[dict[str, str], dict[str, str]]:
    """Parse dslf_list_and_mailer_names.txt for list-name -> short-code resolution.

    Returns two maps, both keyed by UPPERCASED name:
      - short_map:   exact list name -> code, from 'NAME  [Short: CODE]' lines
      - code_by_desc: description   -> code, from Section 1 'CODE  =>  DESCRIPTION' lines
    Both patterns are distinctive enough to scan the whole file without section
    bounds ('[Short:' only appears in the list section; '=>' only in Section 1).
    """
    path = Path(__file__).parent / "dslf_list_and_mailer_names.txt"
    short_map: dict[str, str] = {}
    code_by_desc: dict[str, str] = {}
    try:
        txt = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return short_map, code_by_desc
    for m in re.finditer(r'^\s{2}(.+?)\s*\[Short:\s*([A-Z0-9]+)\]\s*$', txt, re.MULTILINE):
        short_map[m.group(1).strip().upper()] = m.group(2)
    for m in re.finditer(r'^\s{2}([A-Z0-9]{2,7})\s*=>\s*([^\n(]+?)\s*(?:\(|$)', txt, re.MULTILINE):
        code_by_desc.setdefault(m.group(2).strip().upper(), m.group(1))
    return short_map, code_by_desc

_LIST_SHORT_CODES, _CODE_BY_DESC = _load_list_short_codes()


def resolve_list_code(name: str) -> str:
    """Resolve a list name to its short code for the subject prefix, or "" to omit.

    Order: existing regex (3-CODE / CODE- / bare code) -> exact '[Short:]' map ->
    Section 1 description->code reverse map -> "". Only ever returns a space-free code.
    """
    n = (name or "").strip()
    if not n:
        return ""
    cand = _abbrev_list_name(n)
    if cand and " " not in cand:
        return cand
    u = n.upper()
    return _LIST_SHORT_CODES.get(u) or _CODE_BY_DESC.get(u) or ""



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


def build_mailer_report(mailer: str, processed: list[dict], waiting: list[dict]) -> str:
    """
    Body is just '<order#> = <qty>' lines, one per order — nothing else.
    e.g.
        J2044 = 3,570
        J2328 = 5,816
    Quantity is the approved qty for processed tickets, otherwise the SELECT
    PDF qty (falling back to the ticket's Requested Quantity).
    """
    rows: list[tuple[str, int | None]] = []
    for t in processed:
        rows.append((t["manager_order"], t.get("approved_qty")))
    for t in waiting:
        rows.append((t["manager_order"], t.get("select_qty") or t.get("req_qty")))

    lines = []
    for order, qty in sorted(rows, key=lambda r: r[0]):
        qty_str = f"{int(qty):,}" if qty is not None else "-"
        lines.append(f"{order} = {qty_str}")

    return "\n".join(lines)


def group_by_mailer(processed: list[dict], waiting: list[dict]) -> dict[str, dict]:
    """Group both processed and waiting tickets by mailer name (blank → '')."""
    groups: dict[str, dict] = defaultdict(lambda: {"processed": [], "waiting": []})
    for t in processed:
        groups[(t.get("mailer_name") or "").strip()]["processed"].append(t)
    for t in waiting:
        groups[(t.get("mailer_name") or "").strip()]["waiting"].append(t)
    return groups


# Minimum length of a consecutive order run before it is collapsed to "first-last".
_COLLAPSE_MIN_RUN = 2


def _collapse_orders(orders: list[str]) -> str:
    """
    Join order numbers with '/', collapsing consecutive runs to 'first-last'.

    A run is 2+ orders sharing the same alpha prefix with numbers incrementing
    by 1 (e.g. J1000, J1001, J1002, J1003). Runs of _COLLAPSE_MIN_RUN or more
    become 'J1000-J1003'; shorter runs and unparseable tokens stay as-is.
    Original tokens are kept at the range endpoints so any zero-padding is
    preserved.
    """
    items: list[tuple[str | None, int | None, str]] = []
    for o in orders:
        m = re.fullmatch(r'([A-Za-z]+)(\d+)', o)
        items.append((m.group(1).upper(), int(m.group(2)), o) if m else (None, None, o))

    # Sort parseable tokens by (prefix, number); unparseable ones fall to the end.
    items.sort(key=lambda x: (x[0] is None, x[0] or "", x[1] if x[1] is not None else 0, x[2]))

    parts: list[str] = []
    i, n = 0, len(items)
    while i < n:
        pfx, num, orig = items[i]
        if pfx is None:
            parts.append(orig)
            i += 1
            continue
        j = i
        while j + 1 < n and items[j + 1][0] == pfx and items[j + 1][1] == items[j][1] + 1:
            j += 1
        if j - i + 1 >= _COLLAPSE_MIN_RUN:
            parts.append(f"{items[i][2]}-{items[j][2]}")
        else:
            parts.extend(items[k][2] for k in range(i, j + 1))
        i = j + 1
    return "/".join(parts)


def _subject_for(mailer: str, processed: list[dict], waiting: list[dict], default: str) -> str:
    """
    Build the subject as '<prefix>/QTY APPROVAL/<orders>'.

    Prefix depends on whether this mailer is an individual or a group:
      - 1 ticket  (individual) -> list-name abbreviation   e.g. NCF/QTY APPROVAL/J2113
      - >1 tickets (group)     -> mailer-name abbreviation  e.g. HF/QTY APPROVAL/J2113/J2114
    When no clean abbreviation is found, the prefix is omitted entirely
    (just 'QTY APPROVAL/J2113'). Group orders collapse consecutive runs to
    a range (see _collapse_orders).
    """
    tickets = processed + waiting
    orders  = [t["manager_order"] for t in tickets if t.get("manager_order")]
    if not orders:
        return default

    if len(tickets) == 1:
        # Individual order -> list-name short code (e.g. NLEOMF/QTY APPROVAL/J3126).
        # resolve_list_code handles 3-CODE / CODE- / bare, then the names-file
        # '[Short:]' and Section-1 maps; returns "" (omit prefix) when unresolved.
        prefix = resolve_list_code(tickets[0].get("list_name") or "")
    else:
        cand = _abbrev_mailer(mailer) if mailer else ""
        # Only use it if the lookup actually abbreviated the mailer to a clean code.
        prefix = cand if cand and cand != mailer and " " not in cand else ""

    head = f"{prefix}/QTY APPROVAL" if prefix else "QTY APPROVAL"
    return f"{head}/" + _collapse_orders(orders)


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
    parser.add_argument("--combined",        action="store_true",
                        help="Send one combined digest email instead of one email per mailer")
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

    # 5. Build and deliver report(s)
    if args.combined:
        # Single digest email (legacy behavior)
        report  = build_report(waiting, processed)
        subject = _subject_for("", processed, waiting, args.subject)
        send_email(args.email, subject, report, cc=args.cc)
        out_chunks = [report]
    else:
        # One email per mailer, all to the same recipient
        groups     = group_by_mailer(processed, waiting)
        out_chunks = []
        for mailer in sorted(groups):
            g       = groups[mailer]
            report  = build_mailer_report(mailer, g["processed"], g["waiting"])
            subject = _subject_for(mailer, g["processed"], g["waiting"], args.subject)
            mailer_disp = _abbrev_mailer(mailer) if mailer else "Unspecified Mailer"
            print(f"  Sending {mailer_disp}: "
                  f"{len(g['processed'])} approved, {len(g['waiting'])} pending")
            send_email(args.email, subject, report, cc=args.cc)
            out_chunks.append(report)
        if not groups:
            print("  No tickets to report.")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(("\n" + "=" * 60 + "\n\n").join(out_chunks) + "\n")
        print(f"Report written to {args.output}")
    else:
        print(("\n" + "=" * 60 + "\n\n").join(out_chunks))


if __name__ == "__main__":
    main()
