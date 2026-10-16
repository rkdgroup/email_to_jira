"""
Monthly Claude API spend, published as one DSLF Task per month.

    python cost_report.py                    # DRY RUN, last month - prints the totals
    python cost_report.py --month 2026-09    # a given month
    python cost_report.py --month 2026-09 --live   # create / update "API Cost Report - 2026-09"

The source is the QC comment on each ticket, not logs/api_usage.jsonl. That log is wiped on
every Jenkins run (the job rm -rf's and re-clones the repo, and logs/ lives inside it), so
it never holds a month. qc_checker.format_report ends every report with
`model · Ns · $x.xxx`, and that footer is the only cost record that survives.

Not covered, and the report says so: tools_polish (never logged) and manual LLM_writes /
offline tools (local log only).

Issue type is Task, never List Fulfillment, so the report never enters the order workflow,
QC, or the handoff report. Re-running a month updates its ticket instead of adding one.
"""

import re
import sys
import argparse
import logging
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))

log = logging.getLogger(__name__)

TASK_ISSUE_TYPE_ID = "11772"
SUMMARY_PREFIX = "API Cost Report - "
_COST_RE = re.compile(r"·\s*\$(\d+(?:\.\d+)?)\s*$")


def comment_cost(body: str) -> float | None:
    """USD from a QC comment's footer, or None when the footer carries no figure.

    No figure means QC never billed a call (UNVERIFIED before the API) or the model has no
    rate on file - either way there is nothing to add.
    """
    m = _COST_RE.search(body.rstrip())
    return float(m.group(1)) if m else None


def tally(comments: list[tuple[str, str, str]], month: str) -> dict:
    """Totals for `month` (YYYY-MM) from (ticket, created, body) QC comments."""
    runs = unpriced = unverified = 0
    total = 0.0
    tickets = set()
    for ticket, created, body in comments:
        if not created.startswith(month):
            continue
        runs += 1
        tickets.add(ticket)
        if re.search(r"^VERDICT:\s*UNVERIFIED", body, re.MULTILINE):
            unverified += 1
        cost = comment_cost(body)
        if cost is None:
            unpriced += 1
        else:
            total += cost
    return {"month": month, "runs": runs, "tickets": len(tickets), "total": round(total, 3),
            "avg": round(total / (runs - unpriced), 3) if runs > unpriced else 0.0,
            "unpriced": unpriced, "unverified": unverified}


def report_lines(t: dict) -> list[str]:
    return [
        f"Claude API spend - {t['month']}",
        f"Total: ${t['total']:.3f}",
        f"QC runs: {t['runs']} on {t['tickets']} tickets",
        f"Average per priced run: ${t['avg']:.3f}",
        f"Runs with no cost figure: {t['unpriced']} (UNVERIFIED: {t['unverified']})",
        "",
        "Source: the cost footer on each QC CHECK RESULTS comment.",
        "Excludes tools_polish and manual LLM_writes / offline tools - not logged durably.",
    ]


def report_adf(lines: list[str]) -> dict:
    return {"type": "doc", "version": 1, "content": [
        {"type": "paragraph", "content": [{"type": "text", "text": ln}] if ln else []}
        for ln in lines]}


def _prev_month(today: date) -> str:
    y, m = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    return f"{y}-{m:02d}"


def collect(month: str) -> list[tuple[str, str, str]]:
    """Every QC comment on tickets touched since the month began."""
    from tools_jira import search_issues_paged, get_issue_comments
    from qc_checker import _adf_text, _QC_COMMENT_PREFIXES
    # Posting a comment bumps `updated`, so this finds every ticket QC'd in the month.
    # ponytail: one comment GET per ticket (~100-200/month); batch if it grows.
    jql = (f'project = DSLF AND comment ~ "QC CHECK RESULTS" '
           f'AND updated >= "{month}-01" ORDER BY key ASC')
    out = []
    for issue in search_issues_paged(jql, "key"):
        for c in get_issue_comments(issue["key"]):
            body = _adf_text(c.get("body", ""))
            if body.startswith(_QC_COMMENT_PREFIXES):
                out.append((issue["key"], c.get("created", ""), body))
    return out


def publish(month: str, adf: dict) -> str:
    """Create the month's Task, or update it if it exists. Returns the ticket key."""
    import requests
    from tools_jira import (search_issues_paged, update_ticket_fields, _auth,
                            _get_jira_base_url, DSLF_PROJECT_KEY)
    summary = SUMMARY_PREFIX + month
    # Exact match client-side: JQL `~` is a fuzzy text search.
    found = [i for i in search_issues_paged(
                f'project = DSLF AND issuetype = {TASK_ISSUE_TYPE_ID} '
                f'AND summary ~ "API Cost Report"', "summary")
             if i["fields"].get("summary") == summary]
    if found:
        key = found[0]["key"]
        res = update_ticket_fields(key, {"description": adf})
        if "error" in res:
            raise RuntimeError(res["error"])
        return key
    resp = requests.post(f"{_get_jira_base_url()}/rest/api/3/issue", auth=_auth(),
                         headers={"Accept": "application/json",
                                  "Content-Type": "application/json"},
                         json={"fields": {"project": {"key": DSLF_PROJECT_KEY},
                                          "issuetype": {"id": TASK_ISSUE_TYPE_ID},
                                          "summary": summary, "description": adf}},
                         timeout=30)
    if resp.status_code != 201:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()["key"]


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Monthly Claude API cost report -> Jira Task.")
    ap.add_argument("--month", default=_prev_month(date.today()), help="YYYY-MM (default: last month)")
    ap.add_argument("--live", action="store_true", help="create/update the Jira ticket")
    args = ap.parse_args()
    if not re.fullmatch(r"\d{4}-\d{2}", args.month):
        ap.error("--month must be YYYY-MM")

    lines = report_lines(tally(collect(args.month), args.month))
    print("\n".join(lines))
    if not args.live:
        print("\nDRY RUN - pass --live to write the Jira ticket.")
        return 0
    print(f"\nWritten: {publish(args.month, report_adf(lines))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
