"""
One-off: add the client's "Dollar Cap:" line to the Description of existing tickets.

The durable fix is in parse_pipeline._build_adf_description, which now emits the cap on
every new ticket beside "Select By:". This script is the separate correction for tickets
created before that — it does not need to run again once the queue has drained.

Why the cap has to be on the ticket at all: "$10+" on an order is not an open-ended floor.
It means $10.00 through THIS client's contracted cap, recorded per client in
config/client_profiles.yaml (60 clients at $99.99, 48 at $49.99, a tail at $249.99 /
$499.99 / $999.99, some NO CAP, 37 VARIES PER ORDER). Without the cap written down, neither
a human nor qc_llm can tell a correct pull ("RECENT PAYMENT AMT. = 10.00 THRU 99.99") from
one that quietly lost every donor above $99.99.

    python backfill_dollar_cap.py                      # DRY RUN over Needs Assignment
    python backfill_dollar_cap.py --live               # write
    python backfill_dollar_cap.py --status "Needs QC"  # a different queue
    python backfill_dollar_cap.py DSLF-1075 --live     # named tickets only

The cap is written exactly as the profile records it. "NO CAP" and "VARIES PER ORDER" are
not normalised to anything, because each means something different to whoever reads it.
"""

import sys
import logging
import argparse
from pathlib import Path

_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))

log = logging.getLogger(__name__)

CAP_LABEL = "Dollar Cap:"


def _para_text(node: dict) -> str:
    """The plain text of one ADF paragraph node."""
    if not isinstance(node, dict) or node.get("type") != "paragraph":
        return ""
    return "".join(c.get("text", "") for c in node.get("content") or []
                   if isinstance(c, dict))


def insert_cap(adf, cap: str) -> dict | None:
    """Return a copy of `adf` with a cap paragraph inserted, or None if no change is due.

    Position matches what _build_adf_description produces on a fresh ticket: immediately
    after "Select By:", otherwise ahead of the first profile block, otherwise at the end.
    Nothing else in the document is touched — the bullet lists that carry the suppressions
    and the priced selects are left exactly as they are.
    """
    if not isinstance(adf, dict) or adf.get("type") != "doc":
        return None
    content = list(adf.get("content") or [])

    texts = [_para_text(n) for n in content]
    if any(t.strip().upper().startswith(CAP_LABEL.upper()) for t in texts):
        return None  # already carries a cap line

    at = None
    for i, t in enumerate(texts):
        if t.strip().upper().startswith("SELECT BY:"):
            at = i + 1
            break
    if at is None:
        for i, t in enumerate(texts):
            if t.strip().upper().startswith(("STANDARD SUPPRESSIONS:", "SPECIAL INSTRUCTIONS:")):
                at = i
                break
    if at is None:
        at = len(content)

    content.insert(at, {"type": "paragraph",
                        "content": [{"type": "text", "text": f"{CAP_LABEL} {cap}"}]})
    return {"type": "doc", "version": 1, "content": content}


def process(ticket_key: str, live: bool = False) -> dict:
    """Look up the cap for one ticket and add it. Never raises."""
    from tools_jira import get_ticket_qc_fields, update_ticket_fields
    from parse_pipeline import _PROFILE_MAP

    fields = get_ticket_qc_fields(ticket_key)
    if "error" in fields:
        return {"key": ticket_key, "status": "error", "detail": fields["error"]}

    db = str(fields.get("client_db") or "").upper()
    if not db:
        return {"key": ticket_key, "status": "skipped",
                "detail": "no Client Database on the ticket — nothing to look the cap up by"}

    prof = _PROFILE_MAP.get(db) or _PROFILE_MAP.get(db[:-1])
    cap = (prof or {}).get("dollar_cap") or ""
    if not cap:
        return {"key": ticket_key, "status": "skipped",
                "detail": f"no dollar_cap recorded for {db}"}

    new_adf = insert_cap(fields.get("description_adf"), cap)
    if new_adf is None:
        return {"key": ticket_key, "status": "unchanged",
                "detail": f"already states a cap, or the description is not an ADF doc"}

    if not live:
        return {"key": ticket_key, "status": "would-add", "detail": f"{db} -> {cap}"}

    r = update_ticket_fields(ticket_key, {"description": new_adf})
    if "error" in r:
        return {"key": ticket_key, "status": "error", "detail": r["error"]}
    return {"key": ticket_key, "status": "added", "detail": f"{db} -> {cap}"}


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env")

    ap = argparse.ArgumentParser(description="Add the client's Dollar Cap line to existing "
                                             "ticket descriptions.")
    ap.add_argument("tickets", nargs="*", help="ticket keys; default is the whole queue")
    ap.add_argument("--status", default="Needs Assignment", help="queue to scan")
    ap.add_argument("--live", action="store_true",
                    help="actually write (default is a dry run)")
    args = ap.parse_args()

    keys = args.tickets
    if not keys:
        from tools_jira import search_issues_paged
        jql = f'project = DSLF AND status = "{args.status}" ORDER BY created ASC'
        keys = [i["key"] for i in search_issues_paged(jql, "summary")]
        print(f"{len(keys)} ticket(s) in {args.status!r}"
              f"{'' if args.live else '  [DRY RUN]'}\n")
    if not keys:
        print("Nothing to do.")
        return 0

    tally: dict = {}
    for k in keys:
        r = process(k, live=args.live)
        tally[r["status"]] = tally.get(r["status"], 0) + 1
        print(f"  {r['status']:<10} {r['key']:<12} {r['detail']}")

    print("\n" + "  ".join(f"{v} {k}" for k, v in sorted(tally.items())))
    if not args.live and tally.get("would-add"):
        print("\nDry run — nothing was written. Re-run with --live to apply.")
    return 1 if tally.get("error") else 0


if __name__ == "__main__":
    sys.exit(main())
