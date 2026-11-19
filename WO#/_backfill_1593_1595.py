"""One-off: create WOs for DSLF-1593 / DSLF-1595 (IBM i refused the connection 2026-11-16 13:15).

Runs on the Jenkins box (rkdc2-dmiappsdev01) via the pipeline's own jt400 connection
(WorkOrderManager / base.get_connection) and the copied .env. DRY RUN by default.
DSLF-1591 / DSLF-1594 are duplicates of DSLF-1592 / DSLF-1596 and get NO WO.

    python3 _backfill_1593_1595.py --check   # read-only: rows already keyed for these orders
    python3 _backfill_1593_1595.py           # dry run: candidate WO per ticket
    python3 _backfill_1593_1595.py --live    # insert, log to _backfill_1593_1595.jsonl, write WO# to Jira
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
from work_order import WorkOrderManager, _billable_to_wccust, _make_acronym, _today_mmddyy

_LOG = Path(__file__).with_suffix(".jsonl")

# Copied from the Jira fields 2026-11-19 (cf12191 billable, cf12194 mailer, cf12192 manager order, cf12193 mailer PO).
TICKETS = [
    ("DSLF-1593", "N91", "CITIZENS BEHIND THE BADGE", "J6794", "674032"),
    ("DSLF-1595", "N71", "CITIZENS BEHIND THE BADGE", "J6793", "674031"),
]
# Same batch, created fine by the pipeline (WO 469858): proves the check query finds a recent row.
_CONTROL = ("J6796", "674026")


def check() -> None:
    """Read-only. Values are vetted literals inlined (WXCOD# is CHAR(9); bound params can 22001)."""
    values = [v for t in TICKETS for v in (t[3], t[4])] + list(_CONTROL)
    lits = ",".join(f"'{v}'" for v in values)
    conn = WorkOrderManager()._connect()
    try:
        cur = conn.cursor()
        cur.execute(
            'SELECT WWORKO, WSUFX, WCCUST, TRIM(WDESC), TRIM("WXCOD#"), TRIM(WMAILR), WORDE3 '
            f'FROM DMIJOBS.ARWRKSCH WHERE TRIM("WXCOD#") IN ({lits}) OR TRIM(WMAILR) IN ({lits})'
        )
        rows = cur.fetchall()
        print(f"rows matching {values}: {len(rows)}")
        for r in rows:
            print("  ", tuple(r))
        if not any(_CONTROL[0] in str(r) for r in rows):
            print("WARNING: control J6796 (WO 469858) not found - do NOT trust a zero above")
        cur.execute("SELECT MAX(WWORKO) FROM DMIJOBS.ARWRKSCH WHERE WWORKO < 500000")
        print("MAX(WWORKO):", cur.fetchone()[0])
    finally:
        conn.close()


def main() -> None:
    if "--check" in sys.argv:
        return check()
    live = "--live" in sys.argv
    mgr = WorkOrderManager()
    for key, billable, mailer, manager_po, mailer_po in TICKETS:
        wccust = _billable_to_wccust(billable)
        wo = mgr.allocate_and_create(
            wccust=wccust, worde3=_today_mmddyy(), mailer_name=mailer,
            manager_po=manager_po, mailer_po=mailer_po, dry_run=not live,
        )
        out = {"key": key, "billable": billable, "wccust": wccust,
               "wdesc": _make_acronym(mailer), "wo_number": wo, "live": live}
        if live:
            # Durable before Jira is touched, so a WO can never lose its ticket.
            with _LOG.open("a", encoding="utf-8") as f:
                f.write(json.dumps(out) + "\n")
            from tools_jira import update_ticket_fields
            res = update_ticket_fields(key, {"customfield_12089": str(wo)})
            out["jira"] = "error: " + str(res["error"]) if isinstance(res, dict) and res.get("error") else "ok"
        print(json.dumps(out), flush=True)
    print("DONE" if live else "DRY RUN - pass --live to write", flush=True)


if __name__ == "__main__":
    main()
