"""One-off via ODBC: give DSLF-1238 a new WO and write it to the ticket.

DSLF-1238's pipeline WO 467171 (created 2026-09-10) no longer exists in
DMIJOBS.ARWRKSCH and no other row carries DM120, so it was deleted downstream
(the file is unjournaled). Allocates with the pipeline's own allocator.
DRY RUN by default; --live writes.
"""
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

from _backfill_1343_1347 import OdbcWorkOrderManager
from work_order import _billable_to_wccust, _make_acronym, _today_mmddyy, _WO_LIBRARY
from tools_jira import update_ticket_fields, get_ticket_billable_account, search_issues_paged

TICKET, OLD_WO = "DSLF-1238", "467171"
MAILER, MANAGER_PO, MAILER_PO = "BETHANY CHRISTIAN SERVICES", "DM120", "78986-ST"


def main() -> None:
    live = "--live" in sys.argv
    current = search_issues_paged(f"key = {TICKET}", "customfield_12089")[0]["fields"]["customfield_12089"]
    if current != OLD_WO:
        sys.exit(f"{TICKET} WO is {current!r}, expected {OLD_WO} — someone changed it; aborting.")

    mgr = OdbcWorkOrderManager()
    conn = mgr._connect()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT WWORKO, WSUFX FROM {_WO_LIBRARY}.ARWRKSCH WHERE TRIM("WXCOD#") = ?', [MANAGER_PO])
        rows = cur.fetchall()
    finally:
        conn.close()
    if rows:
        sys.exit(f"ARWRKSCH already has a row for {MANAGER_PO}: {rows} — aborting.")

    billable = get_ticket_billable_account(TICKET)
    if not billable:
        sys.exit("No billable account on ticket — aborting.")
    wccust = _billable_to_wccust(billable)
    wo = mgr.allocate_and_create(
        wccust=wccust, worde3=_today_mmddyy(), mailer_name=MAILER,
        manager_po=MANAGER_PO, mailer_po=MAILER_PO, dry_run=not live,
    )
    out = {"key": TICKET, "old_wo": OLD_WO, "billable": billable, "wccust": wccust,
           "wdesc": _make_acronym(MAILER), "wo_number": wo, "live": live}
    if live:
        out["jira_update"] = update_ticket_fields(TICKET, {"customfield_12089": str(wo)})
    print(json.dumps(out), flush=True)
    print("DONE" if live else "DRY RUN - pass --live to write", flush=True)


if __name__ == "__main__":
    main()
