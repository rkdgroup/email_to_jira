"""One-off via ODBC: create WOs for DSLF-1344 / DSLF-1345 and write WO# back to Jira.

Both were created by the Jenkins pipeline on 2026-09-25 with the WO step failing
silently (as did DSLF-1342, backfilled by hand, and DSLF-1343). Same allocator as
the pipeline (WorkOrderManager.allocate_and_create: PEPBK# floor, cross-suffix
verify + backout), but connected through the IBM i Access ODBC Driver because
JPype is blocked on the dev machine. DRY RUN by default; --live writes.
"""
import os
import sys
import json
import logging
from pathlib import Path

import pyodbc

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

from work_order import WorkOrderManager, _billable_to_wccust, _make_acronym, _today_mmddyy
from tools_jira import update_ticket_fields, get_ticket_billable_account

TICKETS = [
    {"key": "DSLF-1344", "mailer": "SILENT CRY FOUNDATION", "manager_po": "J5328", "mailer_po": "133323"},
    {"key": "DSLF-1345", "mailer": "SILENT CRY FOUNDATION", "manager_po": "J5326", "mailer_po": "133349"},
]


class OdbcWorkOrderManager(WorkOrderManager):
    def _connect(self):
        return pyodbc.connect(
            "DRIVER={IBM i Access ODBC Driver};"
            f"SYSTEM={os.environ.get('IBMI_HOST', 'SYSTEM5.DATA-MANAGEMENT.COM')};"
            f"UID={os.environ.get('IBMI_USER', 'DMISUVAM')};PWD={os.environ['IBMI_PASSWORD']}",
            timeout=30, autocommit=True,
        )


def main() -> None:
    live = "--live" in sys.argv
    mgr = OdbcWorkOrderManager()
    for t in TICKETS:
        billable = get_ticket_billable_account(t["key"])
        if not billable:
            print(json.dumps({"key": t["key"], "error": "no billable account on ticket"}), flush=True)
            continue
        wccust = _billable_to_wccust(billable)
        wo = mgr.allocate_and_create(
            wccust=wccust, worde3=_today_mmddyy(), mailer_name=t["mailer"],
            manager_po=t["manager_po"], mailer_po=t["mailer_po"], dry_run=not live,
        )
        out = {"key": t["key"], "billable": billable, "wccust": wccust,
               "wdesc": _make_acronym(t["mailer"]), "wo_number": wo, "live": live}
        if live:
            out["jira_update"] = update_ticket_fields(t["key"], {"customfield_12089": str(wo)})
        print(json.dumps(out), flush=True)
    print("DONE" if live else "DRY RUN - pass --live to write", flush=True)


if __name__ == "__main__":
    main()
