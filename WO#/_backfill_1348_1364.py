"""One-off via ODBC: create WOs for DSLF-1348..1364 (Jenkins .env had no IBMI_PASSWORD).

Same allocator as the pipeline (WorkOrderManager.allocate_and_create). Jira is NOT
touched here: the local Jira token 401s, so the WO# is written back through the
Atlassian connector from the JSONL log this writes. DRY RUN by default.

    python _backfill_1348_1364.py --check   # read-only: rows already keyed for these orders
    python _backfill_1348_1364.py           # dry run: candidate WO per ticket
    python _backfill_1348_1364.py --live    # insert, one JSONL line per WO in _backfill_1348_1364.jsonl
"""
import json
import sys
from pathlib import Path

import pyodbc
from dotenv import dotenv_values

sys.path.insert(0, str(Path(__file__).parent))
from work_order import WorkOrderManager, _billable_to_wccust, _make_acronym, _today_mmddyy

_ENV = dotenv_values(Path(__file__).parent.parent / ".env")
_LOG = Path(__file__).with_suffix(".jsonl")

# Copied from the Jira fields 2026-09-30 (cf12194 mailer, cf12192 manager order, cf12193 mailer PO, cf12191 billable).
TICKETS = [
    # Ticket reads "Catholic Relief Services (#1510194)"; the suffix would make WDESC "CRS(".
    ("DSLF-1348", "W12", "Catholic Relief Services", "70497", "CNE73"),
    ("DSLF-1349", "M84", "PAWS OF HONOR", "J5448", "673043"),
    ("DSLF-1350", "A18", "U.S. DEPUTY SHERIFFS ASSOC.", "J5451", "673059"),
    ("DSLF-1351", "N71", "U.S. DEPUTY SHERIFFS ASSOC.", "J5455", "673066"),
    ("DSLF-1352", "S15", "NATIVE AMER. HERIT. ASSOC", "DM212", "126868"),
    ("DSLF-1353", "P89", "U.S. DEPUTY SHERIFFS ASSOC.", "J5457", "673070"),
    ("DSLF-1354", "P47", "U.S. DEPUTY SHERIFFS ASSOC.", "J5456", "673069"),
    ("DSLF-1355", "A69", "U.S. DEPUTY SHERIFFS ASSOC.", "J5452", "673061"),
    ("DSLF-1356", "N91", "NATIONAL POLICE ASSOCIATION", "J5444", "D01-122992"),
    ("DSLF-1357", "N91", "PAWS OF HONOR", "J5447", "673044"),
    ("DSLF-1358", "B28", "FUND INTEGRATIVE CANCER TREAT.", "J5490", "D01-122998"),
    ("DSLF-1359", "B28", "NATIONAL DIABETES FUND", "J5491", "D01-123049"),
    ("DSLF-1360", "N13", "PAWS OF HONOR", "J5446", "673045"),
    ("DSLF-1361", "N09", "VOLUNTEER FIREFIGHTER ALLIANCE", "J5478", "D01-123095"),
    ("DSLF-1362", "N09", "NATIONAL POLICE ASSOCIATION", "J5477", "D01-123027"),
    ("DSLF-1363", "N09", "NATIONAL POLICE ASSOCIATION", "J5475", "D01-123026"),
    ("DSLF-1364", "N09", "HERITAGE FOUNDATION, THE", "J5463", "L55721"),
]


class OdbcWorkOrderManager(WorkOrderManager):
    def _connect(self):
        return pyodbc.connect(
            "DRIVER={IBM i Access ODBC Driver};"
            f"SYSTEM={_ENV['IBMI_HOST']};UID={_ENV['IBMI_USER']};PWD={_ENV['IBMI_PASSWORD']}",
            timeout=30, autocommit=True,
        )


def check() -> None:
    """Read-only: any ARWRKSCH row already carrying one of these manager orders or mailer POs."""
    mgr_pos = [t[3] for t in TICKETS]
    mailer_pos = [t[4] for t in TICKETS]
    marks = ",".join("?" * len(TICKETS))
    conn = OdbcWorkOrderManager()._connect()
    try:
        cur = conn.cursor()
        cur.execute(
            'SELECT WWORKO, WSUFX, WCCUST, TRIM(WDESC), TRIM("WXCOD#"), TRIM(WMAILR), WORDE3 '
            f'FROM DMIJOBS.ARWRKSCH WHERE TRIM("WXCOD#") IN ({marks}) OR TRIM(WMAILR) IN ({marks})',
            mgr_pos + mailer_pos,
        )
        rows = cur.fetchall()
        print(f"existing rows matching these orders: {len(rows)}")
        for r in rows:
            print("  ", tuple(r))
        cur.execute("SELECT MAX(WWORKO) FROM DMIJOBS.ARWRKSCH WHERE WWORKO < 500000")
        print("MAX(WWORKO):", cur.fetchone()[0])
    finally:
        conn.close()


def main() -> None:
    if "--check" in sys.argv:
        return check()
    live = "--live" in sys.argv
    mgr = OdbcWorkOrderManager()
    for key, billable, mailer, manager_po, mailer_po in TICKETS:
        wccust = _billable_to_wccust(billable)
        wo = mgr.allocate_and_create(
            wccust=wccust, worde3=_today_mmddyy(), mailer_name=mailer,
            manager_po=manager_po, mailer_po=mailer_po, dry_run=not live,
        )
        out = {"key": key, "billable": billable, "wccust": wccust,
               "wdesc": _make_acronym(mailer), "wo_number": wo, "live": live}
        if live:
            # Durable before anything else happens, so a WO can never lose its ticket.
            with _LOG.open("a", encoding="utf-8") as f:
                f.write(json.dumps(out) + "\n")
        print(json.dumps(out), flush=True)
    print("DONE" if live else "DRY RUN - pass --live to write", flush=True)


if __name__ == "__main__":
    main()
