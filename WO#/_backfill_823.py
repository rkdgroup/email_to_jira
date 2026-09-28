"""One-off via ODBC: create a fresh WO for DSLF-823 and write WO# back to Jira.

DSLF-823's original WO 464061 was duplicated by a manually keyed Judicial
Watch/Conrad order (blank suffix) on 7/07/2026 — reassigning the ticket to a
clean number so scheduling doesn't tangle the two jobs.

Same logic as work_order.create_work_order, but connects with the
IBM i Access ODBC Driver instead of jt400 JDBC.
"""
import os
import sys
import json
import logging
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

load_dotenv(Path(__file__).parent / ".env")
load_dotenv(_ROOT / ".env", override=False)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

from work_order import _billable_to_wccust, _make_acronym, _today_mmddyy, _WO_LIBRARY, _WO_MAX
from tools_jira import update_ticket_fields, get_ticket_billable_account

TICKET     = "DSLF-823"
OLD_WO     = 464061
MAILER     = "THE ANN CHILDREN'S FUND"
MANAGER_PO = "J2861"
MAILER_PO  = "670816"

HOST = os.environ.get("IBMI_HOST", "SYSTEM5.DATA-MANAGEMENT.COM")
USER = os.environ.get("IBMI_USER", "DMISUVAM")
PWD  = os.environ.get("IBMI_PASSWORD", "")

billable = get_ticket_billable_account(TICKET)
print(f"billable from Jira: {billable!r}", flush=True)
if not billable:
    sys.exit("No billable account on ticket — aborting.")

conn = pyodbc.connect(
    f"DRIVER={{IBM i Access ODBC Driver}};SYSTEM={HOST};UID={USER};PWD={PWD}",
    timeout=30, autocommit=True,
)
print("ODBC connected", flush=True)
cur = conn.cursor()

cur.execute(f"SELECT MAX(WWORKO) FROM {_WO_LIBRARY}.ARWRKSCH WHERE WWORKO < {_WO_MAX}")
candidate = int(cur.fetchone()[0] or 460000) + 1
while True:
    cur.execute(f"SELECT WWORKO FROM {_WO_LIBRARY}.ARWRKSCH WHERE WWORKO = ? FETCH FIRST 1 ROW ONLY",
                candidate)
    if cur.fetchone() is None:
        break
    candidate += 1
print(f"next WO#: {candidate}", flush=True)

wccust = _billable_to_wccust(billable)
worde3 = _today_mmddyy()
cur.execute(
    f"""
    INSERT INTO {_WO_LIBRARY}.ARWRKSCH (
        WTYPE, WWORKO, WSUFX, WSTORE, WCCUST, WSIZE,
        WORDE3, WCOM2, WVOLUM, WMAIL3, WDESC, WRELES,
        WDNB, WREASN, WXPOST, WADVAN, "WXCOD#", WXPOSO,
        WXPCOD, WESTBL, WESTPS, WPSTCD, WORDER, WCOMP,
        WMAIL, WMAILR
    ) VALUES (
        ?, ?, '  ', 0, ?, '     ',
        ?, 0, 0, ?, ?, 'A',
        ' ', '                             ', 0, 0, ?, ' ',
        ' ', 0, 0, 'L', 0, 0,
        0, ?
    )
    """,
    "M1", candidate, wccust, worde3, worde3,
    _make_acronym(MAILER), MANAGER_PO[:9], MAILER_PO[:15],
)

cur.execute(
    f'SELECT WWORKO, WSUFX, WCCUST, WDESC, "WXCOD#", WMAILR, WORDE3, WRELES '
    f"FROM {_WO_LIBRARY}.ARWRKSCH WHERE WWORKO = ?", candidate)
print(f"verify insert: {cur.fetchone()}", flush=True)
conn.close()

print(json.dumps({"key": TICKET, "billable": billable, "old_wo": OLD_WO,
                  "wo_number": candidate, "wccust": wccust}), flush=True)

upd = update_ticket_fields(TICKET, {"customfield_12089": str(candidate)})
print(json.dumps({"jira_update": upd}), flush=True)
print("DONE", flush=True)
