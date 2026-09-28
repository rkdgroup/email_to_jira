"""One-off backfill: create the WO for DSLF-671 and write WO# back to Jira.

Mirrors parse_pipeline._create_and_link_work_order: re-reads the billable
account from the Jira ticket, inserts into DMIJOBS.ARWRKSCH, then updates
customfield_12089 on the ticket.
"""
import sys
import json
import logging
from pathlib import Path

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

from work_order import create_work_order
from tools_jira import update_ticket_fields, get_ticket_billable_account

TICKET = "DSLF-671"

billable = get_ticket_billable_account(TICKET)
print(f"billable from Jira: {billable!r}", flush=True)
if not billable:
    sys.exit("No billable account on ticket — aborting.")

wo = create_work_order(
    billable=billable,
    mailer_name="PAWS OF HONOR",
    manager_po="J2163",
    mailer_po="670239",
)
print(json.dumps({"key": TICKET, "billable": billable,
                  "wo_number": wo.wo_number, "wccust": wo.wccust}), flush=True)

upd = update_ticket_fields(TICKET, {"customfield_12089": str(wo.wo_number)})
print(json.dumps({"jira_update": upd}), flush=True)
print("DONE", flush=True)
