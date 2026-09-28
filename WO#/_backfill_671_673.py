"""One-off backfill: create WOs for DSLF-671 / DSLF-673 and write WO# back to Jira.

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

TICKETS = [
    {"key": "DSLF-671", "mailer": "PAWS OF HONOR", "manager_po": "J2163", "mailer_po": "670239"},
    {"key": "DSLF-673", "mailer": "PAWS OF HONOR", "manager_po": "J2161", "mailer_po": "670235"},
]

for t in TICKETS:
    billable = get_ticket_billable_account(t["key"])
    if not billable:
        print(json.dumps({"key": t["key"], "error": "no billable account on ticket"}), flush=True)
        continue

    wo = create_work_order(
        billable=billable,
        mailer_name=t["mailer"],
        manager_po=t["manager_po"],
        mailer_po=t["mailer_po"],
    )
    upd = update_ticket_fields(t["key"], {"customfield_12089": str(wo.wo_number)})
    print(json.dumps({
        "key": t["key"],
        "billable": billable,
        "wo_number": wo.wo_number,
        "wccust": wo.wccust,
        "jira_update": upd,
    }), flush=True)

print("DONE", flush=True)
