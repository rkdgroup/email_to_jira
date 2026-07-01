"""
Deletes all tickets in the "Testing Tickets" queue.
Run: python delete_testing_tickets.py
"""

import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("JIRA_BASE_URL", "https://rkdgroup.atlassian.net")
EMAIL    = os.getenv("JIRA_EMAIL")
TOKEN    = os.getenv("JIRA_API_TOKEN")
AUTH     = HTTPBasicAuth(EMAIL, TOKEN)
HEADERS  = {"Accept": "application/json"}

TICKETS = [
    "DSLF-700", "DSLF-699", "DSLF-698", "DSLF-697", "DSLF-622",
    "DSLF-566", "DSLF-558", "DSLF-557", "DSLF-556", "DSLF-555",
    "DSLF-554", "DSLF-553", "DSLF-552", "DSLF-551", "DSLF-550",
    "DSLF-549", "DSLF-548", "DSLF-547", "DSLF-546", "DSLF-545",
    "DSLF-544", "DSLF-543", "DSLF-542", "DSLF-541", "DSLF-453",
]

deleted, failed = [], []

for key in TICKETS:
    url = f"{BASE_URL}/rest/api/3/issue/{key}"
    r = requests.delete(url, auth=AUTH, headers=HEADERS)
    if r.status_code == 204:
        print(f"  DELETED  {key}")
        deleted.append(key)
    else:
        print(f"  FAILED   {key}  [{r.status_code}] {r.text[:120]}")
        failed.append(key)

print(f"\nDone — {len(deleted)} deleted, {len(failed)} failed.")
if failed:
    print("Failed:", ", ".join(failed))
