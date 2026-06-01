"""
Jira REST API tools for creating and searching DSLF list fulfillment tickets.

Credentials required in .env (project root):
    JIRA_BASE_URL=https://rkdgroup.atlassian.net
    JIRA_EMAIL=your@email.com
    JIRA_API_TOKEN=your_api_token
"""

import os
import json
import logging
import requests
from requests.auth import HTTPBasicAuth

log = logging.getLogger(__name__)

def _get_jira_base_url():
    return os.getenv("JIRA_BASE_URL", "https://rkdgroup.atlassian.net")

def _get_jira_email():
    return os.getenv("JIRA_EMAIL")

def _get_jira_api_token():
    return os.getenv("JIRA_API_TOKEN")

DSLF_PROJECT_KEY = "DSLF"
DSLF_ISSUE_TYPE_ID = "11806"

# Static option ID mappings for known select fields
AVAILABILITY_RULE_OPTIONS = {"Nth": "13235", "All Available": "13236"}
FILE_FORMAT_OPTIONS = {
    "ASCII Delimited": "13237",
    "ASCII Fixed": "13238",
    "Excel": "13239",
    "Other": "13240",
}
SHIPPING_METHOD_OPTIONS = {"Email": "13241", "FTP": "13242", "Other": "13243"}

# Cache for dynamically fetched option IDs (e.g. Billable Account)
_option_cache: dict = {}


def _auth() -> HTTPBasicAuth:
    return HTTPBasicAuth(_get_jira_email(), _get_jira_api_token())


def _headers() -> dict:
    return {"Accept": "application/json", "Content-Type": "application/json"}


def _get_field_option_id(field_id: str, label: str) -> str | None:
    """Fetch allowed values for a select field and find the matching option ID."""
    cache_key = f"{field_id}:{label}"
    if cache_key in _option_cache:
        return _option_cache[cache_key]

    url = (
        f"{_get_jira_base_url()}/rest/api/3/issue/createmeta"
        f"/{DSLF_PROJECT_KEY}/issuetypes/{DSLF_ISSUE_TYPE_ID}"
    )
    resp = requests.get(url, auth=_auth(), headers={"Accept": "application/json"}, timeout=15)
    if resp.status_code != 200:
        log.warning("Could not fetch field options for %s: %s", field_id, resp.status_code)
        return None

    for f in resp.json().get("fields", []):
        if f.get("fieldId") != field_id:
            continue
        for opt in f.get("allowedValues", []):
            _option_cache[f"{field_id}:{opt['value'].upper()}"] = opt["id"]
        return _option_cache.get(f"{field_id}:{label.upper()}")

    return None


def create_jira_ticket(
    summary: str,
    mailer_name: str = "",
    mailer_po: str = "",
    list_name: str = "",
    list_manager: str = "",
    requested_quantity: int = 0,
    description: str = "",
    manager_order_number: str = "",
    mail_date: str = "",
    ship_by_date: str = "",
    requestor_name: str = "",
    requestor_email: str = "",
    ship_to_email: str = "",
    key_code: str = "",
    billable_account: str = "",
    availability_rule: str = "",
    file_format: str = "",
    shipping_method: str = "",
    shipping_instructions: str = "",
    omission_description: str = "",
    other_fees: str = "",
    special_seed_instructions: str = "",
    db_code: str = "",
) -> dict:
    """Create a DSLF Jira ticket. Returns dict with 'key' on success or 'error' on failure."""

    fields: dict = {
        "project": {"key": DSLF_PROJECT_KEY},
        "issuetype": {"id": DSLF_ISSUE_TYPE_ID},
        "summary": summary,
    }

    # Description — accepts a pre-built ADF dict or a plain string
    if description:
        if isinstance(description, dict):
            fields["description"] = description
        else:
            fields["description"] = {
                "type": "doc",
                "version": 1,
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": description}]}],
            }

    # Text fields
    if mailer_name:
        fields["customfield_12194"] = mailer_name
    if mailer_po:
        fields["customfield_12193"] = mailer_po
    if manager_order_number:
        fields["customfield_12192"] = manager_order_number
    if key_code:
        fields["customfield_12195"] = key_code
    if list_manager:
        fields["customfield_12231"] = list_manager
    if requestor_name:
        fields["customfield_12232"] = requestor_name
    if requestor_email:
        fields["customfield_12233"] = requestor_email
    if list_name:
        fields["customfield_12234"] = list_name
    if ship_to_email:
        fields["customfield_12275"] = ship_to_email
    if shipping_instructions:
        fields["customfield_12277"] = shipping_instructions
    if other_fees:
        fields["customfield_12278"] = other_fees
    if special_seed_instructions:
        fields["customfield_12311"] = special_seed_instructions

    # Seed Tracking Number — always same as Manager Order Number (pattern from all 83 tickets)
    if manager_order_number:
        fields["customfield_12272"] = manager_order_number

    # Shipping Instructions — CC: requestor_email (pattern from real tickets: CC goes to list manager contact)
    if not shipping_instructions and requestor_email:
        shipping_instructions = f"CC: {requestor_email}"
        fields["customfield_12277"] = shipping_instructions

    # Numeric field
    if requested_quantity:
        fields["customfield_12271"] = int(requested_quantity)

    # Date fields
    if mail_date:
        fields["customfield_12196"] = mail_date
    if ship_by_date:
        fields["duedate"] = ship_by_date

    # Default file format to ASCII Delimited if shipping is Email and no format specified
    if not file_format and shipping_method == "Email":
        file_format = "ASCII Delimited"

    # Select fields — map friendly name to option ID
    if availability_rule:
        opt_id = AVAILABILITY_RULE_OPTIONS.get(availability_rule)
        if opt_id:
            fields["customfield_12273"] = {"id": opt_id}
        else:
            log.warning("Unknown availability_rule: %s", availability_rule)

    if file_format:
        opt_id = FILE_FORMAT_OPTIONS.get(file_format)
        if opt_id:
            fields["customfield_12274"] = {"id": opt_id}
        else:
            log.warning("Unknown file_format: %s", file_format)

    if shipping_method:
        opt_id = SHIPPING_METHOD_OPTIONS.get(shipping_method)
        if opt_id:
            fields["customfield_12276"] = {"id": opt_id}
        else:
            log.warning("Unknown shipping_method: %s", shipping_method)

    # Billable account — dynamic lookup
    if billable_account:
        # Try known mapping first
        known = {"A18": "13021"}
        opt_id = known.get(billable_account.upper())
        if not opt_id:
            opt_id = _get_field_option_id("customfield_12191", billable_account)
        if opt_id:
            fields["customfield_12191"] = {"id": opt_id}
        else:
            log.warning("Could not resolve billable_account option ID for: %s", billable_account)

    # Client Database and Seed Database — derived from db_code (e.g. J75R → client=J75R, seed=J75S)
    if db_code:
        client_db_key = db_code  # e.g. "J75R"
        seed_db_key = db_code[:-1] + "S"  # e.g. "J75S"
        client_id = _get_field_option_id("customfield_12155", client_db_key)
        seed_id = _get_field_option_id("customfield_12156", seed_db_key)
        if client_id:
            fields["customfield_12155"] = {"id": client_id}
        if seed_id:
            fields["customfield_12156"] = {"id": seed_id}

    # Omission description — ADF format
    if omission_description:
        fields["customfield_12270"] = {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": omission_description}],
                }
            ],
        }

    payload = {"fields": fields}
    url = f"{_get_jira_base_url()}/rest/api/3/issue"

    try:
        resp = requests.post(url, auth=_auth(), headers=_headers(), json=payload, timeout=30)
        if resp.status_code in (200, 201):
            data = resp.json()
            log.info("Created Jira ticket: %s", data.get("key"))
            return {"key": data["key"], "id": data["id"], "url": f"{_get_jira_base_url()}/browse/{data['key']}"}
        else:
            log.error("Jira create failed %s: %s", resp.status_code, resp.text)
            return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as e:
        log.error("Jira request failed: %s", e)
        return {"error": str(e)}


def search_jira_tickets(jql: str, max_results: int = 10) -> dict:
    """Search Jira tickets using JQL. Returns list of matching issues."""
    url = f"{_get_jira_base_url()}/rest/api/3/search"
    params = {"jql": jql, "maxResults": max_results, "fields": "summary,status,customfield_12193,customfield_12194"}

    try:
        resp = requests.get(url, auth=_auth(), headers={"Accept": "application/json"}, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            issues = [
                {
                    "key": i["key"],
                    "summary": i["fields"].get("summary", ""),
                    "status": i["fields"].get("status", {}).get("name", ""),
                    "mailer_po": i["fields"].get("customfield_12193", ""),
                }
                for i in data.get("issues", [])
            ]
            return {"total": data.get("total", 0), "issues": issues}
        else:
            return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as e:
        return {"error": str(e)}


def flag_for_review(reason: str, details: str = "") -> dict:
    """Log that this order needs human review. Returns confirmation."""
    log.warning("ORDER FLAGGED FOR REVIEW: %s | %s", reason, details)
    return {
        "flagged": True,
        "reason": reason,
        "details": details,
        "message": "Order has been flagged for human review. No ticket was created.",
    }


def add_comment_to_ticket(ticket_key: str, body: str) -> dict:
    """
    Add a plain-text comment to an existing Jira ticket.
    body is plain text; wrapped in ADF paragraph format for the v3 API.
    Returns dict with 'id' on success or 'error' on failure.
    """
    url = f"{_get_jira_base_url()}/rest/api/3/issue/{ticket_key}/comment"
    payload = {
        "body": {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": body}],
                }
            ],
        }
    }
    try:
        resp = requests.post(url, auth=_auth(), headers=_headers(), json=payload, timeout=30)
        if resp.status_code in (200, 201):
            data = resp.json()
            log.info("Added comment to %s: id=%s", ticket_key, data.get("id"))
            return {"id": data.get("id"), "ticket_key": ticket_key}
        else:
            log.error("Comment failed %s: %s", resp.status_code, resp.text)
            return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as e:
        log.error("Comment request failed: %s", e)
        return {"error": str(e)}


def attach_file_to_ticket(ticket_key: str, file_path: str) -> dict:
    """
    Attach a file (e.g. the source PDF) to an existing Jira ticket.
    Returns {"id": ..., "filename": ...} on success or {"error": ...} on failure.
    """
    url = f"{_get_jira_base_url()}/rest/api/3/issue/{ticket_key}/attachments"
    headers = {"Accept": "application/json", "X-Atlassian-Token": "no-check"}
    try:
        with open(file_path, "rb") as f:
            resp = requests.post(
                url, auth=_auth(), headers=headers,
                files={"file": (os.path.basename(file_path), f)},
                timeout=60,
            )
        if resp.status_code in (200, 201):
            data = resp.json()
            attachment = data[0] if isinstance(data, list) and data else data
            log.info("Attached %s to %s", os.path.basename(file_path), ticket_key)
            return {"id": attachment.get("id"), "filename": attachment.get("filename"), "ticket_key": ticket_key}
        else:
            log.error("Attachment failed %s: %s", resp.status_code, resp.text)
            return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as e:
        log.error("Attachment request failed: %s", e)
        return {"error": str(e)}


def get_ticket_billable_account(ticket_key: str) -> str:
    """
    Fetch a ticket and return the Billable Account value (e.g. 'K40') from
    customfield_12191.  Returns empty string if the field is missing or the
    request fails.
    """
    url = f"{_get_jira_base_url()}/rest/api/3/issue/{ticket_key}"
    params = {"fields": "customfield_12191"}
    try:
        resp = requests.get(url, auth=_auth(), headers={"Accept": "application/json"},
                            params=params, timeout=15)
        if resp.status_code == 200:
            field = resp.json().get("fields", {}).get("customfield_12191") or {}
            return field.get("value", "")
        log.warning("Could not fetch billable account for %s: HTTP %s", ticket_key, resp.status_code)
        return ""
    except Exception as e:
        log.warning("get_ticket_billable_account failed for %s: %s", ticket_key, e)
        return ""


def update_ticket_fields(ticket_key: str, fields: dict) -> dict:
    """
    Update one or more fields on an existing Jira ticket.
    fields dict uses the same format as create_jira_ticket (field_id → value).
    Returns {"ok": True} on success or {"error": ...} on failure.
    """
    url = f"{_get_jira_base_url()}/rest/api/3/issue/{ticket_key}"
    payload = {"fields": fields}
    try:
        resp = requests.put(url, auth=_auth(), headers=_headers(), json=payload, timeout=30)
        if resp.status_code == 204:
            log.info("Updated fields on %s", ticket_key)
            return {"ok": True, "ticket_key": ticket_key}
        else:
            log.error("Update failed %s: %s", resp.status_code, resp.text)
            return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as e:
        log.error("Update request failed: %s", e)
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# QC helper functions
# ---------------------------------------------------------------------------

QC_FIELDS = [
    "summary", "status", "attachment",
    "customfield_12155",  # Client Database
    "customfield_12156",  # Seed Database
    "customfield_12192",  # Manager Order Number
    "customfield_12194",  # Mailer Name
    "customfield_12196",  # Mail Date
    "customfield_12270",  # Omission Description (ADF)
    "customfield_12271",  # Requested Quantity
    "customfield_12231",  # List Manager
]


def get_ticket_attachments(ticket_key: str) -> list:
    """Return attachment list for a ticket. Each dict has id, filename, content, mimeType."""
    url = f"{_get_jira_base_url()}/rest/api/3/issue/{ticket_key}"
    try:
        resp = requests.get(url, auth=_auth(),
                            headers={"Accept": "application/json"},
                            params={"fields": "attachment"}, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("fields", {}).get("attachment") or []
        log.warning("get_ticket_attachments %s: HTTP %s", ticket_key, resp.status_code)
        return []
    except Exception as e:
        log.warning("get_ticket_attachments failed for %s: %s", ticket_key, e)
        return []


def download_attachment(content_url: str, dest_path: str) -> str:
    """Download a Jira attachment to dest_path using streaming. Returns dest_path."""
    resp = requests.get(content_url, auth=_auth(),
                        headers={"Accept": "*/*"},
                        stream=True, timeout=60)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    log.info("Downloaded attachment to %s (%d bytes)", dest_path, os.path.getsize(dest_path))
    return dest_path


def get_ticket_qc_fields(ticket_key: str) -> dict:
    """Fetch all QC-relevant fields in one API call. Returns normalized dict or {"error": ...}."""
    url = f"{_get_jira_base_url()}/rest/api/3/issue/{ticket_key}"
    try:
        resp = requests.get(url, auth=_auth(),
                            headers={"Accept": "application/json"},
                            params={"fields": ",".join(QC_FIELDS)}, timeout=15)
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        raw = resp.json().get("fields", {})

        def _select(fid):
            v = raw.get(fid)
            return v.get("value", "") if isinstance(v, dict) else ""

        return {
            "summary":       raw.get("summary", ""),
            "status":        (raw.get("status") or {}).get("name", ""),
            "attachments":   raw.get("attachment") or [],
            "client_db":     _select("customfield_12155"),
            "seed_db":       _select("customfield_12156"),
            "manager_order": raw.get("customfield_12192") or "",
            "mailer_name":   raw.get("customfield_12194") or "",
            "mail_date":     raw.get("customfield_12196") or "",
            "omission_adf":  raw.get("customfield_12270"),
            "requested_qty": raw.get("customfield_12271") or 0,
            "list_manager":  raw.get("customfield_12231") or "",
        }
    except Exception as e:
        log.warning("get_ticket_qc_fields failed for %s: %s", ticket_key, e)
        return {"error": str(e)}


def get_ticket_transitions(ticket_key: str) -> list:
    """Return available transitions for a ticket as list of {id, name} dicts."""
    url = f"{_get_jira_base_url()}/rest/api/3/issue/{ticket_key}/transitions"
    try:
        resp = requests.get(url, auth=_auth(),
                            headers={"Accept": "application/json"}, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("transitions", [])
        log.warning("get_ticket_transitions %s: HTTP %s", ticket_key, resp.status_code)
        return []
    except Exception as e:
        log.warning("get_ticket_transitions failed for %s: %s", ticket_key, e)
        return []


def transition_ticket(ticket_key: str, transition_name: str) -> dict:
    """Transition a ticket by name. Tries exact match then case-insensitive substring."""
    transitions = get_ticket_transitions(ticket_key)
    if not transitions:
        return {"error": f"No transitions available for {ticket_key}"}

    target = transition_name.lower().strip()
    tid = None
    for t in transitions:
        if t["name"].lower().strip() == target:
            tid = t["id"]
            break
    if not tid:
        for t in transitions:
            if target in t["name"].lower() or t["name"].lower() in target:
                tid = t["id"]
                log.info("Fuzzy transition match: %r -> %r", transition_name, t["name"])
                break

    if not tid:
        available = [t["name"] for t in transitions]
        return {"error": f"Transition {transition_name!r} not found. Available: {available}"}

    url = f"{_get_jira_base_url()}/rest/api/3/issue/{ticket_key}/transitions"
    try:
        resp = requests.post(url, auth=_auth(), headers=_headers(),
                             json={"transition": {"id": tid}}, timeout=30)
        if resp.status_code == 204:
            log.info("Transitioned %s -> %r (id=%s)", ticket_key, transition_name, tid)
            return {"ok": True, "transition_id": tid, "ticket_key": ticket_key}
        log.error("Transition failed %s: %s %s", ticket_key, resp.status_code, resp.text)
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as e:
        log.error("Transition request failed: %s", e)
        return {"error": str(e)}
