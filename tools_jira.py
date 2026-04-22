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
        f"?projectKeys={DSLF_PROJECT_KEY}&issuetypeIds={DSLF_ISSUE_TYPE_ID}&expand=projects.issuetypes.fields"
    )
    resp = requests.get(url, auth=_auth(), headers={"Accept": "application/json"}, timeout=15)
    if resp.status_code != 200:
        log.warning("Could not fetch field options for %s: %s", field_id, resp.status_code)
        return None

    data = resp.json()
    try:
        fields = data["projects"][0]["issuetypes"][0]["fields"]
        allowed = fields.get(field_id, {}).get("allowedValues", [])
        for opt in allowed:
            if opt.get("value", "").upper() == label.upper():
                _option_cache[cache_key] = opt["id"]
                return opt["id"]
    except (KeyError, IndexError) as e:
        log.warning("Failed to parse field options: %s", e)

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
