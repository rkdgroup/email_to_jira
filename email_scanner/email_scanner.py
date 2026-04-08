"""
DSLF Email Scanner — polls the "List Rental" folder in the shared Outlook mailbox,
downloads PDF attachments, and creates DSLF Jira tickets via the existing pipeline.

Auth: Microsoft Graph API with MSAL ROPC flow (username + password).
      Authenticates as service account on every run using credentials from .env.
      Requires MS_CLIENT_ID, MS_CLIENT_SECRET, MS_SERVICE_ACCOUNT, MS_SERVICE_PASSWORD, MS_TENANT_ID.

Usage:
    python email_scanner.py            # run once
    python email_scanner.py --loop     # run every 5 minutes (always-on)
"""

import os
import sys
import json
import time
import logging
import argparse
import tempfile
import base64
import requests
from pathlib import Path
from dotenv import load_dotenv

# ── Path setup ────────────────────────────────────────────────────────────────
_SCRIPT_DIR  = Path(__file__).parent
_PROJECT_DIR = _SCRIPT_DIR.parent
sys.path.insert(0, str(_PROJECT_DIR))

load_dotenv(_PROJECT_DIR / ".env")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_SCRIPT_DIR / "logs" / "email_scanner.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
CLIENT_ID        = os.getenv("MS_CLIENT_ID", "")
TENANT_ID        = os.getenv("MS_TENANT_ID", "common")
SCOPES        = ["Mail.Read", "Mail.ReadWrite", "Mail.Read.Shared", "Mail.ReadWrite.Shared"]
POLL_INTERVAL = 60  # 5 minutes

SHARED_MAILBOX        = os.getenv("IMAP_EMAIL", "Listfulfillment@data-management.com")
SOURCE_FOLDER         = "List Rental"
PROCESSED_FOLDER      = "List Rental/Processed"
FAILED_FOLDER         = "List Rental/Failed"
THREAD_MAP_FILE     = _SCRIPT_DIR / "thread_map.json"   # conversationId → ticket key
PROCESSED_IDS_FILE  = _SCRIPT_DIR / "processed_ids.json"  # message IDs already handled

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Sender domain → broker key.  Used to route orders that lack standard PDF fingerprints.
SENDER_BROKER_MAP = {
    "keyacquisition.com":         "kap",
    "keyacquisitionpartners.com": "kap",
    "rmidirect.com":              "rmi_direct",
    "adstradata.com":             "adstra",
    "conraddirect.com":           "conrad_direct",
    "washingtonlists.com":        "washington_lists",
}


def _mailbox_base() -> str:
    return f"{GRAPH_BASE}/users/{SHARED_MAILBOX}"


# ── MSAL Auth ─────────────────────────────────────────────────────────────────

def get_access_token(force_login: bool = False) -> str:
    import msal

    CLIENT_SECRET = os.getenv("MS_CLIENT_SECRET", "")
    USERNAME      = os.getenv("MS_SERVICE_ACCOUNT", "")
    PASSWORD      = os.getenv("MS_SERVICE_PASSWORD", "")

    if not CLIENT_ID or not CLIENT_SECRET or not USERNAME or not PASSWORD:
        log.error("MS_CLIENT_ID, MS_CLIENT_SECRET, MS_SERVICE_ACCOUNT, MS_SERVICE_PASSWORD must be set in .env")
        sys.exit(1)

    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET,
    )

    result = app.acquire_token_by_username_password(
        username=USERNAME,
        password=PASSWORD,
        scopes=SCOPES,
    )

    if "access_token" not in result:
        log.error("Auth failed: %s", result.get("error_description", result))
        sys.exit(1)

    log.info("Authenticated as %s", USERNAME)
    return result["access_token"]


# ── Graph API helpers ─────────────────────────────────────────────────────────

def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _get(token: str, url: str, params: dict = None) -> dict:
    resp = requests.get(url, headers=_headers(token), params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def _patch(token: str, url: str, body: dict) -> None:
    resp = requests.patch(
        url, headers={**_headers(token), "Content-Type": "application/json"},
        json=body, timeout=20)
    resp.raise_for_status()


def _post(token: str, url: str, body: dict) -> dict:
    resp = requests.post(
        url, headers={**_headers(token), "Content-Type": "application/json"},
        json=body, timeout=20)
    resp.raise_for_status()
    return resp.json()


# ── Folder resolution ─────────────────────────────────────────────────────────

_folder_cache: dict[str, str] = {}


def _get_folder_id(token: str, folder_path: str) -> str:
    if folder_path in _folder_cache:
        return _folder_cache[folder_path]

    base      = _mailbox_base()
    parts     = folder_path.split("/")
    parent_id = "inbox"

    for part in parts:
        data    = _get(token, f"{base}/mailFolders/{parent_id}/childFolders",
                       params={"$filter": f"displayName eq '{part}'"})
        folders = data.get("value", [])
        if folders:
            parent_id = folders[0]["id"]
        else:
            created   = _post(token, f"{base}/mailFolders/{parent_id}/childFolders",
                              {"displayName": part})
            parent_id = created["id"]
            log.info("Created folder: %s", part)

    _folder_cache[folder_path] = parent_id
    return parent_id


# ── Thread map (conversationId → Jira ticket key) ────────────────────────────

def _load_thread_map() -> dict:
    if THREAD_MAP_FILE.exists():
        return json.loads(THREAD_MAP_FILE.read_text())
    return {}


def _save_thread_map(thread_map: dict) -> None:
    THREAD_MAP_FILE.write_text(json.dumps(thread_map, indent=2))


# ── Processed message IDs ─────────────────────────────────────────────────────

def _load_processed_ids() -> set:
    if PROCESSED_IDS_FILE.exists():
        return set(json.loads(PROCESSED_IDS_FILE.read_text()))
    return set()


def _mark_processed(msg_id: str) -> None:
    ids = _load_processed_ids()
    ids.add(msg_id)
    PROCESSED_IDS_FILE.write_text(json.dumps(list(ids), indent=2))


# ── Email processing ──────────────────────────────────────────────────────────

def _add_jira_comment(ticket_key: str, subject: str, sender: str, body: str) -> None:
    """Add a follow-up email as a comment on an existing Jira ticket."""
    from tools_jira import add_comment_to_ticket
    comment = f"Follow-up email from {sender}\nSubject: {subject}\n\n{body}"
    result  = add_comment_to_ticket(ticket_key, comment)
    if "error" in result:
        log.error("Failed to add comment to %s: %s", ticket_key, result["error"])
    else:
        log.info("Added follow-up comment to %s", ticket_key)


def _fetch_full_body(token: str, msg_id: str) -> str:
    """Fetch the full message body as plain text.
    Requests text/plain explicitly to avoid HTML <style> bloat pushing
    key content past the broker detection scan window."""
    resp = requests.get(
        f"{_mailbox_base()}/messages/{msg_id}",
        headers={**_headers(token), "Prefer": 'outlook.body-content-type="text"'},
        params={"$select": "body"},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json().get("body", {}).get("content", "").strip()


def _generate_pdf_from_text(text: str, subject: str = "") -> str:
    """Generate a PDF from plain text using PyMuPDF. Returns temp file path.
    Writes line-by-line across multiple pages so no content is ever truncated."""
    import fitz
    doc = fitz.open()
    full_text = f"{subject}\n\n{text}" if subject else text
    lines = full_text.splitlines()
    page = doc.new_page()
    y = 50
    for line in lines:
        if y > 760:
            page = doc.new_page()
            y = 50
        page.insert_text((50, y), line, fontsize=10, fontname="helv")
        y += 14
    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp_path = tmp.name
    tmp.close()
    doc.save(tmp_path)
    doc.close()
    return tmp_path


def _download_attachment(token: str, msg_id: str, att: dict, suffix: str) -> str | None:
    """Download an attachment and save to a temp file. Returns temp path or None on failure."""
    try:
        att_data  = _get(token, f"{_mailbox_base()}/messages/{msg_id}/attachments/{att['id']}")
        file_bytes = base64.b64decode(att_data["contentBytes"])
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(file_bytes)
            return tmp.name
    except Exception as e:
        log.error("Failed to download attachment %r: %s", att.get("name"), e)
        return None


def process_message(token: str, message: dict, failed_folder_id: str, processed_folder_id: str) -> None:
    from parse_pipeline import process_pdf
    from tools_jira import attach_file_to_ticket
    import re

    msg_id          = message["id"]
    subject         = message.get("subject", "(no subject)")
    sender          = message.get("from", {}).get("emailAddress", {}).get("address", "").lower()
    conversation_id = message.get("conversationId", "")

    # Derive broker hint from sender domain so orders with non-standard PDF headers
    # (e.g. KAP emails sent from no-reply@keyacquisition.com) are routed correctly.
    sender_domain  = sender.split("@")[-1] if "@" in sender else ""
    broker_hint    = SENDER_BROKER_MAP.get(sender_domain, "")
    if broker_hint:
        log.info("Broker hint from sender domain %r: %s", sender_domain, broker_hint)

    log.info("Processing: %r from %s", subject, sender)

    # If thread already has a ticket, add follow-up as comment
    thread_map = _load_thread_map()
    if conversation_id and conversation_id in thread_map:
        existing_key = thread_map[conversation_id]
        log.info("Follow-up on thread -> adding comment to %s", existing_key)
        msg_detail = _get(token, f"{_mailbox_base()}/messages/{msg_id}",
                          params={"$select": "body"})
        body = msg_detail.get("body", {}).get("content", "")
        body = re.sub(r"<[^>]+>", "", body).strip()
        if body:
            _add_jira_comment(existing_key, subject, sender, body)
        _mark_processed(msg_id)
        return

    # Fetch all attachments (include isInline to catch embedded PDFs)
    data     = _get(token, f"{_mailbox_base()}/messages/{msg_id}/attachments",
                    params={"$select": "id,name,contentType,isInline"})
    all_atts = data.get("value", [])

    log.info("Attachments found (%d): %s", len(all_atts),
             [(a.get("name"), a.get("contentType"), a.get("isInline")) for a in all_atts])

    pdf_atts   = [a for a in all_atts
                  if not a.get("isInline")
                  and (a.get("name", "").lower().endswith(".pdf")
                       or "pdf" in a.get("contentType", "").lower())]
    other_atts = [a for a in all_atts if a not in pdf_atts]

    if not pdf_atts:
        log.info("No PDF attachment in %r — generating PDF from email body", subject)
        body_text = _fetch_full_body(token, msg_id)
        if not body_text:
            log.warning("No PDF and empty body in %r — skipping", subject)
            _mark_processed(msg_id)
            return
        pdf_atts = [{"_generated": True, "body_text": body_text, "subject": subject}]

    any_failed  = False
    ticket_keys = []
    for att in pdf_atts:
        if att.get("_generated"):
            att_name = att.get("subject") or "email_body"
            tmp_path = _generate_pdf_from_text(att["body_text"], att["subject"])
        else:
            att_name = att.get("name", "attachment.pdf")
            tmp_path = _download_attachment(token, msg_id, att, ".pdf")
        if not tmp_path:
            any_failed = True
            continue
        try:
            result = process_pdf(tmp_path, broker_hint=broker_hint)
            if result.get("success"):
                key = result.get("ticket_key")
                log.info("Ticket created: %s from %r", key, att_name)
                ticket_keys.append(key)

                # Attach other files (Excel, zip, etc.) to the ticket
                for other in other_atts:
                    other_name = other.get("name", "file")
                    ext = Path(other_name).suffix or ".bin"
                    other_path = _download_attachment(token, msg_id, other, ext)
                    if other_path:
                        try:
                            attach_file_to_ticket(key, other_path)
                            log.info("Extra file attached to %s: %r", key, other_name)
                        except Exception as e:
                            log.warning("Could not attach %r to %s: %s", other_name, key, e)
                        finally:
                            try:
                                Path(other_path).unlink()
                            except Exception:
                                pass
            else:
                log.error("Pipeline failed for %r: %s", att_name,
                          "; ".join(result.get("errors", ["unknown"])))
                any_failed = True
        except Exception as e:
            log.error("Exception on %r: %s", att_name, e)
            any_failed = True
        finally:
            try:
                Path(tmp_path).unlink()
            except Exception:
                pass

    # Save thread -> ticket mapping for first successful ticket
    if ticket_keys and conversation_id:
        thread_map[conversation_id] = ticket_keys[0]
        _save_thread_map(thread_map)

    _mark_processed(msg_id)

    if any_failed:
        log.warning("Moving %r to Failed folder", subject)
        _post(token, f"{_mailbox_base()}/messages/{msg_id}/move",
              {"destinationId": failed_folder_id})
    elif ticket_keys:
        log.info("Moving %r to Processed folder", subject)
        _post(token, f"{_mailbox_base()}/messages/{msg_id}/move",
              {"destinationId": processed_folder_id})


# ── Main scan ─────────────────────────────────────────────────────────────────

def run_scan() -> None:
    token        = get_access_token()
    source_id    = _get_folder_id(token, SOURCE_FOLDER)
    failed_id    = _get_folder_id(token, FAILED_FOLDER)
    processed_id = _get_folder_id(token, PROCESSED_FOLDER)

    data     = _get(token, f"{_mailbox_base()}/mailFolders/{source_id}/messages",
                    params={"$top": 50,
                            "$select": "id,subject,from,receivedDateTime,hasAttachments,conversationId",
                            "$orderby": "receivedDateTime asc"})
    processed = _load_processed_ids()
    messages  = [m for m in data.get("value", []) if m["id"] not in processed]

    if not messages:
        log.info("No new messages in '%s'.", SOURCE_FOLDER)
        return

    log.info("Found %d new message(s).", len(messages))
    for msg in messages:
        try:
            process_message(token, msg, failed_id, processed_id)
        except Exception as e:
            log.error("Error processing message: %s", e)
            _mark_processed(msg["id"])


def main():
    parser = argparse.ArgumentParser(description="DSLF Email Scanner")
    parser.add_argument("--loop", action="store_true",
                        help=f"Run every {POLL_INTERVAL // 60} minutes")
    args = parser.parse_args()

    if args.loop:
        log.info("Started — polling every %d minutes.", POLL_INTERVAL // 60)
        while True:
            try:
                run_scan()
            except Exception as e:
                log.error("Scan error: %s", e)
            time.sleep(POLL_INTERVAL)
    else:
        run_scan()


if __name__ == "__main__":
    main()
