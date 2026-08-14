"""
Advisory LLM review for qc_checker.

The 14 rule-based checks in qc_checker.run_qc_checks() are the verdict: they decide
PASS/FAIL and nothing here can change that. This module adds a second, independent
reading — the SELECT PDF itself plus the ticket as it actually stands — and reports
discrepancies the rules do not cover, including on fields the rules DO cover, so a rule
that is itself wrong becomes visible rather than silently authoritative.

Findings are advisory by construction: they never enter checks[], pass_count, or
hard_fails. qc_checker prints them in their own section of the QC comment.

Every failure path returns [] — no API key, budget exhausted, timeout, API error,
refusal, malformed output, oversize PDF. A QC run is never worse than it was, and an
Anthropic outage cannot stop QC from posting its verdict.
"""

import os
import json
import time
import base64
import logging
from pathlib import Path

log = logging.getLogger(__name__)

QC_MODEL     = "claude-sonnet-5"
QC_EFFORT    = "medium"
QC_TIMEOUT_S = 30
# Per process, same idea as tools_polish.POLISH_BUDGET_S: the Jenkins build has a 4-minute
# timeout and the QC scanner can walk many tickets in one run. Once this is spent the
# remaining tickets get rules-only QC rather than a build that dies half way.
# Measured ~30s per ticket, so this allows roughly three AI reviews plus one in flight
# (120s worst case) inside the 240s Jenkins timeout, leaving the rest of the run room to
# download, parse and comment on every remaining ticket rules-only.
QC_BUDGET_S  = 90
_MAX_PDF_MB  = 32

_spent_s = 0.0
_cache: dict = {}

_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "field":        {"type": "string"},
                    "severity":     {"type": "string", "enum": ["mismatch", "missing", "note"]},
                    "select_value": {"type": "string"},
                    "ticket_value": {"type": "string"},
                    "issue":        {"type": "string"},
                },
                "required": ["field", "severity", "select_value", "ticket_value", "issue"],
            },
        },
    },
    "required": ["findings"],
}

_SYSTEM = """You are quality-checking a DSLF list-rental fulfilment ticket (Data Management \
Inc.) against the SELECT report PDF that was produced when the list was actually pulled.

The PDF is the record of what was SELECTED. The ticket is the record of what was ORDERED.
Your job is to report where they disagree.

Report EVERY discrepancy you find, including on fields you suspect are already checked
elsewhere — a second opinion on those is wanted. For each one give the value as printed on
the SELECT, the value on the ticket, and one sentence on why it matters. Use severity
"mismatch" when both sides state a value and they conflict, "missing" when the SELECT
states something the ticket does not carry at all, and "note" for anything a human should
eyeball but which may well be fine.

DO NOT REPORT THESE — they are correct by design and flagging them is noise:
- Billable Account not sharing the Client Database's prefix. The configured billing account
  legitimately differs (A52D bills to A68, S05D bills to S15).
- File Format "ASCII Fixed" when the ship-to is Saturn, any data-axle.com address, or one
  of the fixed-format houses (data@trylondm.com, data@talonmm.com, data@rkdgroup.com,
  tisdata@trinitydirect.net, tapelibrarian@directmail.com). Those are forced by house rule.
  For Saturn and data-axle the delivery method is forced to FTP as well.
- Other Fees reading "STATE OMITS" when six or more states, zips or SCFs are omitted. That
  is applied automatically.
- A blank Mail Date, File Format, or Other Fees. Blank is acceptable on all three.
- Records Selected not matching Requested Quantity when the Availability Rule is
  "All Available" — the requested figure is only an estimate there. Under "Nth" the count
  must not exceed the requested maximum, and that IS worth reporting.
- Standard suppressions, "Select By", and standing FLAG OMITS in the ticket's Description or
  Omission Description that do not appear on the SELECT. Those come from the client profile
  on file, not from the order, so their absence from the SELECT means nothing.
- Seed Tracking Number repeating the Manager Order Number. That is intentional.
- The ticket status, work order number, or attachments.

Return an empty findings array when the ticket and the SELECT agree. Reporting nothing is a
valid and common answer — do not invent a finding to fill the list."""


def _ticket_text(fields: dict) -> str:
    """Readable rendering of the ticket, bullets and line structure preserved."""
    from compare_extraction import adf_to_lines

    out = []
    for label, key in (
        ("Summary", "summary"), ("List Name", "list_name"), ("Mailer Name", "mailer_name"),
        ("List Manager", "list_manager"), ("Manager Order #", "manager_order"),
        ("Client Database", "client_db"), ("Seed Database", "seed_db"),
        ("Requested Quantity", "requested_qty"), ("Availability Rule", "availability_rule"),
        ("File Format", "file_format"), ("Shipping Method", "shipping_method"),
        ("Ship To Email", "ship_to_email"), ("Shipping Instructions", "shipping_instructions"),
    ):
        out.append(f"{label}: {fields.get(key) if fields.get(key) not in (None, '') else '(empty)'}")

    for label, key in (("Description", "description_adf"), ("Omission Description", "omission_adf")):
        lines = adf_to_lines(fields.get(key))
        out.append(f"\n{label}:")
        out.extend(f"  {ln}" for ln in lines) if lines else out.append("  (empty)")
    return "\n".join(out)


def review(pdf_path: str, ticket_fields: dict, select_data: dict = None) -> list:
    """Compare the SELECT PDF against the ticket. Returns findings; never raises."""
    global _spent_s

    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        log.info("AI QC review skipped — ANTHROPIC_API_KEY not set")
        return []
    if _spent_s >= QC_BUDGET_S:
        log.warning("AI QC budget of %ds exhausted for this run — skipping", QC_BUDGET_S)
        return []

    try:
        data = Path(pdf_path).read_bytes()
    except Exception as e:
        log.warning("AI QC review skipped — cannot read %s: %s", pdf_path, e)
        return []
    if len(data) > _MAX_PDF_MB * 1024 * 1024:
        log.warning("AI QC review skipped — %s exceeds %d MB", pdf_path, _MAX_PDF_MB)
        return []

    ticket_text = _ticket_text(ticket_fields)
    cache_key = (hash(data), ticket_text)
    if cache_key in _cache:
        return _cache[cache_key]

    started = time.monotonic()
    try:
        import anthropic
        client = anthropic.Anthropic(timeout=QC_TIMEOUT_S)
        resp = client.messages.create(
            model=QC_MODEL,
            max_tokens=4000,
            thinking={"type": "adaptive"},
            output_config={"effort": QC_EFFORT,
                           "format": {"type": "json_schema", "schema": _SCHEMA}},
            system=_SYSTEM,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "document",
                     "source": {"type": "base64", "media_type": "application/pdf",
                                "data": base64.standard_b64encode(data).decode("ascii")}},
                    {"type": "text",
                     "text": f"THE TICKET AS IT STANDS:\n\n{ticket_text}\n\n"
                             f"Compare it against the attached SELECT report and report "
                             f"every discrepancy."},
                ],
            }],
        )
        if resp.stop_reason == "refusal":
            log.warning("AI QC review refused by the model — skipping")
            return []
        text = next((b.text for b in resp.content if b.type == "text"), "")
        findings = json.loads(text).get("findings", []) if text else []
    except Exception as e:
        log.warning("AI QC review failed (%s) — rules-only QC stands", e)
        return []
    finally:
        _spent_s += time.monotonic() - started

    findings = [f for f in findings if isinstance(f, dict) and f.get("field")]
    log.info("AI QC review: %d finding(s) in %.1fs", len(findings), time.monotonic() - started)
    _cache[cache_key] = findings
    return findings
