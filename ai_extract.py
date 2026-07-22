"""
Claude-based PDF field extraction for DSLF list-rental orders.

Sends a broker purchase-order PDF to the Anthropic API (Opus 4.8 by default) and
returns the DSLF ticket fields as structured JSON. This is an OFFLINE tool used by
compare_extraction.py to show how Claude would parse an order vs. the rule-based
parser output already on a ticket. It does NOT touch Jira and is not wired into the
live pipeline.

The two prose fields (description, omission_description) come back as arrays of
strings — one line / criterion per element — which is the whole point: clean,
per-line writing instead of the run-on blob the rule-based path produces.

Usage (module):
    from ai_extract import extract_fields_from_pdf
    result = extract_fields_from_pdf("order.pdf")
    fields = result["fields"]      # dict of DSLF fields
    usage  = result["usage"]       # {"input_tokens": ..., "output_tokens": ...}
"""

import os
import json
import base64
import logging
from pathlib import Path

from dotenv import load_dotenv

_SCRIPT_DIR = Path(__file__).parent
load_dotenv(_SCRIPT_DIR / ".env")

log = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-opus-4-8"

# ── Structured-output schema ────────────────────────────────────────────────
# Hand-written JSON Schema (no min/max/length constraints — unsupported by
# structured outputs). Every property required; objects use additionalProperties
# false. description / omission_description / special_seed_instructions are arrays
# of strings so the model returns clean per-line content.
_STR = {"type": "string"}
_STR_ARR = {"type": "array", "items": {"type": "string"}}

DSLF_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "list_name":               _STR,
        "mailer_name":             _STR,
        "manager_order_number":    _STR,
        "mailer_po":               _STR,
        "list_manager":            _STR,
        "requestor_name":          _STR,
        "requestor_email":         _STR,
        "mail_date":               _STR,   # YYYY-MM-DD or "" if absent
        "due_date":                _STR,   # Ship By, YYYY-MM-DD or ""
        "requested_quantity":      {"type": "integer"},   # 0 if not stated
        "availability_rule":       _STR,   # "All Available" | "Nth" | ""
        "file_format":             _STR,   # ASCII Delimited | ASCII Fixed | Excel | Other | ""
        "ship_to_email":           _STR,
        "shipping_method":         _STR,   # Email | FTP | Other | ""
        "shipping_instructions":   _STR,
        "other_fees":              _STR,   # "STATE OMITS" when 6+ states/zips/SCFs, else ""
        "key_code":                _STR,
        "db_code":                 _STR,   # best-guess database code (e.g. N11D), or ""
        "description":             _STR_ARR,          # selection / order body, one line per element
        "omission_description":    _STR_ARR,          # one omit criterion per element
        "special_seed_instructions": _STR_ARR,        # ONLY "Insert:" lines; usually empty
    },
    "required": [
        "list_name", "mailer_name", "manager_order_number", "mailer_po",
        "list_manager", "requestor_name", "requestor_email", "mail_date",
        "due_date", "requested_quantity", "availability_rule", "file_format",
        "ship_to_email", "shipping_method", "shipping_instructions",
        "other_fees", "key_code", "db_code", "description",
        "omission_description", "special_seed_instructions",
    ],
}

# Field order for callers that render results (compare_extraction.py).
FIELD_ORDER = list(DSLF_SCHEMA["properties"].keys())

_SYSTEM = """You extract structured fields from broker purchase-order PDFs for the DSLF \
List Rental pipeline (Data Management Inc.). Output must follow these house rules exactly.

TITLE SEMANTICS
- Mailer Name = the organization SENDING the mail (the client renting the list).
- List Name = the donor list being RENTED. NEVER swap these two.
- Manager Order Number is the broker's internal order id (e.g. ADSTRA J-/I-prefix, RMI MGT#,
  Data Axle Order#). It is NOT the Mailer PO.
- Mailer PO is a separate purchase-order number (often 6-7 digits, or BRK-/suffix forms).

LIST MANAGER — must be EXACTLY one of:
  ADSTRA, AALC, AMLC, CELCO, CONRAD, DATA-AXLE, KAP, MARY E GRANGER, NEGEV,
  NAMES IN THE NEWS, RKD, RMI, WASHINGTON LISTS, WE ARE MOORE

AVAILABILITY RULE
- "Full Run" / "All Available" -> "All Available"
- "NTH NAME" / every Nth -> "Nth"
- If not stated, "" .

FILE FORMAT: one of ASCII Delimited, ASCII Fixed, Excel, Other (or "" if unstated).
SHIPPING METHOD: Email, FTP, or Other (or "").

FIELD PLACEMENT — this is the important part for writing quality:
- omission_description: ONE array element per distinct OMIT / suppression criterion.
  Keep each criterion on its own line. Do NOT merge multiple criteria into one string.
  Include STATE OMITS, flag omits, "OMIT PREVIOUS ORDER ...", "1 PER HOUSEHOLD",
  standard omit lines, NCOA reason codes, zip/SCF omits, etc. — each as its own element.
- description: the selection / order body (segment criteria, "Select By", standard
  suppressions, special instructions). One logical line per array element. This is
  selection criteria, NOT omit criteria.
- other_fees: "STATE OMITS" only when the omission has 6+ states/zips/SCFs; else "".
- special_seed_instructions: ONLY lines that begin with "Insert:". Never FTP details or
  email addresses. Usually an empty array.
- key_code: text after "And"/"&" on a MATERIAL line (Conrad), a "Key Code:" field, or an
  Order# suffix. "" if none.
- db_code: your best guess at the internal database code if the order states one; else "".

GENERAL
- Dates as YYYY-MM-DD. requested_quantity as an integer (0 if not stated).
- Transcribe values faithfully from the PDF. Do not invent data. Leave a field "" (or [] /
  0) when the order does not contain it. Preserve the order's own wording in the prose arrays.
"""

_USER_TEXT = (
    "Extract the DSLF ticket fields from this purchase-order PDF. Follow the house rules "
    "in the system prompt. Return every field; use \"\" / [] / 0 for anything the order "
    "does not state."
)


def extract_fields_from_pdf(pdf_path: str, model: str = DEFAULT_MODEL) -> dict:
    """Send the PDF to Claude and return {"fields": {...}, "usage": {...}, "model": ...}.

    Raises RuntimeError on missing key/oversize; propagates anthropic API errors.
    """
    import anthropic

    if not os.getenv("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY not set in environment/.env")

    p = Path(pdf_path)
    data = p.read_bytes()
    if len(data) > 32 * 1024 * 1024:
        raise RuntimeError(f"PDF {p.name} exceeds the 32 MB request limit")
    b64 = base64.standard_b64encode(data).decode("ascii")

    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=model,
        max_tokens=8000,
        thinking={"type": "adaptive"},
        output_config={
            "effort": "high",
            "format": {"type": "json_schema", "schema": DSLF_SCHEMA},
        },
        system=_SYSTEM,
        messages=[{
            "role": "user",
            "content": [
                {"type": "document",
                 "source": {"type": "base64",
                            "media_type": "application/pdf",
                            "data": b64}},
                {"type": "text", "text": _USER_TEXT},
            ],
        }],
    )

    if resp.stop_reason == "refusal":
        raise RuntimeError(f"Claude refused: {getattr(resp, 'stop_details', None)}")

    text = next((b.text for b in resp.content if b.type == "text"), "")
    if not text:
        raise RuntimeError("No text block in Claude response")
    fields = json.loads(text)

    usage = {
        "input_tokens": resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
        "cache_read_input_tokens": getattr(resp.usage, "cache_read_input_tokens", 0),
    }
    log.info("Claude extraction: %s in / %s out tokens (%s)",
             usage["input_tokens"], usage["output_tokens"], model)
    return {"fields": fields, "usage": usage, "model": resp.model}
