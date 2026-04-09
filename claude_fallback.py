"""
Claude AI fallback parser for unrecognized broker formats.

Uses Claude Opus 4.6 with adaptive thinking to extract fields from PDF text.
Called when no broker pattern matches in broker_detector.
"""

import os
import json
import re
import logging

log = logging.getLogger(__name__)

MAX_TEXT_LENGTH = 8000

EXTRACTION_PROMPT = """You are processing a list rental purchase order PDF. Extract ALL fields as JSON.

## Field Extraction Rules (CRITICAL)

**List Manager** = the data/list company whose data is being rented (e.g. ADSTRA, Data Axle, Acxiom)
- NOT the broker — the broker is just a middleman
- Look for "List:" in the PDF, the company next to it is the list manager

**Requestor Name** = the contact person AT the data/list company
- Must match the Requestor Email domain
- NOT the broker contact
- Look for "Contact:" in the Ship-To section

**Requestor Email** = email of the requestor at the data company (from Ship-To contact section)

**Shipping Instructions** = format as: CC: email@domain.com
- Use the Ship-To contact's email

**Availability Rule**:
- If quantity line says "X OR ALL AVAILABLE" → "All Available"
- If it's a fixed count → "Nth"

**Ship By Date** = the "SHIP BY" or "WANTED BY" date → format as YYYY-MM-DD
**Mail Date** = the "MAIL DATE" → format as YYYY-MM-DD

**Summary** = [MAILER NAME] - [LIST NAME] - PO [PO NUMBER]

Return ONLY a JSON object with these exact field names:
{
  "mailer_name": "",
  "mailer_po": "",
  "list_name": "",
  "list_manager": "",
  "requested_quantity": 0,
  "manager_order_number": "",
  "mail_date": "",
  "ship_by_date": "",
  "requestor_name": "",
  "requestor_email": "",
  "ship_to_email": "",
  "key_code": "",
  "availability_rule": "Nth or All Available",
  "file_format": "ASCII Delimited or ASCII Fixed or Excel or Other",
  "shipping_method": "Email or FTP or Other",
  "shipping_instructions": "CC: email@domain.com",
  "omission_description": "",
  "other_fees": "",
  "segment_criteria": ""
}

All date fields must be YYYY-MM-DD format.
requested_quantity must be an integer.
Leave empty string for fields you cannot determine.
"""


def claude_fallback_parse(text: str):
    """
    Parse PDF text using Claude Opus 4.6.

    Returns a ParseResult with source="claude_fallback" and confidence=0.75.
    """
    from parse_result import ParseResult

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set — cannot use Claude fallback")
        return ParseResult(
            source="claude_fallback",
            confidence=0.0,
            warnings=("ANTHROPIC_API_KEY not set",),
        )

    try:
        import anthropic
        client = anthropic.Anthropic()

        truncated = text[:MAX_TEXT_LENGTH]

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            messages=[
                {
                    "role": "user",
                    "content": f"{EXTRACTION_PROMPT}\n\n--- PDF TEXT ---\n{truncated}\n--- END ---",
                }
            ],
        )

        raw = response.content[0].text

        # Strip markdown code fences
        raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
        raw = re.sub(r"\s*```$", "", raw.strip())

        data = json.loads(raw)

        # Ensure requested_quantity is int
        qty = data.get("requested_quantity", 0)
        if isinstance(qty, str):
            qty = int(re.sub(r"[^\d]", "", qty) or "0")

        return ParseResult(
            source="claude_fallback",
            confidence=0.75,
            mailer_name=str(data.get("mailer_name", "")),
            mailer_po=str(data.get("mailer_po", "")),
            list_name=str(data.get("list_name", "")),
            list_manager=str(data.get("list_manager", "")),
            requested_quantity=int(qty),
            manager_order_number=str(data.get("manager_order_number", "")),
            mail_date=str(data.get("mail_date", "")),
            ship_by_date=str(data.get("ship_by_date", "")),
            requestor_name=str(data.get("requestor_name", "")),
            requestor_email=str(data.get("requestor_email", "")),
            ship_to_email=str(data.get("ship_to_email", "")),
            key_code=str(data.get("key_code", "")),
            availability_rule=str(data.get("availability_rule", "")),
            file_format=str(data.get("file_format", "")),
            shipping_method=str(data.get("shipping_method", "")),
            shipping_instructions=str(data.get("shipping_instructions", "")),
            omission_description=str(data.get("omission_description", "")),
            other_fees=str(data.get("other_fees", "")),
            segment_criteria=str(data.get("segment_criteria", "")),
            warnings=("Processed by Claude fallback",),
        )

    except json.JSONDecodeError as e:
        log.error("Claude response was not valid JSON: %s", e)
        return ParseResult(
            source="claude_fallback",
            confidence=0.0,
            warnings=(f"JSON parse error: {e}",),
        )
    except Exception as e:
        log.error("Claude fallback failed: %s", e)
        return ParseResult(
            source="claude_fallback",
            confidence=0.0,
            warnings=(f"Claude API error: {e}",),
        )
