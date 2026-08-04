"""
Structural polish of the two prose fields (Description, Omission Description).

The rule-based parsers copy PDF text verbatim, so both fields inherit the PDF's line
wrapping and column layout. DSLF-967 is the canonical example: one sentence wrapped
across two PDF lines became

    12MOS 7/25-6/26 $10+ MALE
    DONORS, OMIT NJ,MN, AND ZIPS

— a sentence split mid-phrase, with an omit criterion stranded in the Description.

polish_fields() sends only the PDF-derived prose to Claude and asks for four STRUCTURAL
operations: join wrapped lines, move omit tails to the omission field, drop duplicates,
drop empties. Nothing may be reworded, added, or dropped — enforced by a token-set gate
(_validate), not by trusting the model.

Profile-injected content (Select By, Standard Suppressions, Special Instructions,
FLAG OMITS) is config-sourced and already clean; it is never sent here. parse_pipeline
re-attaches it around the polished text.

Every failure path — no API key, budget exhausted, timeout, API error, refusal, failed
validation — returns the inputs unchanged. A ticket is never worse than it is today.
"""

import logging
import os
import re
import time

log = logging.getLogger(__name__)

# Haiku measured 5/5 correct on the DSLF-967 case at ~2.5s median, vs 6.0s (Opus 5) and
# 7.9s (Sonnet 5) for the same 5/5 — the work is mechanical re-arrangement, so the cheapest
# tier is also the fastest path through the 4-minute Jenkins build.
POLISH_MODEL     = "claude-haiku-4-5"
POLISH_TIMEOUT_S = 20      # per call
POLISH_BUDGET_S  = 120     # per process — keeps the 4-min Jenkins build safe
POLISH_MAX_TOKENS = 2000

# Strip surrounding punctuation so a line-join ("DONORS," -> "DONORS") is not read as
# a dropped fact. Interior characters ($ . / -) are kept: "$10+", "7/25-6/26", "43216".
# A leading "$" is part of the token — without it "$10+" and "10" are indistinguishable
# and a dropped dollar sign would slip past the gate.
_TOKEN_RE = re.compile(r"[$A-Za-z0-9][A-Za-z0-9$#/.\-+]*")

# A line carrying both selection and omission criteria legitimately splits in two, so the
# output may exceed the input line count by at most one per such line.
_OMIT_HINT_RE = re.compile(r"\bOMIT\b|\bEXCLUDE\b|PER\s+HOUSEHOLD", re.IGNORECASE)

# The ONLY words the model may introduce. Broker PDFs list the priced selects as bare
# fragments under a "Selects:" header that never reaches this module (the parser keeps the
# values and drops the header), so DSLF-967's description read "$10+ / 12 MOS HOTLINE /
# GENDER" with nothing saying what they were. The model may restore that one label; the
# token gate still rejects every other invented word, so the worst case is a mislabelled
# group, never a fabricated criterion.
_ALLOWED_NEW_TOKENS = {"SELECTS"}

_POLISH_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "description_lines":      {"type": "array", "items": {"type": "string"}},
        "omission_lines":         {"type": "array", "items": {"type": "string"}},
    },
    "required": ["description_lines", "omission_lines"],
}

_SYSTEM = """You clean up two text fields extracted from broker list-rental purchase-order \
PDFs for the DSLF pipeline (Data Management Inc.). The text was copied verbatim out of a PDF, \
so it carries the PDF's line wrapping and column layout.

You may perform ONLY these five operations:

1. JOIN a line that the PDF wrapped mid-phrase back onto the previous line. A wrap is
   recognisable because the first line ends mid-thought and the next continues it — e.g.
   "12MOS 7/25-6/26 $10+ MALE" + "DONORS, OMIT NJ,MN, AND ZIPS" is one sentence.
2. MOVE an omission criterion out of the description and into the omission list. Omission
   criteria are the parts that say what is EXCLUDED: "OMIT <states>", "OMIT PREVIOUS ORDER",
   "1 PER HOUSEHOLD", zip/SCF omits, flag omits. Selection criteria — what is being SELECTED
   (dollar amounts, date ranges, gender, recency, select names) — stay in the description.
   When a single line is part selection and part omission, split it at that boundary and put
   each half in the right field.
3. DROP a redundant line. A line is redundant when another line in either field already
   carries all of its criteria. This includes the common case where the SAME omit criterion
   appears twice: once cleanly, and once fused with words that belong to the description.
   Keep the clean copy and drop the fused one — do not leave both.
4. DROP a line that is empty or pure punctuation.
5. LABEL a run of bare select criteria in the DESCRIPTION with the single heading
   "Selects:", and indent each line of that run by exactly two spaces. Broker PDFs list the
   priced selects under a "Selects:" header that was lost in extraction, leaving fragments
   like "$10+", "12 MOS HOTLINE", "GENDER" floating with nothing to identify them. Apply
   this ONLY when TWO OR MORE consecutive description lines are bare criteria fragments of
   that kind — short, no verb, naming a select dimension or its value. Do NOT label a single
   stray line, a full sentence, a pull description, or anything in the omission field.
   "Selects:" is the ONLY heading you may ever write; it is also the only word in your
   entire output that is allowed not to appear in the input.

When joining or splitting at a comma, the comma was only separating the two fragments: drop
it rather than leaving it stranded at the end of a line.

You may NOT:
- Reword, rephrase, translate, expand, abbreviate, or re-order anything.
- Add any word, label, heading, number, or punctuation that was not in the input, with the
  single exception of the "Selects:" heading described in operation 5.
- Drop any criterion, state code, number, zip, date, dollar amount, or flag.
- Change capitalisation or spacing inside a line, except for the single space added where
  you join two wrapped lines, and trimming leading/trailing whitespace.

Apart from that one heading, every word in your output must have come from the input. You are
re-arranging text between and within two fields, nothing more. When in doubt, leave the line
exactly as it is: an unchanged line is always acceptable, a reworded one never is.

WORKED EXAMPLE

Input description:
  12MOS 7/25-6/26 $10+ MALE
  DONORS, OMIT NJ,MN, AND ZIPS
  $10+
  12 MOS HOTLINE
  GENDER
Input omission:
  APO, FPO
  DONORS, OMIT NJ,MN, AND ZIPS

Correct output description:
  12MOS 7/25-6/26 $10+ MALE DONORS
  Selects:
    $10+
    12 MOS HOTLINE
    GENDER
Correct output omission:
  APO, FPO
  OMIT NJ,MN, AND ZIPS

Why: the first two description lines are one wrapped sentence, so they join, and the comma
that separated them is dropped. "OMIT NJ,MN, AND ZIPS" is an omission, so it moves. The
omission list already held that criterion fused with the stray word "DONORS" — that fused
copy is redundant once the clean copy is there, so it is dropped rather than kept alongside.
The last three description lines are bare select fragments, so they take the "Selects:"
heading and a two-space indent; "$10+" is kept even though it repeats a value from the pull
description, because it is a distinct priced select and dropping it would lose a criterion.
"""

_USER_TEMPLATE = """DESCRIPTION LINES:
{desc}

OMISSION LINES:
{omit}

Return the same content with only the four permitted structural operations applied."""

# (description, omission) -> (description, omission). Multi-page AMLC and batched KAP
# orders repeat identical text, so this collapses several calls into one per run.
_cache: dict = {}
_spent_s: float = 0.0


def _tokens(*texts: str) -> set:
    """Fact tokens across the given text, uppercased and stripped of edge punctuation."""
    out = set()
    for t in texts:
        for m in _TOKEN_RE.findall(t or ""):
            tok = m.upper().strip(".-/+")
            if tok:
                out.add(tok)
    return out


def _lines(text: str) -> list:
    return [ln.strip() for ln in (text or "").splitlines() if ln.strip()]


def _clean_out(raw: list) -> list:
    """
    Trim the model's lines while keeping the one thing indentation encodes: membership in
    the group under the heading above. Any leading whitespace collapses to exactly two
    spaces, which _build_adf_description reads as "render me as a bullet".
    """
    out = []
    for item in raw:
        text = str(item)
        if not text.strip():
            continue
        out.append(("  " if text[:1].isspace() else "") + text.strip())
    return out


def _validate(desc_in: str, omit_in: str, desc_out: list, omit_out: list) -> bool:
    """
    True when the model only re-arranged text.

    The token set is compared across the PAIR, not per field: moving a criterion from the
    description to the omission list is the point of the exercise, while inventing or
    dropping one is exactly what must be caught. Set (not multiset) comparison, so
    de-duplication is allowed.
    """
    before = _tokens(desc_in, omit_in)
    after = _tokens("\n".join(desc_out), "\n".join(omit_out))

    invented = after - before - _ALLOWED_NEW_TOKENS
    dropped = before - after
    if invented or dropped:
        log.warning("Polish rejected — invented=%s dropped=%s",
                    sorted(invented)[:8] or "none", sorted(dropped)[:8] or "none")
        return False

    # A permitted heading labels one group, so it may appear at most once, and only in the
    # description — the omission field is a flat list of criteria with nothing to head.
    # Without this the token gate alone would let the model paper the field with headings,
    # since a repeat introduces no new token.
    def _heading(line: str) -> str:
        key = line.strip().rstrip(":").upper()
        return key if key in _ALLOWED_NEW_TOKENS else ""

    if any(_heading(l) for l in omit_out):
        log.warning("Polish rejected — heading written into the omission field")
        return False

    headings = [h for h in (_heading(l) for l in desc_out) if h]
    if len(headings) != len(set(headings)):
        log.warning("Polish rejected — heading repeated: %s", sorted(set(headings)))
        return False

    in_lines = _lines(desc_in) + _lines(omit_in)
    n_in = len(in_lines)
    n_out = len([l for l in desc_out if l.strip()]) + len([l for l in omit_out if l.strip()])
    # Each line that mixes selection and omission criteria may split into two, so allow
    # one extra output line per such input line — but no more. A heading is a whole line of
    # its own, so it earns one more, and only when it is actually present.
    ceiling = n_in + sum(1 for l in in_lines if _OMIT_HINT_RE.search(l)) + len(headings)
    if n_out > ceiling:
        log.warning("Polish rejected — line count grew beyond the split allowance (%d -> %d, max %d)",
                    n_in, n_out, ceiling)
        return False

    return True


def text_to_adf(lines: list) -> dict:
    """Render display lines as an ADF doc (one paragraph per line)."""
    content = [{"type": "paragraph", "content": [{"type": "text", "text": str(ln)}]}
               for ln in lines if str(ln).strip()]
    return {"type": "doc", "version": 1,
            "content": content or [{"type": "paragraph", "content": []}]}


def polish_fields(segment_criteria: str, omission_description: str,
                  model: str = POLISH_MODEL) -> tuple:
    """
    Structurally clean the two PDF-derived prose values.

    Returns (segment_criteria, omission_description) — the inputs unchanged whenever the
    polish cannot be completed and verified. Never raises.
    """
    global _spent_s

    desc_in = segment_criteria or ""
    omit_in = omission_description or ""

    # Nothing to re-arrange in a single line, and nothing to do with no content at all.
    if len(_lines(desc_in)) + len(_lines(omit_in)) < 2:
        return desc_in, omit_in

    key = (model, desc_in, omit_in)
    if key in _cache:
        log.info("Polish cache hit")
        return _cache[key]

    if not os.getenv("ANTHROPIC_API_KEY"):
        log.info("ANTHROPIC_API_KEY not set — skipping polish")
        return desc_in, omit_in

    if _spent_s >= POLISH_BUDGET_S:
        log.warning("Polish budget of %ds exhausted for this run — skipping", POLISH_BUDGET_S)
        return desc_in, omit_in

    started = time.monotonic()
    try:
        import anthropic
        import json

        client = anthropic.Anthropic().with_options(
            timeout=float(POLISH_TIMEOUT_S), max_retries=1)

        # Mechanical re-arrangement, so the lowest effort tier — but Haiku 4.5 predates
        # the effort parameter and errors if it is sent.
        output_config = {"format": {"type": "json_schema", "schema": _POLISH_SCHEMA}}
        if "haiku" not in model:
            output_config["effort"] = "low"

        resp = client.messages.create(
            model=model,
            max_tokens=POLISH_MAX_TOKENS,
            output_config=output_config,
            system=[{"type": "text", "text": _SYSTEM,
                     "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": _USER_TEMPLATE.format(
                desc="\n".join(_lines(desc_in)) or "(none)",
                omit="\n".join(_lines(omit_in)) or "(none)")}],
        )

        if resp.stop_reason == "refusal":
            log.warning("Polish refused by the model — keeping rule-based text")
            return desc_in, omit_in

        text = next((b.text for b in resp.content if b.type == "text"), "")
        if not text:
            log.warning("Polish returned no text block — keeping rule-based text")
            return desc_in, omit_in

        data = json.loads(text)
        desc_out = _clean_out(data.get("description_lines", []))
        omit_out = _clean_out(data.get("omission_lines", []))

        if not _validate(desc_in, omit_in, desc_out, omit_out):
            return desc_in, omit_in

        result = ("\n".join(desc_out), "\n".join(omit_out))
        _cache[key] = result
        log.info("Polished: description %d -> %d line(s), omission %d -> %d line(s)",
                 len(_lines(desc_in)), len(desc_out), len(_lines(omit_in)), len(omit_out))
        return result

    except Exception as e:
        log.warning("Polish failed (%s: %s) — keeping rule-based text", type(e).__name__, e)
        return desc_in, omit_in
    finally:
        _spent_s += time.monotonic() - started
