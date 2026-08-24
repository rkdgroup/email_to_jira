"""
LLM QC for DSLF fulfilment tickets — the verdict, not a second opinion.

The question this answers: the ticket states what was ORDERED; the SELECT report states
what was actually PULLED. Did the pull deliver the order?

    python qc_llm.py                 # every ticket in Needs QC, print only
    python qc_llm.py DSLF-1075       # one ticket
    python qc_llm.py --post          # also post the verdict as a Jira comment
    python qc_llm.py --model claude-sonnet-5 --effort medium

THREE VERDICTS, AND WHY THE THIRD EXISTS
PASS and FAIL are the model's. UNVERIFIED is the code's, and it is the important one.

This module used to be advisory: the 14 rule-based checks in qc_checker.run_qc_checks()
decided PASS/FAIL and every failure path here returned [] — no findings, no harm, because
the rules still had the last word. As the sole checker that same [] would read as "nothing
wrong found" and pass the ticket. A missing API key, a timeout, an exhausted budget or an
Anthropic outage would silently pass everything in the queue.

So no failure path may return a pass. Every one returns UNVERIFIED with the reason
attached, and UNVERIFIED is not a pass — it means QC did not run and the ticket still
needs a human. knowledge.md puts it best, about its own Jira reads: never let a failed
read become a clean verdict.

THE GATE, NOT THE MODEL, DECIDES PASS/FAIL
_reconcile() overrides the model's own verdict field: any WRONG or BLOCKING-BLANK finding
forces FAIL. A model that lists a wrong Client Database and then says PASS cannot pass the
ticket. Same philosophy as tools_polish._validate — the model proposes, the gate disposes.

WHAT IT IS NOT
Not a check on whether the ticket was created correctly from the broker's order — that is
a different job against a different PDF, and knowledge.md specifies it.
"""

import os
import re
import sys
import json
import time
import base64
import logging
import argparse
from pathlib import Path

log = logging.getLogger(__name__)

# knowledge.md's reasoning, now that this call is the verdict rather than a second opinion:
# "These are production tickets that drive real data pulls; a wrong database or a wrong
# destination sends the wrong donor file to the wrong company. Accuracy matters more than
# speed or cost." Override per run with --model / --effort.
QC_MODEL     = "claude-opus-5"
QC_EFFORT    = "high"
QC_TIMEOUT_S = 90

# Per process. As the advisory pass this was 90s, sized so ~3 tickets got an AI reading
# inside the Jenkins 4-minute timeout and the rest fell back to the rules. There is no
# fallback any more: a ticket past the budget gets UNVERIFIED, i.e. no QC at all. Sized
# here for a full queue instead, which means the Jenkins timeout has to come up with it —
# see the note in CLAUDE.md. Set QC_BUDGET_S=0 to disable the cap.
QC_BUDGET_S  = int(os.getenv("QC_BUDGET_S", "600"))
_MAX_PDF_MB  = 32

PASS       = "PASS"
FAIL       = "FAIL"
UNVERIFIED = "UNVERIFIED"

# Severities, from knowledge.md. The first two force FAIL; NOTE never does.
_BLOCKING = ("WRONG", "BLOCKING-BLANK")

_spent_s = 0.0
_cache: dict = {}

_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "verdict": {"type": "string", "enum": [PASS, FAIL]},
        "delivered": {
            "type": "string",
            "description": "One sentence: did the SELECT deliver what the ticket asked "
                           "for? Name the ask and the delivery.",
        },
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "field":        {"type": "string"},
                    "severity":     {"type": "string",
                                     "enum": ["WRONG", "BLOCKING-BLANK", "NOTE"]},
                    "ticket_value": {"type": "string"},
                    "select_value": {"type": "string"},
                    "expected":     {"type": "string"},
                    "issue":        {"type": "string"},
                },
                "required": ["field", "severity", "ticket_value", "select_value",
                             "expected", "issue"],
            },
        },
    },
    "required": ["verdict", "delivered", "findings"],
}

_SYSTEM = """You are the quality check on a DSLF list-rental fulfilment ticket (Data \
Management Inc.). Your verdict decides whether this pull is considered correct — there is \
no second reviewer behind you and no rule-based check to catch what you miss.

THE TICKET IS THE ORDER. THE SELECT REPORT IS THE DELIVERY.
The ticket states what the client asked for: the select criteria, the quantity, the
availability rule, what to omit, the destination and the file format. The attached SELECT
report is the record of what was actually pulled from the database. Your job is to decide
whether the delivery satisfies the order.

Work in this direction, not the reverse: take each demand the ticket makes, find the
evidence in the SELECT that it was met, and say so. A demand you cannot find evidence for
is a finding. Do not merely scan for two values that look different — a criterion the
SELECT never applied at all is the more serious and more common defect.

WHAT TO CHECK, WORST FIRST
1. Client Database. The SELECT's own database code and customer name must be the client the
   ticket names. A pull from the wrong database sends the wrong donor file to the wrong
   company and is the single worst outcome here. Two real incidents: an order for
   3-NPTA-NAT POLICE / TROOPER AS (N13D) was pulled as N24D, National Blue Line Police
   Foundation; an order for 3-SAVE SURVIVORS & VICTIMS EMP (S30D) was pulled as S32D, SAVE
   Mission Recovery. The shape is always two similarly-named clients and the sibling being
   picked. When the names share a distinctive word, say so explicitly.
2. Select criteria. Every priced select the ticket lists — recency window, dollar band,
   HOTLINE, GENDER, Nth, state or zip select — must be reflected in what the SELECT pulled.
   A ticket asking "12 MONTH $10-$99.99" against a SELECT that pulled 24 months is WRONG.
   Quote both sides.

   DOLLAR BANDS — read this before reporting one, it is the easiest thing here to get
   wrong. "$10+" on an order is NOT an open-ended floor. It means $10.00 through this
   client's contracted cap, and that cap is a per-client term given to you in the CLIENT
   PROFILE block. A pull of "RECENT PAYMENT AMT. = 10.00 THRU 99.99" against an order
   reading "$10+" is CORRECT for a client whose cap is $99.99, and it is the normal,
   expected shape of these reports. Caps genuinely differ between clients — $49.99,
   $99.99, $249.99, $999.99, or none — so judge the SELECT's ceiling against the profile
   cap you were given and nothing else:
     - ceiling matches the client's cap                  -> correct, report nothing
     - ceiling is lower than the cap (cap $99.99, pulled
       10.00 THRU 49.99)                                 -> WRONG, records were lost
     - ceiling where the profile says NO CAP / NONE      -> WRONG unless the order itself
                                                            states a band
     - cap "VARIES PER ORDER" or not recorded            -> the order decides; if the order
                                                            gives no band either, this is a
                                                            NOTE for a human, never WRONG
   The report header naming the select "$10+" while the criteria line reads "10.00 THRU
   99.99" is not a contradiction — the header is the order's shorthand and the criteria
   line is the cap applied. Do not report it as one.
3. Omissions. Every criterion in the ticket's Omission Description must have been applied:
   flag omits, state and zip/SCF omits, OMIT PREVIOUS ORDER, 1 PER HOUSEHOLD, DMA panders.
   An omit the SELECT did not apply means suppressed records shipped. Check the flags and
   the state/zip lists individually, not as a group.
4. Quantity and availability. Under "Nth" the records selected must not exceed the
   requested quantity. Under "All Available" the requested figure is only an estimate and a
   difference is expected and must not be reported.
5. Manager Order Number. The SELECT's P.O.# must match the ticket's Manager Order Number. A
   mismatch means this SELECT belongs to a different order and nothing else you conclude
   about it is reliable — say that plainly.

SEVERITY
WRONG           the delivery contradicts the order, a house rule, or itself.
BLOCKING-BLANK  something required to judge or to fulfil is absent.
NOTE            worth a human's eye, may well be fine.

Use WRONG only when you can name the evidence on both sides. If you cannot tie a ticket
demand to anything in the SELECT either way, that is BLOCKING-BLANK with what you could not
verify stated — uncertainty is a finding of its own, never a silent pass and never a guess
presented as fact.

DO NOT REPORT THESE — correct by design, and flagging them is noise:
- A dollar band whose upper limit equals the client's profile cap, on an order written as
  "$10+" or "$0.01+". That IS the order, executed correctly. See DOLLAR BANDS above.
- Billable Account not sharing the Client Database's prefix. The configured billing account
  legitimately differs: A52D bills to A68, S05D bills to S15, N11D bills to N09.
- Seed Database being the Client Database with a trailing S. That is the rule.
- Seed Tracking Number repeating the Manager Order Number. Intentional.
- File Format "ASCII Fixed" when the ship-to is Saturn, any data-axle.com drop box, or one
  of the fixed-format houses (data@trylondm.com, data@talonmm.com, data@rkdgroup.com,
  tisdata@trinitydirect.net, tapelibrarian@directmail.com). Forced by house rule. For
  Saturn and the data-axle drop box the method is forced to FTP as well.
- Other Fees reading "STATE OMITS" when six or more states, zips or SCFs are omitted.
  Applied automatically.
- A blank Mail Date, File Format, Other Fees, Key Code or Special Seed Instructions.
- Records Selected differing from Requested Quantity when Availability Rule is
  "All Available". Under "Nth" an overage IS worth reporting.
- Standard suppressions, the "Select By" line, and standing FLAG OMITS present in the
  ticket but absent from the SELECT. Those come from the client profile on file rather than
  from this order, so their absence from the SELECT means nothing.
- The ticket's status, work order number, attachments, or title format.

VERDICT
FAIL if any finding is WRONG or BLOCKING-BLANK. PASS only when the SELECT demonstrably
delivered the order and nothing outstanding remains. A clean pass is a normal and common
outcome — do not invent a finding to fill the list. But never pass a ticket whose central
demand you could not verify: say what is unverified instead.

In "delivered", state in one sentence what was asked and what arrived, naming the numbers.
Quote every value verbatim from the ticket or the SELECT. Never paraphrase a field value."""


def _ticket_text(fields: dict) -> str:
    """Readable rendering of the ticket, bullets and line structure preserved."""
    from compare_extraction import adf_to_lines

    out = []
    for label, key in (
        ("Summary", "summary"), ("List Name", "list_name"), ("Mailer Name", "mailer_name"),
        ("List Manager", "list_manager"), ("Manager Order #", "manager_order"),
        ("Client Database", "client_db"), ("Seed Database", "seed_db"),
        ("Billable Account", "billable_account"),
        ("Requested Quantity", "requested_qty"), ("Availability Rule", "availability_rule"),
        ("File Format", "file_format"), ("Shipping Method", "shipping_method"),
        ("Ship To Email", "ship_to_email"), ("Shipping Instructions", "shipping_instructions"),
        ("Other Fees", "other_fees"), ("Mail Date", "mail_date"),
    ):
        v = fields.get(key)
        out.append(f"{label}: {v if v not in (None, '') else '(empty)'}")

    for label, key in (("Description — what to pull", "description_adf"),
                       ("Omission Description — what to suppress", "omission_adf")):
        lines = adf_to_lines(fields.get(key))
        out.append(f"\n{label}:")
        if lines:
            out.extend(f"  {ln}" for ln in lines)
        else:
            out.append("  (empty)")
    return "\n".join(out)


def _select_context(select_data: dict) -> str:
    """The rule-parsed SELECT values, offered as a cross-check rather than as truth.

    These come from regexes over the same PDF the model is reading. They are handed over
    labelled as unreliable on purpose: where they disagree with the PDF the PDF wins, and
    a regex that silently found nothing must not read as an absent value on the report.
    """
    if not select_data:
        return ""
    keep = ("job_number", "client_db", "customer_name", "manager_order", "total_records",
            "mailing_date", "seed_db", "flags", "state_omits", "zip_omits", "criteria")
    rows = []
    for k in keep:
        if k in select_data and select_data[k] not in (None, "", [], {}):
            rows.append(f"  {k}: {select_data[k]}")
    if not rows:
        return ""
    return ("\n\nMACHINE EXTRACTION of the same SELECT PDF, by regex. Treat it as a hint "
            "only — it is frequently incomplete, and a blank here does NOT mean the value "
            "is absent from the report. Where it disagrees with the PDF, the PDF is "
            "authoritative:\n" + "\n".join(rows))


def _profile_context(ticket_fields: dict) -> str:
    """The client's own profile terms, as the authority on what the order's shorthand means.

    Without this the checker cannot read a dollar band at all. "$10+" on an order does NOT
    mean an open-ended floor — it means $10 through *this client's* cap, and the cap is a
    per-client term recorded in their profile document: 60 clients cap at $99.99, 48 at
    $49.99, and a tail run to $249.99, $499.99, $999.99 or no cap at all. Judging "$10+"
    against an assumed open range reports every correctly-executed pull as a defect.
    """
    from parse_pipeline import _PROFILE_MAP

    db = str(ticket_fields.get("client_db") or "").upper()
    prof = _PROFILE_MAP.get(db) or _PROFILE_MAP.get(db[:-1] if db else "")
    if not prof:
        return ("\n\nCLIENT PROFILE: none on file for this database. You cannot confirm a "
                "dollar-band ceiling without it — if the SELECT applied one, say it could "
                "not be verified rather than calling it wrong.")

    rows = [f"  Dollar cap: {prof.get('dollar_cap') or '(not recorded)'}"]
    if prof.get("select_by"):
        rows.append(f"  Select by: {prof['select_by']}")
    if prof.get("flags"):
        rows.append(f"  Standing flag omits: {prof['flags']}")
    return ("\n\nCLIENT PROFILE for " + db + " — the contracted terms for this client, and "
            "authoritative on what the order's shorthand means:\n" + "\n".join(rows))


def _unverified(reason: str) -> dict:
    """The one thing this module must never get wrong: an error is not a pass."""
    log.warning("QC UNVERIFIED — %s", reason)
    return {"verdict": UNVERIFIED, "delivered": "", "findings": [],
            "unverified_reason": reason, "model": None, "elapsed_s": 0.0}


def _reconcile(result: dict) -> dict:
    """Force FAIL when any finding is blocking, whatever the model put in `verdict`.

    A model that lists a WRONG Client Database and then reports PASS must not be able to
    pass the ticket. The gate is the guarantee, not the model's own summary judgement.
    """
    findings = result.get("findings") or []
    blocking = [f for f in findings
                if str(f.get("severity", "")).upper() in _BLOCKING]
    if blocking and result.get("verdict") != FAIL:
        log.warning("Model said %s with %d blocking finding(s) — forcing FAIL",
                    result.get("verdict"), len(blocking))
        result["verdict"] = FAIL
        result["verdict_forced"] = True
    result["blocking_count"] = len(blocking)
    return result


def review(pdf_path: str, ticket_fields: dict, select_data: dict = None,
           model: str = None, effort: str = None) -> dict:
    """Judge the SELECT PDF against the ticket.

    Returns {"verdict": PASS|FAIL|UNVERIFIED, "delivered": str, "findings": [...]}.
    Never raises. UNVERIFIED on every failure path — never a pass.
    """
    global _spent_s

    model  = model or QC_MODEL
    effort = effort or QC_EFFORT

    if not os.getenv("ANTHROPIC_API_KEY"):
        return _unverified("ANTHROPIC_API_KEY not set — no QC ran on this ticket")
    if QC_BUDGET_S and _spent_s >= QC_BUDGET_S:
        return _unverified(f"AI QC budget of {QC_BUDGET_S}s exhausted for this run — "
                           f"this ticket was not checked")

    try:
        data = Path(pdf_path).read_bytes()
    except Exception as e:
        return _unverified(f"cannot read the SELECT PDF ({e})")
    if len(data) > _MAX_PDF_MB * 1024 * 1024:
        return _unverified(f"SELECT PDF exceeds {_MAX_PDF_MB} MB — not sent")

    ticket_text = _ticket_text(ticket_fields)
    cache_key = (hash(data), ticket_text, model, effort)
    if cache_key in _cache:
        return dict(_cache[cache_key])

    started = time.monotonic()
    try:
        import anthropic
        client = anthropic.Anthropic(timeout=QC_TIMEOUT_S)
        resp = client.messages.create(
            model=model,
            max_tokens=8000,
            thinking={"type": "adaptive"},
            output_config={"effort": effort,
                           "format": {"type": "json_schema", "schema": _SCHEMA}},
            system=_SYSTEM,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "document",
                     "source": {"type": "base64", "media_type": "application/pdf",
                                "data": base64.standard_b64encode(data).decode("ascii")}},
                    {"type": "text",
                     "text": f"THE ORDER, as the ticket states it:\n\n{ticket_text}"
                             f"{_profile_context(ticket_fields)}"
                             f"{_select_context(select_data)}\n\n"
                             f"The attached SELECT report is what was actually pulled. "
                             f"Decide whether it delivered this order."},
                ],
            }],
        )
        if resp.stop_reason == "refusal":
            return _unverified("the model refused this SELECT PDF")
        text = next((b.text for b in resp.content if b.type == "text"), "")
        if not text:
            return _unverified("the model returned no output")
        result = json.loads(text)
    except Exception as e:
        return _unverified(f"API call failed ({e}) — no QC ran on this ticket")
    finally:
        _spent_s += time.monotonic() - started

    elapsed = time.monotonic() - started
    result["findings"] = [f for f in (result.get("findings") or [])
                          if isinstance(f, dict) and f.get("field")]
    result["model"] = model
    result["elapsed_s"] = elapsed
    result = _reconcile(result)

    log.info("QC %s: %d finding(s), %d blocking, %.1fs",
             result["verdict"], len(result["findings"]),
             result["blocking_count"], elapsed)
    _cache[cache_key] = dict(result)
    return result


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

_SEV_ORDER = {"WRONG": 0, "BLOCKING-BLANK": 1, "NOTE": 2}


def format_report(ticket_key: str, select_filename: str, result: dict) -> str:
    """The QC comment. Worst finding first, every value quoted."""
    verdict = result.get("verdict", UNVERIFIED)
    lines = [f"QC CHECK RESULTS — {ticket_key}",
             f"SELECT: {select_filename}",
             f"VERDICT: {verdict}"]

    if verdict == UNVERIFIED:
        lines += ["",
                  f"QC DID NOT RUN: {result.get('unverified_reason', 'unknown')}",
                  "",
                  "This is NOT a pass. The ticket has not been checked and still needs a",
                  "human before it ships."]
        return "\n".join(lines)

    if result.get("verdict_forced"):
        lines.append("(verdict forced to FAIL — blocking findings present)")
    if result.get("delivered"):
        lines += ["", f"DELIVERED: {result['delivered']}"]

    findings = sorted(result.get("findings") or [],
                      key=lambda f: _SEV_ORDER.get(str(f.get("severity", "")).upper(), 3))
    if not findings:
        lines += ["", "No discrepancies found between the order and the SELECT."]
    else:
        blocking = result.get("blocking_count", 0)
        lines += ["", f"FINDINGS: {len(findings)} ({blocking} blocking)", ""]
        for i, f in enumerate(findings, 1):
            lines.append(f"{i}. [{f.get('severity')}] {f.get('field')}")
            for label, key in (("Ticket", "ticket_value"), ("SELECT", "select_value"),
                               ("Expected", "expected")):
                v = f.get(key)
                if v:
                    lines.append(f"   {label + ':':<10}{v}")
            if f.get("issue"):
                lines.append(f"   {'Why:':<10}{f['issue']}")
            lines.append("")

    lines.append(f"Checked by {result.get('model')} in {result.get('elapsed_s', 0):.1f}s.")
    return "\n".join(lines).rstrip()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def check_ticket(ticket_key: str, post: bool = False,
                 model: str = None, effort: str = None) -> dict:
    """Download the ticket's SELECT PDF, judge it, optionally comment. Never raises."""
    import tempfile, shutil
    from tools_jira import (get_ticket_qc_fields, download_attachment,
                            add_comment_to_ticket)
    from qc_checker import find_select_attachment, parse_select_pdf

    fields = get_ticket_qc_fields(ticket_key)
    if "error" in fields:
        # A failed Jira read is not an empty queue and not a clean ticket.
        return {"ticket_key": ticket_key,
                **_unverified(f"Jira read failed: {fields['error']}")}

    select_att, _ = find_select_attachment(fields.get("attachments") or [])
    if not select_att:
        return {"ticket_key": ticket_key,
                **_unverified("no SELECT PDF attached — nothing to check the order against")}

    tmp_dir = tempfile.mkdtemp(prefix="dslf_qcllm_")
    tmp_path = os.path.join(tmp_dir, select_att["filename"])
    try:
        try:
            download_attachment(select_att["content"], tmp_path)
        except Exception as e:
            return {"ticket_key": ticket_key,
                    **_unverified(f"SELECT PDF download failed: {e}")}

        select_data = parse_select_pdf(tmp_path)
        select_data.pop("parse_errors", None)

        result = review(tmp_path, fields, select_data, model=model, effort=effort)
        result["ticket_key"]      = ticket_key
        result["select_filename"] = select_att["filename"]
        result["report"]          = format_report(ticket_key, select_att["filename"], result)

        if post:
            cr = add_comment_to_ticket(ticket_key, result["report"], code_block=True)
            result["posted"] = "error" not in cr
            if not result["posted"]:
                log.error("Could not post QC comment to %s: %s", ticket_key, cr["error"])
        return result
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")

    ap = argparse.ArgumentParser(
        description="LLM QC of DSLF tickets: did the SELECT deliver what the ticket asked?")
    ap.add_argument("tickets", nargs="*",
                    help="ticket keys; default is every ticket in Needs QC")
    ap.add_argument("--post", action="store_true",
                    help="post the verdict as a Jira comment (default: print only)")
    ap.add_argument("--model", default=QC_MODEL)
    ap.add_argument("--effort", default=QC_EFFORT,
                    choices=("low", "medium", "high", "xhigh", "max"))
    ap.add_argument("--json", metavar="FILE", help="write all results to FILE as JSON")
    args = ap.parse_args()

    keys = args.tickets
    if not keys:
        from tools_jira import search_issues_paged
        from qc_checker import NEED_QC_STATUS
        jql = f'project = DSLF AND status = "{NEED_QC_STATUS}" ORDER BY created ASC'
        issues = search_issues_paged(jql, "summary")
        keys = [i["key"] for i in issues]
        print(f"{len(keys)} ticket(s) in {NEED_QC_STATUS}\n")
    if not keys:
        print("Nothing to check.")
        return 0

    results = []
    for k in keys:
        r = check_ticket(k, post=args.post, model=args.model, effort=args.effort)
        results.append(r)
        print("\n" + "=" * 78)
        print(r.get("report") or format_report(k, r.get("select_filename", "—"), r))

    tally = {}
    for r in results:
        tally[r.get("verdict", UNVERIFIED)] = tally.get(r.get("verdict", UNVERIFIED), 0) + 1
    print("\n" + "=" * 78)
    print("  ".join(f"{v} {k}" for k, v in sorted(tally.items())))
    if tally.get(UNVERIFIED):
        print(f"\n{tally[UNVERIFIED]} ticket(s) UNVERIFIED — not checked, not passed.")

    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2, default=str),
                                   encoding="utf-8")
        print(f"Wrote {args.json}")

    return 1 if tally.get(FAIL) or tally.get(UNVERIFIED) else 0


if __name__ == "__main__":
    sys.exit(main())
