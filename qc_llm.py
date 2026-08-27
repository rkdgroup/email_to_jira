"""
The QC checker for DSLF list-rental tickets. One file, one checker, no rule engine.

Two questions, asked by the same command against the same ticket:

  ORDER CHECK   Was this ticket CREATED correctly from the broker's order PDF?
                Runs on any ticket with a recognisable order attached. Its findings are
                the fixable ones — the order is authoritative, so a wrong Mailer PO is a
                transcription error with a known correct value. With --fix those are
                written back to Jira.

  SELECT CHECK  Did the PULL deliver what the ticket asked for? Ticket vs the SELECT
                report. Its findings are NOT fixable by editing the ticket: a select run
                on the wrong dollar band needs re-running, and editing the ticket to match
                a bad pull would destroy the evidence. Report only, always.

    python qc_llm.py                     # every ticket in Needs QC
    python qc_llm.py DSLF-1075           # one ticket (accepts several keys)
    python qc_llm.py --status "Needs Assignment"   # the creation-check queue
    python qc_llm.py --post              # post the verdict as a Jira comment
    python qc_llm.py --post --fix        # also apply the order-check corrections
    python qc_llm.py --order-only | --select-only
    python qc_llm.py --model M --effort low|medium|high|xhigh|max --json FILE

THREE VERDICTS, AND THE THIRD IS THE POINT
PASS and FAIL are the model's. UNVERIFIED is the code's, and it is returned by every
failure path: no API key, timeout, exhausted budget, API error, refusal, unreadable or
oversize PDF, failed Jira read. UNVERIFIED IS NOT A PASS — it means QC did not run and the
ticket still needs a human. Every failure path must return it. The previous advisory
version returned [] on failure, which was harmless only because rules still decided; as
the sole checker that same [] reads as "nothing wrong found" and passes the whole queue.

THE GATE, NOT THE MODEL, DECIDES
_reconcile() forces FAIL whenever any finding is WRONG or BLOCKING-BLANK, whatever the
model wrote in `verdict`. A model cannot list a wrong Client Database and still pass the
ticket. Same philosophy as tools_polish._validate — the model proposes, the gate disposes.

WHY THERE IS NO RULE-BASED CHECKER ANY MORE
There was one (qc_checker.run_qc_checks, 14 regex checks). Its verdict was
`pass_count >= 4 and not hard_fails`: failures were never counted and never subtracted, so
a ticket could carry any number of non-hard FAILs and still pass on four passes. Measured:
4 passes / 5 fails -> QC PASSED. The denominator moved too, because WARN rows were dropped
entirely, so the same threshold meant different things on different tickets. What it knew
that was worth keeping is now stated in the two prompts below rather than in if-statements.
The SELECT text regexes it also owned live on in select_pdf.py, which makes no judgements.
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

_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))

log = logging.getLogger(__name__)

# These are production tickets that drive real data pulls; a wrong database or a wrong
# destination sends the wrong donor file to the wrong company. Accuracy beats speed and
# cost. Override per run with --model / --effort.
QC_MODEL     = "claude-opus-5"
QC_EFFORT    = "high"
QC_TIMEOUT_S = 90

# Per process, across both checks. Two calls per ticket now, so a full queue costs roughly
# double what the SELECT-only pass did. QC_BUDGET_S=0 disables the cap.
QC_BUDGET_S  = int(os.getenv("QC_BUDGET_S", "900"))
_MAX_PDF_MB  = 32

NEED_QC_STATUS     = "Needs QC"
NEEDS_ASSIGN_STATUS = "Needs Assignment"

PASS       = "PASS"
FAIL       = "FAIL"
UNVERIFIED = "UNVERIFIED"

# The first two force FAIL; NOTE never does.
_BLOCKING = ("WRONG", "BLOCKING-BLANK")

_spent_s = 0.0
_cache: dict = {}


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

def _finding_props(with_fix: bool) -> dict:
    props = {
        "field":        {"type": "string"},
        "severity":     {"type": "string", "enum": ["WRONG", "BLOCKING-BLANK", "NOTE"]},
        "ticket_value": {"type": "string"},
        "select_value": {"type": "string"},
        "expected":     {"type": "string"},
        "issue":        {"type": "string"},
    }
    if with_fix:
        props["fix_field"] = {
            "type": "string",
            "description": "Machine key of the ticket field to correct, or \"\" when this "
                           "finding is not a simple field correction. Must be one of: "
                           + ", ".join(sorted(_FIXABLE)),
        }
        props["fix_value"] = {
            "type": "string",
            "description": "The corrected value, verbatim from the order PDF. \"\" when "
                           "you are not proposing a write.",
        }
    return props


def _schema(with_fix: bool) -> dict:
    required = ["field", "severity", "ticket_value", "select_value", "expected", "issue"]
    if with_fix:
        required += ["fix_field", "fix_value"]
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "verdict": {"type": "string", "enum": [PASS, FAIL]},
            "delivered": {
                "type": "string",
                "description": "One sentence naming what was asked and what arrived.",
            },
            "findings": {
                "type": "array",
                "items": {"type": "object", "additionalProperties": False,
                          "properties": _finding_props(with_fix), "required": required},
            },
        },
        "required": ["verdict", "delivered", "findings"],
    }


# ---------------------------------------------------------------------------
# Auto-fix whitelist
# ---------------------------------------------------------------------------
#
# Only the ORDER check produces fixes, and only for fields the broker's order PDF is
# authoritative on. Three groups are deliberately absent:
#
#   client_db / seed_db / billable_account — resolved from config by client_lookup, not
#     read off the order. They are also select fields whose options are resolved against
#     a live createmeta lookup, and a wrong write here is the single worst outcome in this
#     system: the wrong donor file to the wrong company. Reported, never written.
#   description / omission — ADF prose owned by the parsers and tools_polish. A field-level
#     overwrite would flatten the bullet structure that _build_adf_description creates.
#   status / work order — not data, and not this tool's business.
#
from tools_jira import (AVAILABILITY_RULE_OPTIONS, FILE_FORMAT_OPTIONS,
                        SHIPPING_METHOD_OPTIONS)

_FIXABLE = {
    "summary":               ("summary",           "text"),
    "manager_order":         ("customfield_12192", "text"),
    "mailer_po":             ("customfield_12193", "text"),
    "mailer_name":           ("customfield_12194", "text"),
    "list_name":             ("customfield_12234", "text"),
    "list_manager":          ("customfield_12231", "manager"),
    "key_code":              ("customfield_12195", "text"),
    "mail_date":             ("customfield_12196", "date"),
    "due_date":              ("duedate",           "date"),
    "requested_qty":         ("customfield_12271", "number"),
    "availability_rule":     ("customfield_12273", "option"),
    "file_format":           ("customfield_12274", "option"),
    "shipping_method":       ("customfield_12276", "option"),
    "ship_to_email":         ("customfield_12275", "text"),
    "shipping_instructions": ("customfield_12277", "text"),
    "requestor_name":        ("customfield_12232", "text"),
    "requestor_email":       ("customfield_12233", "text"),
    "seed_tracking":         ("customfield_12272", "text"),
    "other_fees":            ("customfield_12278", "text"),
    "special_seed":          ("customfield_12311", "text"),
}

_OPTION_MAPS = {
    "availability_rule": AVAILABILITY_RULE_OPTIONS,
    "file_format":       FILE_FORMAT_OPTIONS,
    "shipping_method":   SHIPPING_METHOD_OPTIONS,
}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_SEVERITY_BLOCK = """\
SEVERITY
WRONG           the ticket contradicts the source document, a house rule, or itself.
BLOCKING-BLANK  something required to judge or to fulfil the order is absent. This is also
                where "I could not verify it either way" goes.
NOTE            worth a human's eye, may well be fine.

Use WRONG only when you can name the evidence on both sides. If you cannot tie a demand to
anything in the document either way, that is BLOCKING-BLANK with what you could not verify
stated — uncertainty is a finding of its own, never a silent pass and never a guess
presented as fact.

Quote every value verbatim from the ticket or the document. Never paraphrase a field value.
A clean pass is a normal and common outcome — do not invent a finding to fill the list.

LENGTH — these findings are read in a Jira comment, so be brief
"ticket_value", "select_value" and "expected" hold VALUES ONLY, quoted verbatim. No
commentary, no parenthetical explanation, no restating the rule inside them.
"issue" is at most ONE clause of 15 words saying what is wrong. It must add something the
values do not already show — if the two values make the problem obvious on their own, leave
"issue" empty. Never explain the rule, never re-quote a value you already put in a value
field, never justify your severity.
"delivered" is one clause of at most 20 words.
Long findings get skimmed and the real defect gets missed, so length costs accuracy."""


_SYSTEM_SELECT = """You are the quality check on a DSLF list-rental fulfilment ticket (Data \
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

1. CLIENT DATABASE. The SELECT's own database code and customer name must be the client the
   ticket names. A pull from the wrong database sends the wrong donor file to the wrong
   company and is the single worst outcome here. Two real incidents: an order for
   3-NPTA-NAT POLICE / TROOPER AS (N13D) was pulled as N24D, National Blue Line Police
   Foundation; an order for 3-SAVE SURVIVORS & VICTIMS EMP (S30D) was pulled as S32D, SAVE
   Mission Recovery. The shape is always two similarly-named clients and the sibling being
   picked. When the names share a distinctive word, say so explicitly.

   ONE EXCEPTION, and it is common: on a HOSTED list the SELECT prints the host/master
   account name while the ticket names the rented list, and the two legitimately differ.
   A SELECT customer of AREIVIM against a ticket list of 3-HOC HEAL OUR CHILDREN, or
   NEWPORT CREATIVE SWEEPS MASTER against 3-SDCA CHARITABLE APPEALS MF, is this shape and
   not a wrong-client incident. The database CODE still has to match the ticket. Where the
   code matches and only the printed name differs, that is a NOTE naming both, never WRONG.

2. MANAGER ORDER NUMBER. The SELECT's P.O.# must match the ticket's Manager Order Number. A
   mismatch means this SELECT belongs to a different order and nothing else you conclude
   about it is reliable — say that plainly and make it the first finding.

3. SELECT CRITERIA. Every priced select the ticket lists — recency window, dollar band,
   HOTLINE, GENDER, Nth, state or zip select — must be reflected in what the SELECT pulled.
   A ticket asking "12 MONTH $10-$99.99" against a SELECT that pulled 24 months is WRONG.
   Quote both sides.

   DOLLAR BANDS — read this before reporting one, it is the easiest thing here to get
   wrong. "$10+" on an order is NOT an open-ended floor. It means $10.00 through this
   client's contracted cap, and that cap is a per-client term given to you in the CLIENT
   PROFILE block and normally also printed on the ticket as "Dollar Cap:". A pull of
   "RECENT PAYMENT AMT. = 10.00 THRU 99.99" against an order reading "$10+" is CORRECT for
   a client whose cap is $99.99, and it is the normal, expected shape of these reports.
   Caps genuinely differ between clients — $49.99, $99.99, $249.99, $999.99, or none — so
   judge the SELECT's ceiling against the cap you were given and nothing else:
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

4. THE INCLUDE SET. The account-history table's "INCLUDE BY ACCOUNT #:" line names the
   universe the select was drawn from, with a title like "10-99.99 L3M". It states the
   criteria independently of the REPORT line, so it catches a select run off the wrong
   pool — which the REPORT line alone cannot reveal, because that line only echoes what
   was typed on the job.

   Judge it in ONE direction only. The include set is a STANDING universe and is routinely
   WIDER than any single order: "0-49.99 L3M" backing a "$5+" order, an L6M window backing
   a 4-month ask. Wider is correct and must not be reported. What matters is that the
   universe is never NARROWER than the ask — a higher floor or a shorter window means
   qualifying donors were never in the pool to begin with:
     - include floor ABOVE the order's floor (pool starts at $10, order asks $5+)
       -> WRONG: donors giving $5-$10 could never have been selected
     - include window SHORTER than the ask (pool is L3M, order asks 12 months)
       -> WRONG: the universe is too narrow
   Read the ask from the ticket description first; fall back to the SELECT's own REPORT
   line only when the description states neither in a readable form, and say which you
   used. More than one include set on a select is worth a NOTE naming the others.

5. RECORDS SELECTED. A finished SELECT can never legitimately have selected 0 records — an
   empty output file is always a failure, whatever else passed, and no availability rule
   excuses it. Report a 0 as WRONG.
     - Availability Rule "Nth": the count must NOT EXCEED the requested quantity. An
       overage is WRONG. Under is fine.
     - Availability Rule "All Available": the requested figure is only an estimate. A
       difference in either direction is expected and must NOT be reported.

6. OMISSIONS. Every criterion in the ticket's Omission Description must have been applied:
   flag omits, state and zip/SCF omits, OMIT PREVIOUS ORDER, 1 PER HOUSEHOLD, DMA panders.
   An omit the SELECT did not apply means suppressed records shipped. Check the flags and
   the state/zip lists individually, not as a group, and judge each asymmetrically:
     - a flag, state or zip the TICKET requires that the SELECT did not omit -> WRONG,
       suppressed records shipped
     - an EXTRA omit in the SELECT that the ticket does not name -> NOTE at most. Selects
       carry standing suppressions that never appear on an order.
     - the ticket's omission saying only "FLAG OMITS: FLAGS LISTED BELOW IN SPECIAL INST."
       or similar, with the real flags left as prose -> BLOCKING-BLANK. The flags cannot
       be verified at all; ask for them to be listed explicitly as "FLAG OMITS: <codes>".

7. DELIVERY. File format, shipping method, destination and CC, judged against the SELECT's
   own NOTES block. The CC address the SELECT names should appear in the ticket's Shipping
   Instructions. On an FTP order the ticket's Ship To Email should read
   "FTP NOTIFY: someone@domain" — a bare address there is a NOTE, not a failure.

""" + _SEVERITY_BLOCK + """

DO NOT REPORT THESE — correct by design, and flagging them is noise:
- A dollar band whose upper limit equals the client's profile cap, on an order written as
  "$10+" or "$0.01+". That IS the order, executed correctly. See DOLLAR BANDS above.
- A hosted list's SELECT printing the host/master account name where the ticket names the
  rented list, when the database code itself matches. See 1.
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
- Standard suppressions, the "Select By" line, the "Dollar Cap" line, and standing FLAG
  OMITS present in the ticket but absent from the SELECT. Those come from the client
  profile on file rather than from this order, so their absence from the SELECT means
  nothing.
- The ticket's status, work order number, attachments, or title format.

VERDICT
FAIL if any finding is WRONG or BLOCKING-BLANK. PASS only when the SELECT demonstrably
delivered the order and nothing outstanding remains. Never pass a ticket whose central
demand you could not verify: say what is unverified instead.

In "delivered", name the ask and the delivery in one short clause with the numbers, e.g.
"asked 5,000 Nth, pulled 4,847". Leave "fix_field" and "fix_value" out of your thinking
here — this check proposes no writes."""


_SYSTEM_ORDER = """You are checking whether a DSLF list-rental Jira ticket was CREATED \
correctly from the broker's purchase order (Data Management Inc.). The attached PDF is the \
order as the broker sent it. The ticket is what the pipeline made of it. Your job is to \
find every place the ticket misreads, drops or misfiles what the order says.

THE ORDER PDF IS AUTHORITATIVE on everything printed on it. The ticket is not.

For each finding that is a plain field correction, set "fix_field" to the machine key and
"fix_value" to the corrected value read verbatim off the order. Leave both "" when the
finding is not a single-field correction — a structural problem, a missing document, or
anything you are not certain enough to write. Those writes are applied to the live ticket,
so propose one only when the order states the correct value plainly and you can quote it.

Never propose a fix for client_db, seed_db, billable_account, the Description or the
Omission Description. Those are resolved from configuration or built by the pipeline, not
read off the order. Report them; do not try to correct them.

WHAT TO CHECK

1. IDENTITY
   - Title must be "{LIST NAME} - {MAILER NAME} - {MANAGER ORDER NUMBER}", e.g.
     "JUDICIAL WATCH DONORS - HERITAGE FOUNDATION - W74926JW". This applies to EVERY
     broker including KAP. A title of the shape "P.O. DL984 AID FOR STARVING CHILDREN" is
     WRONG — that was an old KAP defect, not a design, and 64 tickets carried it.
     The number in the title is the MANAGER ORDER NUMBER, never the Mailer PO.
   - List Manager must be exactly one of: ADSTRA, AALC, AMLC, CELCO, CONRAD, DATA-AXLE,
     KAP, MARY E GRANGER, NEGEV, NAMES IN THE NEWS, RKD, RMI, WASHINGTON LISTS,
     WE ARE MOORE. Anything else is WRONG — it breaks the database lookup. Note that
     SimioCloud orders are WE ARE MOORE (SimioCloud is their ordering platform), and
     RKD-serviced AMLC orders are AMLC.
   - Mailer PO and Manager Order # must be different values. Identical, or the manager
     order number sitting in Mailer PO, is WRONG. Which line each comes from, by broker:
       ADSTRA          Mailer PO 6-digit or BRK-prefixed   | Mgr Order J- or I-prefix
       RMI             Broker PO# field                    | MGT#
       WE ARE MOORE    Ship Label number                   | Order#
       DATA AXLE       Ship Label "PO:" with suffix        | Order#
       WASHINGTON      Client Reference with suffix        | Order Number
       KAP             the "Broker order:" value           | "KAP Order:" DL- or DM-prefix
       CONRAD DIRECT   BROK/MAIL PO: field                 | PURCHASE ORDER NO
       NAMES IN NEWS   6-7 digit number                    | LR #
       CELCO           ORDER #                             | ORDER #  (same value)
       RKD / AMLC      "Client P.O.:" — in AMLC's columnar layout the value can sit up to
                       25 lines BELOW its label            | first 5-6 digit number in the
                                                             first 10 lines
   - Seed Tracking Number must equal the Manager Order #. Different, or blank when a
     manager order exists, is WRONG and fixable.
   - Mailer Name is the ORGANISATION SENDING THE MAIL. List Name is the DONOR LIST BEING
     RENTED. If they are swapped — Mailer Name reading like a donor list, List Name like a
     mailing organisation — that is WRONG. On the order forms the words "Mailer" and
     "Broker" are used interchangeably: read the value, not the label.

2. THE DATABASE TRIAD — the highest-value check, and report-only
   Client Database (N11D), Seed Database (N11S) and Billable Account (N09) come from one
   db_code resolved from configuration.
   - All three blank: BLOCKING-BLANK. But first check the known-missing list below.
   - Seed must be Client's code with the last character replaced by S (S05D -> S05S).
     A mismatch is WRONG.
   - Billable differing from Client-minus-suffix is a NOTE, never WRONG. Legitimate by
     design: S05D bills to S15, A52D to A68, N11D to N09.
   - WRONG CLIENT is the one that matters most. Compare the List Name on the order against
     the organisation the database code actually belongs to. Two real incidents, both the
     same shape — two similarly-named charities and the ticket landing on the sibling:
       3-NPTA-NAT POLICE / TROOPER AS is N13D; the ticket was given N24D (National Blue
       Line Police Foundation). Shared token: POLICE.
       3-SAVE SURVIVORS & VICTIMS EMP is S30D; the ticket was given S32D (SAVE Mission
       Recovery). Shared token: SAVE.
     When the list name shares a distinctive word with another known client, say so
     explicitly and ask for human confirmation rather than assuming it is fine.
   - An ADSTRA list name carrying a 5-digit code in parentheses — "NLEOMF DONORS (49210)"
     — makes that code the definitive identifier. Quote it in the finding.
   - KNOWN MISSING JIRA OPTIONS, do not report as a parse bug: C65D (3-CARI CHILDREN AT
     RISK INTL), X14D (3-ASFD AUTISM SPECTR DIS FOUND), M84D (3-MBF MANS BEST FRIEND),
     N13D (3-NPTA-NAT POLICE / TROOPER AS). These db_codes are correct but have no option
     in the Jira select fields, so the triad comes out blank. Report BLOCKING-BLANK with
     "needs a Jira admin to add the option — not a data error". Nobody should hunt a
     parser bug that is not there.
   - A ticket whose list is "BFF- BRIGHTFOCUS FDN MF (DMI)" resolves to A63D, which is on
     the skip list and should never have produced a ticket at all. Report that as WRONG,
     phrased as "should have been skipped, not created".

3. DESCRIPTION vs OMISSION DESCRIPTION — what goes where
   The Description holds what to PULL: recency windows, dollar bands, HOTLINE, GENDER,
   Nth, plus the profile's "Select By", "Dollar Cap", "Standard Suppressions" and "Special
   Instructions" blocks. The Omission Description holds everything OMITTED or SUPPRESSED:
   flags, states, zips/SCFs, OMIT PREVIOUS ORDER, 1 PER HOUSEHOLD, DMA panders, and the
   profile's "FLAG OMITS:" line.
   - A line containing OMIT, EXCLUDE, SUPPRESS, PANDER, 1 PER HOUSEHOLD or a run of state
     codes, sitting in the Description body, is WRONG — it belongs in Omission. A select
     line reading "0-12 $10+ OMIT MN MS NC" should have left its state omit in Omission.
   - The Description mentioning omits while the Omission Description is empty is WRONG.
   - A Description containing ONLY the profile blocks, with no order-specific select line
     above them, means the ticket lost its criteria. WRONG.
   - A missing "FLAG OMITS:" line is a NOTE.
   - The order stating a dollar select while the Description carries no "Dollar Cap:" line
     is a NOTE: the cap is what tells a reader and the fulfilment check that "$10+" is
     bounded. Not fixable from the order — the cap comes from the client profile.
   - A SHORT Description is not itself a problem. Flag only when the order-specific
     criteria are absent or misfiled.

4. QUANTITY AND AVAILABILITY
   - Availability Rule must be "Nth" or "All Available"; blank is BLOCKING-BLANK. On the
     orders, "Full Run" means All Available and "NTH NAME" means Nth.
   - "Nth" with no quantity is WRONG — an Nth select needs a target.
   - Quantity blank is WRONG under Nth, a NOTE under All Available.
   - A quantity that is really a price (100, 95) or a fragment of a date is WRONG: a
     number was read off the wrong line.
   - KNOWN TRAP: on exchange orders the quantity is labelled "Exch Qty", not "Rental Qty".
     A blank quantity on an exchange order is that trap's signature.
   - KNOWN TRAP: broker boilerplate reading "Please provide the all available quantity
     before shipping for approval" is NOT an instruction to pull all available. A ticket
     saying All Available while also carrying a specific Nth-sized target is that trap.

5. DELIVERY — and only the Ship To block of the order decides it
   A broker, list manager or contact named anywhere else on the page is irrelevant.
   - Ship To Email equal to the Requestor Email is WRONG. The requestor is the broker's
     own rep — they are the CC, not the destination.
   - Ship To at a broker's own domain (keyacquisition.com, adstradata.com) is WRONG.
   - Ship To at a list-agency domain (rmlc.net, veradata.com, esteemarketing.com,
     dmgroup.com, maryegranger.com) is usually the mailer's agency contact rather than the
     drop point. WRONG when the ticket looks like a plain email delivery; name the address.
   - These destinations always mean File Format = ASCII Fixed, delivery stays Email:
     data@trylondm.com, data@talonmm.com, data@rkdgroup.com, tisdata@trinitydirect.net,
     tapelibrarian@directmail.com. Anything else on those addresses is WRONG.
   - incoming.files@data-axle.com means ASCII Fixed AND FTP with the "FTP NOTIFY:" prefix.
     A PERSON's mailbox at data-axle.com does not — Data Axle is also a broker and their
     staff appear on orders routinely. Do not confuse the two.
   - Any order mentioning Saturn ships ASCII Fixed via FTP with Ship To rewritten as
     "FTP NOTIFY: ... (SATURN CORP)".
   - Shipping Method FTP with a bare address in Ship To Email, no "FTP NOTIFY:" prefix,
     is a NOTE.
   - Shipping Instructions is normally "CC: {requestor email}". Holding the destination
     instead is a NOTE.
   - A typo'd requestor address copied faithfully off the order — rwojack@keyacquistion.com
     for rwojack@keyacquisition.com — is WRONG and fixable: mail to it bounces. Quote the
     corrected spelling.
   - Blank File Format is not a defect: new tickets default to ASCII Delimited.

6. REQUESTOR — the LIST MANAGER's contact, never the mailer's or the broker's rep.
   Defaults when the order names nobody: ADSTRA -> BOBBI DURRETT,
   BOBBI.DURRETT@ADSTRADATA.COM; RMI -> ALICIA GALLAGHER, AGALLAGHER@RMIDIRECT.COM;
   WE ARE MOORE -> MICHELLE NAY, MNAY@WEAREMOORE.COM; KAP -> JENNY GOMEZ,
   jgomez@keyacquisition.com; CONRAD DIRECT -> Brenda Gundlah, bgundlah@conraddirect.com.
   A requestor email at the MAILER's domain rather than the list manager's is WRONG.

7. OTHER FEES should read "STATE OMITS" when the Omission Description lists six or more
   states, zips or SCFs (count state codes plus 3-5 digit numbers). Below six it stays
   blank. Both directions are a NOTE at most — this is automatic and expected.

8. SPECIAL SEED INSTRUCTIONS holds only "Insert:" lines, and is blank on most orders.
   FTP or email details in there are WRONG.

9. DATES. Mail Date and Due Date (the order's "Ship By") must be YYYY-MM-DD and must match
   the order. A date in the wrong slot — ship-by in Mail Date — is WRONG.

""" + _SEVERITY_BLOCK + """

VERDICT
FAIL if any finding is WRONG or BLOCKING-BLANK. PASS when the ticket faithfully reproduces
the order. In "delivered", name the order in one short clause, e.g. "KAP DL995, CRU Inner
City, 5,000 Nth". Put the order's own wording in "select_value" — for this check that field
means "what the ORDER says" — and keep it to the shortest fragment that proves the point,
not the whole line off the page."""


# ---------------------------------------------------------------------------
# Context builders
# ---------------------------------------------------------------------------

def _ticket_text(fields: dict) -> str:
    """Readable rendering of the ticket, bullets and line structure preserved."""
    from compare_extraction import adf_to_lines

    out = []
    for label, key in (
        ("Summary", "summary"), ("Status", "status"),
        ("List Name", "list_name"), ("Mailer Name", "mailer_name"),
        ("List Manager", "list_manager"), ("Manager Order #", "manager_order"),
        ("Mailer PO", "mailer_po"), ("Seed Tracking Number", "seed_tracking"),
        ("Client Database", "client_db"), ("Seed Database", "seed_db"),
        ("Billable Account", "billable_account"),
        ("Requested Quantity", "requested_qty"), ("Availability Rule", "availability_rule"),
        ("File Format", "file_format"), ("Shipping Method", "shipping_method"),
        ("Ship To Email", "ship_to_email"), ("Shipping Instructions", "shipping_instructions"),
        ("Requestor Name", "requestor_name"), ("Requestor Email", "requestor_email"),
        ("Key Code", "key_code"), ("Other Fees", "other_fees"),
        ("Special Seed Instructions", "special_seed"),
        ("Mail Date", "mail_date"), ("Due Date (Ship By)", "due_date"),
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


def _load_adstra_flag_omits() -> dict:
    """seed_database → expected flag-omit set, from config/adstra_omit_database.yaml.

    Carried over from the deleted rule-based checker, where it was a third source of truth
    on flags beside the SELECT and the ticket. It is configuration, not a regex, so it
    survives: ADSTRA publishes a standing per-database omit set and a SELECT that does not
    match it is worth a human's eye.
    """
    yaml_path = _ROOT / "config" / "adstra_omit_database.yaml"
    if not yaml_path.exists():
        return {}
    try:
        import yaml
        with open(yaml_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        log.warning("Could not load %s: %s", yaml_path.name, e)
        return {}
    result: dict = {}
    for entry in data.get("adstra_database", []):
        seed_db = str(entry.get("seed_database", "")).upper()
        flags   = entry.get("flags", [])
        if seed_db and flags and seed_db not in result:
            result[seed_db] = {ch for fl in flags for ch in str(fl)}
    return result


_ADSTRA_FLAG_OMITS = _load_adstra_flag_omits()


def _adstra_flag_context(ticket_fields: dict) -> str:
    """ADSTRA's published standing flag omits for this seed database, when there are any."""
    seed = str(ticket_fields.get("seed_db") or "").strip().upper()
    expected = _ADSTRA_FLAG_OMITS.get(seed)
    if not expected:
        return ""
    return (f"\n\nADSTRA STANDING FLAG OMITS for seed database {seed}: "
            f"{sorted(expected)}. This is ADSTRA's published default set for this database, "
            f"a third source beside the ticket and the report. A SELECT omitting a different "
            f"set is worth a NOTE naming both — it is not automatically wrong, because an "
            f"individual order can vary it.")


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------

def _unverified(reason: str, check: str = "") -> dict:
    """The one thing this module must never get wrong: an error is not a pass."""
    log.warning("QC UNVERIFIED%s — %s", f" [{check}]" if check else "", reason)
    return {"verdict": UNVERIFIED, "delivered": "", "findings": [],
            "unverified_reason": reason, "model": None, "elapsed_s": 0.0,
            "check": check, "blocking_count": 0}


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


def _worst(*verdicts) -> str:
    """Combine check verdicts. UNVERIFIED outranks FAIL: not knowing is worse than knowing."""
    vs = [v for v in verdicts if v]
    if not vs:
        return UNVERIFIED
    for v in (UNVERIFIED, FAIL):
        if v in vs:
            return v
    return PASS


# ---------------------------------------------------------------------------
# The call
# ---------------------------------------------------------------------------

def _review(pdf_path: str, system: str, schema: dict, user_text: str,
            check: str, model: str = None, effort: str = None) -> dict:
    """One PDF, one prompt, one verdict. Never raises. UNVERIFIED on every failure path."""
    global _spent_s

    model  = model or QC_MODEL
    effort = effort or QC_EFFORT

    if not os.getenv("ANTHROPIC_API_KEY"):
        return _unverified("ANTHROPIC_API_KEY not set — no QC ran on this ticket", check)
    if QC_BUDGET_S and _spent_s >= QC_BUDGET_S:
        return _unverified(f"AI QC budget of {QC_BUDGET_S}s exhausted for this run — "
                           f"this ticket was not checked", check)

    try:
        data = Path(pdf_path).read_bytes()
    except Exception as e:
        return _unverified(f"cannot read the PDF ({e})", check)
    if len(data) > _MAX_PDF_MB * 1024 * 1024:
        return _unverified(f"PDF exceeds {_MAX_PDF_MB} MB — not sent", check)

    cache_key = (hash(data), user_text, system[:64], model, effort)
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
                           "format": {"type": "json_schema", "schema": schema}},
            system=system,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "document",
                     "source": {"type": "base64", "media_type": "application/pdf",
                                "data": base64.standard_b64encode(data).decode("ascii")}},
                    {"type": "text", "text": user_text},
                ],
            }],
        )
        if resp.stop_reason == "refusal":
            return _unverified("the model refused this PDF", check)
        text = next((b.text for b in resp.content if b.type == "text"), "")
        if not text:
            return _unverified("the model returned no output", check)
        result = json.loads(text)
    except Exception as e:
        return _unverified(f"API call failed ({e}) — no QC ran on this ticket", check)
    finally:
        _spent_s += time.monotonic() - started

    elapsed = time.monotonic() - started
    result["findings"] = [f for f in (result.get("findings") or [])
                          if isinstance(f, dict) and f.get("field")]
    result["model"]     = model
    result["elapsed_s"] = elapsed
    result["check"]     = check
    result = _reconcile(result)

    log.info("QC[%s] %s: %d finding(s), %d blocking, %.1fs",
             check, result["verdict"], len(result["findings"]),
             result["blocking_count"], elapsed)
    _cache[cache_key] = dict(result)
    return result


def review_select(pdf_path: str, ticket_fields: dict,
                  model: str = None, effort: str = None) -> dict:
    """Did the SELECT deliver what the ticket asked for?"""
    try:
        user = ("THE ORDER, as the ticket states it:\n\n"
                + _ticket_text(ticket_fields)
                + _profile_context(ticket_fields)
                + _adstra_flag_context(ticket_fields)
                + "\n\nThe attached SELECT report is what was actually pulled. Decide "
                  "whether it delivered this order.")
    except Exception as e:
        # Assembling the prompt reads the profile YAML and the ticket's ADF. If that
        # throws, the check has not run — and "has not run" is UNVERIFIED, never a pass.
        return _unverified(f"could not assemble the ticket context ({e})", "SELECT")
    return _review(pdf_path, _SYSTEM_SELECT, _schema(False), user, "SELECT", model, effort)


def review_order(pdf_path: str, ticket_fields: dict,
                 model: str = None, effort: str = None) -> dict:
    """Was the ticket created correctly from the broker's order?"""
    try:
        user = ("THE TICKET, as the pipeline created it:\n\n"
                + _ticket_text(ticket_fields)
                + _profile_context(ticket_fields)
                + "\n\nThe attached PDF is the broker's purchase order, the document this "
                  "ticket was created from and authoritative on everything printed on it. "
                  "Decide whether the ticket reproduces it correctly.")
    except Exception as e:
        return _unverified(f"could not assemble the ticket context ({e})", "ORDER")
    return _review(pdf_path, _SYSTEM_ORDER, _schema(True), user, "ORDER", model, effort)


# Kept so existing callers and test_qc_llm_verdict.py keep working.
review = review_select


# ---------------------------------------------------------------------------
# Auto-fix
# ---------------------------------------------------------------------------

def _validate_fix(field: str, value: str, ticket_fields: dict) -> tuple:
    """(jira_field_id, jira_value, None) if writable, else (None, None, reason).

    Same job as LLM_writes._validate_and_fix, in the opposite direction: that one blanks a
    value Jira would reject before a create, this one refuses to send it at all. A select
    option Jira cannot resolve is dropped server-side WITHOUT failing the request, so an
    unchecked write here looks like it worked and silently changed nothing.
    """
    if field not in _FIXABLE:
        return None, None, f"{field!r} is not an auto-fixable field (reported only)"
    value = (value or "").strip()
    if not value:
        return None, None, "no replacement value proposed"

    fid, kind = _FIXABLE[field]

    if kind == "option":
        options = _OPTION_MAPS[field]
        if value not in options:
            return None, None, (f"{value!r} is not a Jira option for {field} "
                                f"({'/'.join(options)}) — it would be dropped silently")
        return fid, {"id": options[value]}, None

    if kind == "manager":
        from client_lookup import _MANAGER_TO_FILE
        if value.upper() not in frozenset(_MANAGER_TO_FILE):
            return None, None, (f"{value!r} is not one of the 14 known list managers — "
                                f"a wrong value here is what mis-set C69 on DSLF-130")
        return fid, value.upper(), None

    if kind == "date":
        if not _DATE_RE.match(value):
            return None, None, f"{value!r} is not YYYY-MM-DD — Jira would reject it"
        return fid, value, None

    if kind == "number":
        try:
            n = int(str(value).replace(",", "").strip())
        except ValueError:
            return None, None, f"{value!r} is not an integer quantity"
        if n <= 0:
            return None, None, f"quantity {n} is not plausible"
        return fid, n, None

    # Seed Tracking Number is forced equal to the Manager Order # by house rule. Letting
    # the model write anything else here would break the rule it was asked to enforce.
    if field == "seed_tracking":
        mgr = (ticket_fields.get("manager_order") or "").strip()
        if mgr and value != mgr:
            return None, None, (f"seed tracking must equal the Manager Order # ({mgr!r}), "
                                f"not {value!r}")
    return fid, value, None


def apply_fixes(ticket_key: str, findings: list, ticket_fields: dict,
                dry_run: bool = False) -> dict:
    """Write the order check's field corrections back to the ticket.

    One PUT for every accepted fix at once. Returns {"applied": [...], "refused": [...]}.
    Never raises — a refused fix is reported in the comment, not swallowed.
    """
    applied, refused, payload = [], [], {}

    for f in findings:
        field = str(f.get("fix_field") or "").strip()
        value = f.get("fix_value") or ""
        if not field:
            continue
        if str(f.get("severity", "")).upper() == "NOTE":
            refused.append(f"{field}: NOTE-level finding, not auto-applied")
            continue
        fid, jval, reason = _validate_fix(field, value, ticket_fields)
        if reason:
            refused.append(f"{field}: {reason}")
            continue
        if fid in payload:
            refused.append(f"{field}: a second fix for the same field, ignored")
            continue
        payload[fid] = jval
        applied.append(f"{field}: {f.get('ticket_value') or '(empty)'} -> {value}")

    if not payload:
        return {"applied": [], "refused": refused, "ok": True}
    if dry_run:
        return {"applied": applied, "refused": refused, "ok": True, "dry_run": True}

    from tools_jira import update_ticket_fields
    r = update_ticket_fields(ticket_key, payload)
    if "error" in r:
        log.error("Fix write failed on %s: %s", ticket_key, r["error"])
        return {"applied": [], "refused": refused + [f"write failed: {r['error']}"],
                "ok": False}
    log.info("Applied %d fix(es) to %s", len(applied), ticket_key)
    return {"applied": applied, "refused": refused, "ok": True}


# ---------------------------------------------------------------------------
# Attachments
# ---------------------------------------------------------------------------

def find_order_attachment(attachments: list, ticket_key: str = "") -> tuple:
    """The broker's order PDF, identified by running the broker fingerprints over it.

    Filename is not reliable here — the order sits alongside SELECT reports, DUMP files and
    email-acknowledgement PDFs, and brokers name their orders anything. detect_broker() is
    the same test the pipeline used to create the ticket, so an attachment it recognises is
    the order by definition. Returns (attachment, broker_key, warnings).
    """
    import tempfile, shutil
    from tools_jira import download_attachment
    from tools_pdf import extract_pdf_text
    from parsers import detect_broker

    warnings: list = []
    pdfs = [a for a in attachments
            if str(a.get("filename", "")).lower().endswith(".pdf")
            and not re.search(r'(?<![A-Z])SELECT(?![A-Z])', a.get("filename", ""), re.I)]
    if not pdfs:
        return None, "", warnings

    # Oldest first: the order is attached at creation, everything else arrives later.
    pdfs.sort(key=lambda a: a.get("created", ""))
    tmp_dir = tempfile.mkdtemp(prefix="dslf_order_")
    try:
        for att in pdfs:
            path = os.path.join(tmp_dir, att["filename"])
            try:
                download_attachment(att["content"], path)
                text = extract_pdf_text(path)
                if text.startswith("[ERROR"):
                    continue
                match = detect_broker(text)
            except Exception as e:
                warnings.append(f"could not read {att['filename']}: {e}")
                continue
            if match:
                return att, match.broker_key, warnings
        warnings.append(
            f"{len(pdfs)} non-SELECT PDF(s) attached but none matched a broker "
            f"fingerprint — the order check did not run")
        return None, "", warnings
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

_SEV_ORDER = {"WRONG": 0, "BLOCKING-BLANK": 1, "NOTE": 2}


_SEV_SHORT = {"BLOCKING-BLANK": "BLANK"}


def _one_line(i: int, f: dict) -> list:
    """One finding as one line, plus a second only when the reason adds something.

    The old layout spent five lines per finding on labelled Ticket/Source/Expected/Why
    rows. On a five-finding ticket that is 25 lines of scaffolding for about six facts, and
    a reader scrolls instead of reading. The values carry the meaning, so they go inline:
        3. WRONG  Mailer PO: 23063 -> E23063
           order reads "PO# E23063", leading E dropped
    """
    def short(v, n=64):
        """Bound the line regardless of what the model put in the field."""
        v = " ".join(str(v or "").split())
        return v if len(v) <= n else v[:n - 1].rstrip() + "…"

    sev   = str(f.get("severity", "")).upper()
    label = _SEV_SHORT.get(sev, sev)
    field = f.get("field") or "?"

    def build(n, with_source):
        have = short(f.get("ticket_value"), n)
        src  = short(f.get("select_value"), n)
        want = short(f.get("expected"), n) or src
        parts = []
        if have:
            parts.append(have)
        if want and want != have:
            parts.append(f"-> {want}")
        # The source quote only earns space when it is not just where `want` came from,
        # and when it is an actual value rather than a bare label off the page ("Key Code:").
        if (with_source and src and src not in (have, want) and want not in src
                and not src.endswith(":")):
            parts.append(f"(source: {src})")
        return f"{field}: {' '.join(parts)}" if parts else field

    # One finding, one readable line. A per-field cap is not enough on its own: two long
    # values plus a source quote still add up past any sane width, and a wrapped line in a
    # Jira comment is what made the old format unreadable. Budget the whole line instead,
    # dropping the least load-bearing part first.
    head = build(64, True)
    if len(head) > 110:
        head = build(64, False)
    if len(head) > 110:
        head = build(40, False)

    out = [f"{i}. {label:<6} {head}"]
    # The reason is worth a line only when it says more than the values already did.
    why = short(f.get("issue"), 150)
    if why and why.lower() not in head.lower():
        out.append(f"   {why}")
    return out


def _format_one(result: dict, title: str) -> list:
    """One check's section of the comment: a heading line, then one line per finding."""
    verdict = result.get("verdict", UNVERIFIED)

    if verdict == UNVERIFIED:
        return [f"{title}: UNVERIFIED — did not run: "
                f"{result.get('unverified_reason', 'unknown')}"]

    findings = sorted(result.get("findings") or [],
                      key=lambda f: _SEV_ORDER.get(str(f.get("severity", "")).upper(), 3))
    if not findings:
        return [f"{title}: PASS — nothing wrong found."]

    n = len(findings)
    head = f"{title}: {verdict} — {n} finding" + ("" if n == 1 else "s")
    blocking = result.get("blocking_count", 0)
    if blocking:
        head += f", {blocking} blocking"
    if result.get("verdict_forced"):
        head += " (verdict forced by the gate)"

    lines = [head]
    for i, f in enumerate(findings, 1):
        lines += _one_line(i, f)
    return lines


def format_report(ticket_key: str, result: dict) -> str:
    """The QC comment. Posted whatever the verdict — a clean ticket says so explicitly."""
    lines = [f"QC CHECK RESULTS — {ticket_key}",
             f"VERDICT: {result.get('verdict', UNVERIFIED)}"]

    order  = result.get("order")
    select = result.get("select")

    if order:
        lines += [""] + _format_one(order, "ORDER (ticket vs the broker order)")
    if select:
        lines += [""] + _format_one(select, "SELECT (ticket vs the pull)")
    if not order and not select:
        lines += ["", "Nothing to check: no broker order PDF and no SELECT PDF attached.",
                  "This is NOT a pass."]

    fixes = result.get("fixes") or {}
    if fixes.get("applied") or fixes.get("refused"):
        lines.append("")
        verb = "WOULD FIX" if fixes.get("dry_run") else "FIXED"
        for a in fixes.get("applied", []):
            lines.append(f"{verb}  {a}")
        for r in fixes.get("refused", []):
            lines.append(f"SKIPPED  {r}")

    if result.get("verdict") == PASS:
        lines += ["", "Checked and correct — no action needed."]
    elif result.get("verdict") == UNVERIFIED:
        lines += ["", "QC did not run — NOT a pass. This ticket still needs a human."]

    models = {c.get("model") for c in (order, select) if c and c.get("model")}
    secs   = sum(c.get("elapsed_s", 0) for c in (order, select) if c)
    if models:
        lines.append(f"\n{', '.join(sorted(models))} · {secs:.0f}s")
    return "\n".join(lines).rstrip()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def check_ticket(ticket_key: str, post: bool = False, fix: bool = False,
                 do_order: bool = True, do_select: bool = True,
                 model: str = None, effort: str = None, dry_run: bool = False) -> dict:
    """Run both checks on one ticket, optionally fix and comment. Never raises."""
    import tempfile, shutil
    from tools_jira import (get_ticket_qc_fields, download_attachment,
                            add_comment_to_ticket)
    from select_pdf import find_select_attachment

    out: dict = {"ticket_key": ticket_key}

    fields = get_ticket_qc_fields(ticket_key)
    if "error" in fields:
        # A failed Jira read is not an empty queue and not a clean ticket.
        out.update(_unverified(f"Jira read failed: {fields['error']}"))
        out["report"] = format_report(ticket_key, out)
        return out

    attachments = fields.get("attachments") or []
    tmp_dir = tempfile.mkdtemp(prefix="dslf_qc_")
    try:
        if do_order:
            order_att, broker, warns = find_order_attachment(attachments, ticket_key)
            out["order_warnings"] = warns
            if order_att:
                out["order_filename"] = order_att["filename"]
                out["broker"] = broker
                path = os.path.join(tmp_dir, order_att["filename"])
                try:
                    download_attachment(order_att["content"], path)
                    out["order"] = review_order(path, fields, model=model, effort=effort)
                except Exception as e:
                    out["order"] = _unverified(f"order PDF download failed: {e}", "ORDER")
            elif warns:
                out["order"] = _unverified("; ".join(warns), "ORDER")

        if do_select:
            select_att, warns = find_select_attachment(attachments)
            out["select_warnings"] = warns
            if select_att:
                out["select_filename"] = select_att["filename"]
                path = os.path.join(tmp_dir, select_att["filename"])
                try:
                    download_attachment(select_att["content"], path)
                    out["select"] = review_select(path, fields, model=model, effort=effort)
                except Exception as e:
                    out["select"] = _unverified(f"SELECT PDF download failed: {e}", "SELECT")

        out["verdict"] = _worst(*(c.get("verdict") for c in
                                  (out.get("order"), out.get("select")) if c))
        if not out.get("order") and not out.get("select"):
            out["verdict"] = UNVERIFIED

        # Only the ORDER check proposes writes: a bad pull needs re-running, and editing
        # the ticket to match it would erase the evidence that it was wrong.
        if fix and out.get("order"):
            out["fixes"] = apply_fixes(ticket_key, out["order"].get("findings") or [],
                                       fields, dry_run=dry_run)

        out["report"] = format_report(ticket_key, out)

        if post and not dry_run:
            cr = add_comment_to_ticket(ticket_key, out["report"], code_block=True)
            out["posted"] = "error" not in cr
            if not out["posted"]:
                log.error("Could not post QC comment to %s: %s", ticket_key, cr["error"])
        return out
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Queue scan — the re-run guard, carried over from the deleted rule checker
# ---------------------------------------------------------------------------

_QC_COMMENT_PREFIXES = ("QC CHECK RESULTS", "QC SKIPPED")
_RERUN_GRACE_SECONDS = 120  # posting the comment itself updates the ticket


def _adf_text(adf) -> str:
    """Flatten an ADF document (or plain string) to text, nested nodes included."""
    if isinstance(adf, str):
        return adf
    if not isinstance(adf, dict):
        return ""
    out = []

    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "text":
                out.append(node.get("text", ""))
            for child in node.get("content") or []:
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(adf)
    return "".join(out)


def _last_qc_comment_time(ticket_key: str) -> str | None:
    """When QC last ran on this ticket — None if it never actually ran.

    An UNVERIFIED comment deliberately does NOT count. UNVERIFIED means the check did not
    run (no key, budget spent, API error), and treating its comment as "already checked"
    would skip the ticket on every future scan: the ticket has not changed, so the guard
    below would never let it back in, and a ticket the checker never read would sit in the
    queue looking done. Anything the budget cut off must come back on the next run.
    """
    from tools_jira import get_issue_comments
    for c in get_issue_comments(ticket_key):
        body = _adf_text(c.get("body", ""))
        if not body.startswith(_QC_COMMENT_PREFIXES):
            continue
        if re.search(r'^VERDICT:\s*UNVERIFIED', body, re.MULTILINE):
            return None
        return c.get("created", "")
    return None


def _updated_after_qc(ticket_updated: str, qc_created: str) -> bool:
    """True if the ticket was meaningfully changed after the last QC comment."""
    from datetime import datetime
    for fmt, cut in (("%Y-%m-%dT%H:%M:%S.%f%z", None), ("%Y-%m-%dT%H:%M:%S", 19)):
        try:
            a = datetime.strptime(ticket_updated[:cut] if cut else ticket_updated, fmt)
            b = datetime.strptime(qc_created[:cut] if cut else qc_created, fmt)
            return (a - b).total_seconds() > _RERUN_GRACE_SECONDS
        except Exception:
            continue
    log.warning("Could not parse timestamps (updated=%r, qc=%r) — treating as unchanged",
                ticket_updated, qc_created)
    return False


def scan(status: str, **kw) -> list:
    """Every ticket in `status`, skipping ones unchanged since their last QC comment."""
    from tools_jira import search_issues_paged

    jql = f'project = DSLF AND status = "{status}" ORDER BY created ASC'
    log.info("Scanning: %s", jql)
    issues = search_issues_paged(jql, "summary,status,updated")
    log.info("Found %d ticket(s) in %r", len(issues), status)

    results = []
    for issue in issues:
        key  = issue["key"]
        last = _last_qc_comment_time(key)
        if last and not _updated_after_qc(issue["fields"].get("updated", ""), last):
            log.info("%s: no changes since last QC — skipping", key)
            continue
        results.append(check_ticket(key, **kw))
    return results


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env")

    import config_guard
    config_guard.validate_configs_or_exit()

    ap = argparse.ArgumentParser(
        description="LLM QC for DSLF tickets: was it created right, and did the pull "
                    "deliver it?")
    ap.add_argument("tickets", nargs="*",
                    help=f"ticket keys; default is every ticket in {NEED_QC_STATUS}")
    ap.add_argument("--status", default=NEED_QC_STATUS,
                    help=f"queue to scan when no keys are given (default {NEED_QC_STATUS!r})")
    ap.add_argument("--post", action="store_true",
                    help="post the verdict as a Jira comment (default: print only)")
    ap.add_argument("--fix", action="store_true",
                    help="apply the ORDER check's field corrections to the live ticket")
    ap.add_argument("--dry-run", action="store_true",
                    help="never write: no comment, no field fix, report what would happen")
    ap.add_argument("--order-only", action="store_true", help="skip the SELECT check")
    ap.add_argument("--select-only", action="store_true", help="skip the ORDER check")
    ap.add_argument("--model", default=QC_MODEL)
    ap.add_argument("--effort", default=QC_EFFORT,
                    choices=("low", "medium", "high", "xhigh", "max"))
    ap.add_argument("--json", metavar="FILE", help="write all results to FILE as JSON")
    args = ap.parse_args()

    kw = dict(post=args.post, fix=args.fix, dry_run=args.dry_run,
              do_order=not args.select_only, do_select=not args.order_only,
              model=args.model, effort=args.effort)

    if args.tickets:
        results = [check_ticket(k, **kw) for k in args.tickets]
    else:
        results = scan(args.status, **kw)
        if not results:
            print("Nothing to check.")
            return 0

    for r in results:
        print("\n" + "=" * 78)
        print(r.get("report") or format_report(r.get("ticket_key", "?"), r))

    tally: dict = {}
    for r in results:
        v = r.get("verdict", UNVERIFIED)
        tally[v] = tally.get(v, 0) + 1
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
