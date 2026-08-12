"""
Pure-LLM DSLF ticket creation: Claude extracts every field from the order PDF, and the
result is handed to the same post-parse pipeline the rule-based path uses.

This exists for orders the rule-based path cannot handle at all. A PDF matching none of
the 12 fingerprints in parsers/_RULES is flagged for review and produces no ticket, and
hybrid_create.py cannot help either — it starts from process_pdf() and raises when the
rule-based parse fails. This path only needs the PDF to be readable by Claude.

    python LLM_writes.py order.pdf                 # dry run — print fields, create nothing
    python LLM_writes.py order.pdf --live          # create the ticket
    python LLM_writes.py /path/to/folder/          # every *.pdf in the folder (dry run)
    python LLM_writes.py order.pdf --json out.json # dump the built fields
    python LLM_writes.py order.pdf --model claude-opus-5 --effort high

HOW IT STAYS ACCURATE
Claude only supplies what is actually printed on the order. Everything else comes from the
same code the rule-based path runs, via parse_pipeline.finalize_and_create():

  * db_code / Billable / Client DB / Seed DB from client_lookup, including the AMLC
    rental-vs-exchange branch and the ADSTRA list-code tier. Claude's own db_code guess is
    reported but never sent — config is authoritative (a config billable_account can
    legitimately differ from the db_code prefix, e.g. A52D -> A68).
  * The client profile's Select By / Standard Suppressions / Special Instructions blocks
    in the Description, and its FLAG OMITS: line in the Omission Description.
  * The tools_polish structural clean of both prose fields.
  * Duplicate check, SKIP_DB_CODES, the IBM i work order, and all four attachment steps
    (source PDF, supplementary zip-omit files, their 9500-row splits, client profile).

Multi-page PDFs split into one ticket per page, except ADSTRA, matching process_pdf().

Prefer parse_pipeline.py for any broker that IS recognized — a parser reads a known layout
exactly, where this path is reading it for the first time every run.
"""

import re
import sys
import copy
import json
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

_SCRIPT_DIR = Path(__file__).parent
load_dotenv(_SCRIPT_DIR / ".env")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("LLM_writes")

import ai_extract
from parse_result import ParseResult
from parse_pipeline import finalize_and_create
from client_lookup import _MANAGER_TO_FILE
from tools_jira import (
    get_ticket_qc_fields,
    AVAILABILITY_RULE_OPTIONS,
    FILE_FORMAT_OPTIONS,
    SHIPPING_METHOD_OPTIONS,
)
from tools_pdf import extract_pdf_text, get_pdf_page_count, split_pdf_into_pages
from compare_extraction import adf_to_lines

# Sonnet at medium effort: this extraction is structured transcription against a fixed
# schema, not open-ended reasoning, so the deeper tiers buy little. ai_extract's own
# defaults (Opus at high effort) are deliberately left alone — compare_extraction.py and
# hybrid_create.py still use them. Override per run with --model / --effort.
DEFAULT_MODEL  = "claude-sonnet-5"
DEFAULT_EFFORT = "medium"
_EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")

# Only its being non-zero matters: validate_result() blocks creation at exactly 0.0, and
# rule-based parsers report 0.92. A lower number records that nothing verified this read.
CONFIDENCE_LLM_BASED = 0.85

_DB_CODE_RE = re.compile(r"^([A-Z]\d{2})[A-Z]?$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# The 14 exact List Manager values, taken from the lookup that already keys on them rather
# than re-typed here — a value outside this set skips the broker-sheet lookup entirely,
# which is what produced the wrong C69 database codes on DSLF-130..134.
_VALID_LIST_MANAGERS = frozenset(_MANAGER_TO_FILE)

# ADSTRA's 5-digit list code is enrich_fields' most reliable lookup (tier 0), so it is
# worth asking for. Extend a copy — DSLF_SCHEMA is shared with compare_extraction.py.
_SCHEMA = copy.deepcopy(ai_extract.DSLF_SCHEMA)
_SCHEMA["properties"]["adstra_list_code"] = {"type": "string"}
_SCHEMA["required"] = list(_SCHEMA["required"]) + ["adstra_list_code"]

# Layered on top of ai_extract._SYSTEM rather than replacing it. Everything here is
# transcribed from CLAUDE.md and the parsers — per-broker field sources the base prompt
# does not mention, and the layout traps that have caused real mis-parses.
_SYSTEM = ai_extract._SYSTEM + """

════════════════════════════════════════════════════════════════════════
BROKER-SPECIFIC RULES — the order's own layout decides which apply.
════════════════════════════════════════════════════════════════════════

MAILER PO vs MANAGER ORDER NUMBER — these are different fields on every order.
  ADSTRA            mailer_po = 6-digit or BRK-prefixed | manager_order = J- or I-prefix
  RMI               mailer_po = "Broker PO#" field      | manager_order = MGT#
  WE ARE MOORE      mailer_po = Ship Label number       | manager_order = Order#
  DATA-AXLE         mailer_po = Ship Label "PO:" with suffix (58364-RN) | manager_order = Order# (2316747)
  WASHINGTON LISTS  mailer_po = Client Reference w/ suffix | manager_order = Order Number
  KAP               mailer_po = Broker order # value    | manager_order = KAP ORDER, DL-prefix
  CONRAD            mailer_po = "BROK/MAIL PO:" field   | manager_order = PURCHASE ORDER NO
  NAMES IN THE NEWS mailer_po = 6-7 digit number        | manager_order = LR #
  CELCO             both come from ORDER #
  SimioCloud        mailer_po = Ship Label "PO#", else the first 4+ digit run in the label
  RKD / AMLC        mailer_po = "Client P.O.:"          | manager_order = first 5-6 digit
                    number in the first 10 lines (Service Bureau No. / Purchase Order No.)

AMLC LAYOUT TRAP: AMLC orders are columnar. The value for "Client P.O.:" can sit up to 25
lines BELOW its own label, with unrelated text in between. Do not pair a label with the
text that happens to follow it on the same visual row — track the column.

LIST MANAGER TRAPS:
  * A SimioCloud order's list_manager is WE ARE MOORE (SimioCloud is their ordering
    platform). It is never DATA-AXLE, even though the layout resembles Data Axle's.
  * An AMLC order serviced by RKD contains the words "RKD GROUP". If it says "American
    Mailing Lists Corporation Management", it is AMLC, not RKD.

REQUESTOR is the LIST MANAGER's own contact, never the broker's rep. On an ADSTRA form
that means the "Contact:" name together with the @adstradata.com address — a person named
under "Broker:" placed the order and does not belong on the ticket at all. Use the table
below only when the order names no contact for the list manager.

REQUESTOR — when the order is from one of these brokers and names no other requestor:
  ADSTRA        BOBBI DURRETT     BOBBI.DURRETT@ADSTRADATA.COM
  RMI           ALICIA GALLAGHER  AGALLAGHER@RMIDIRECT.COM
  WE ARE MOORE  MICHELLE NAY      MNAY@WEAREMOORE.COM
  KAP           Jenny Gomez       jgomez@keyacquisition.com
  CONRAD        Brenda Gundlah    bgundlah@conraddirect.com

SHIP TO EMAIL — KAP orders print two addresses. Use the "Email to:" line inside the Ship
To block. The first "Email:" on the page is the mailer's own contact, not the destination.

KEY CODE:
  * Conrad: the text after "And" or "&" on the MATERIAL line, e.g.
    "...PO# L50278HF & HF Thirteen Star Flag #2215A" -> "HF Thirteen Star Flag #2215A".
    Not always present; leave "" when absent.
  * Data Axle: the "Key Code:" field, or the suffix on the Order#.

LIST NAME is the abbreviation as printed (FAIR, JW, WWP), not the expanded name.

ADSTRA LIST CODE: if the order shows a 5-digit ADSTRA list code, return it in
adstra_list_code. Otherwise "". This is the most reliable database lookup key there is.

FINDING THE SELECT CRITERIA — this is the most-missed field, so read carefully. The priced
select is the segment description printed alongside the list name and the price: a recency
window, a dollar band, "HOTLINE", "MULTI", an Nth, a gender or state select. On KAP and
several other forms it sits in the List / Price block, e.g.

    List:                 AID FOR STARVING CHILDREN
    Price:                12 MONTH (8/25-7/26) $10-$49.99

On an ADSTRA form the select is the "Pull Description:" value, e.g. "3MOS $10+ DONORS".

Take the select line VERBATIM. Never append words lifted from an omit line to it: a line
like "ADDR. USA NAMES ONLY, OMIT CAN., P.R., FOREIGN, APO, FPO & MIL." is one omit
criterion and belongs whole in omission_description — do not split its first clause off
into the description.

and the form's own literal "Selects:" label is a SEPARATE, OFTEN EMPTY field. An empty
"Selects:" label does not mean the order has no selects — look at the List / Price block
before concluding that. Never emit a "Selects:" heading with nothing under it: if you truly
find no priced select criteria anywhere, return an empty description array instead.

DESCRIPTION FORMATTING — the priced selects are rendered as a bulleted list downstream,
and that rendering keys off indentation. When the order lists priced select criteria,
emit a "Selects:" line followed by each criterion indented by two spaces:
    Selects:
      $10+
      12 MOS HOTLINE
Unindented lines render as bare fragments with nothing saying what they are.

SHIPPING INSTRUCTIONS — the cc list and nothing else:
    CC: CRAGUSA@ESTEEMARKETING.COM, DSNYDER@ESTEEMARKETING.COM
Do not repeat the destination address there — it is already the ship_to_email field — and
do not carry subject-line rules, file-naming rules, or quantity-approval notes. When the
order names no cc addresses, return "".

DESCRIPTION SCOPE — the description is the SELECT criteria and nothing else. Do not add
order terms, exchange/rental status, pricing, net arrangement, category, delivery notes,
or quantity-approval notes. Anything that is an omit or a suppression goes in
omission_description, never here.

"Selects:" is the ONLY heading that takes an indented block. Every other label you emit
must be a single self-contained line — a downstream cleanup step flattens indentation
under any other heading, which would leave a bare label with loose lines under it.

DO NOT return the client's standard suppressions, "Select By" line, or standing flag
omits even if the order restates them — those come from the client profile on file and
are added downstream. Return only what THIS order says.
"""


def _build_summary(fields: dict) -> str:
    """Title is LIST NAME - MAILER NAME - MANAGER ORDER NUMBER (never the Mailer PO)."""
    parts = [str(fields.get(k) or "").strip()
             for k in ("list_name", "mailer_name", "manager_order_number")]
    return " - ".join(p for p in parts if p)


def _validate_and_fix(f: dict) -> list:
    """Blank any extracted value Jira would reject or store wrong. Returns warning lines.

    Runs before the ParseResult is built, because that dataclass is frozen. The tail's
    own validate_result() is the advisory second pass.
    """
    warnings = []

    lm = (f.get("list_manager") or "").strip().upper()
    if lm and lm not in _VALID_LIST_MANAGERS:
        warnings.append(f"list_manager {f['list_manager']!r} is not one of the 14 known values "
                        f"— blanked (a wrong value here is what mis-set C69 on DSLF-130)")
        f["list_manager"] = ""
    else:
        f["list_manager"] = lm

    for field, options in (("availability_rule", AVAILABILITY_RULE_OPTIONS),
                           ("file_format", FILE_FORMAT_OPTIONS),
                           ("shipping_method", SHIPPING_METHOD_OPTIONS)):
        value = (f.get(field) or "").strip()
        if value and value not in options:
            warnings.append(f"{field} {value!r} has no Jira option "
                            f"({'/'.join(options)}) — blanked, it would be dropped anyway")
            f[field] = ""

    for field, label in (("mail_date", "mail_date"), ("due_date", "ship_by_date")):
        value = (f.get(field) or "").strip()
        if value and not _DATE_RE.match(value):
            warnings.append(f"{label} {value!r} is not YYYY-MM-DD — blanked "
                            f"(Jira rejects the whole create on a bad date)")
            f[field] = ""

    return warnings


def build_result(pdf_path: str, model: str = DEFAULT_MODEL,
                 effort: str = DEFAULT_EFFORT) -> tuple:
    """Claude-extract the PDF and return (ParseResult, meta).

    db_code and billable_account are deliberately left empty on the ParseResult: the tail
    resolves both from config, which is authoritative. Claude's guess rides in meta so it
    can be reported without being sent.
    """
    extracted = ai_extract.extract_fields_from_pdf(
        pdf_path, model=model, effort=effort, system=_SYSTEM, schema=_SCHEMA)
    f = extracted["fields"]
    warnings = _validate_and_fix(f)

    result = ParseResult(
        source=f"llm:{model}",
        confidence=CONFIDENCE_LLM_BASED,
        summary=_build_summary(f),
        mailer_name=f.get("mailer_name", ""),
        mailer_po=f.get("mailer_po", ""),
        list_name=f.get("list_name", ""),
        list_manager=f.get("list_manager", ""),
        requested_quantity=int(f.get("requested_quantity") or 0),
        manager_order_number=f.get("manager_order_number", ""),
        mail_date=f.get("mail_date", ""),
        # DSLF_SCHEMA calls the Ship By date due_date; ParseResult calls it ship_by_date.
        ship_by_date=f.get("due_date", ""),
        requestor_name=f.get("requestor_name", ""),
        requestor_email=f.get("requestor_email", ""),
        ship_to_email=f.get("ship_to_email", ""),
        key_code=f.get("key_code", ""),
        availability_rule=f.get("availability_rule", ""),
        file_format=f.get("file_format", ""),
        shipping_method=f.get("shipping_method", ""),
        shipping_instructions=f.get("shipping_instructions", ""),
        # Plain text, not ADF: the tail polishes it and then builds the ADF with the
        # client profile's blocks wrapped around it.
        segment_criteria="\n".join(f.get("description") or []),
        omission_description="\n".join(f.get("omission_description") or []),
        other_fees=f.get("other_fees", ""),
        special_seed_instructions="\n".join(f.get("special_seed_instructions") or []),
        adstra_list_code=f.get("adstra_list_code", ""),
        warnings=tuple(warnings),
    )
    meta = {
        "usage": extracted.get("usage", {}),
        "model": extracted.get("model", model),
        "effort": effort,
        "warnings": warnings,
        "claude_db_code": (f.get("db_code") or "").strip().upper(),
    }
    return result, meta


def _report_extraction(result: ParseResult, meta: dict) -> None:
    """Short pre-flight. The tail's own _print_result dumps the final field set."""
    print(f"\n== Claude extraction ({meta['model']} @ {meta['effort']} effort) ==")
    if meta["claude_db_code"]:
        shape = "" if _DB_CODE_RE.match(meta["claude_db_code"]) else " (unexpected shape)"
        print(f"  db_code guess: {meta['claude_db_code']}{shape} "
              f"— not sent; config decides")
    for w in meta["warnings"]:
        print(f"  ! {w}")
    blanks = [k for k in ("list_manager", "mailer_po", "manager_order_number",
                          "requested_quantity") if not getattr(result, k)]
    if blanks:
        print(f"  ! blank: {', '.join(blanks)} — nothing verified this read, "
              f"check against the PDF before creating.")
    usage = meta.get("usage", {})
    print(f"  ({usage.get('input_tokens')} in / {usage.get('output_tokens')} out tokens)")


def _verify_created(ticket_key: str, result: ParseResult) -> list:
    """Re-read the ticket and report what actually landed. Never raises.

    A create writes no changelog entries — the changelog only records later changes — so
    verification has to be a field read. Select options that could not be resolved are
    dropped server-side without failing the create, which is exactly what this catches.
    """
    stored = get_ticket_qc_fields(ticket_key)
    if stored.get("error"):
        return [f"verify skipped: {stored['error']}"]

    expected = {
        "summary":           result.summary,
        "list_name":         result.list_name,
        "mailer_name":       result.mailer_name,
        "manager_order":     result.manager_order_number,
        "list_manager":      result.list_manager,
        "requested_qty":     int(result.requested_quantity or 0),
        "availability_rule": result.availability_rule,
        # a blank file_format defaults to ASCII Delimited inside create_jira_ticket
        "file_format":       result.file_format or "ASCII Delimited",
        "shipping_method":   result.shipping_method,
        "ship_to_email":     result.ship_to_email,
    }
    # create_jira_ticket rewrites these from the ship-to house rules (Saturn, data-axle,
    # the fixed-format email houses), so a difference here is expected, not a mismatch.
    house = {"file_format", "shipping_method", "ship_to_email"}

    lines = []
    for field, want in expected.items():
        got = stored.get(field, "")
        if field == "requested_qty":
            got = int(got or 0)
        if str(got).strip() == str(want).strip():
            continue
        if field in house:
            lines.append(f"  ~ {field}: sent {want!r}, stored {got!r} "
                         f"(ship-to house rules may override this)")
        elif want and not got:
            lines.append(f"  ! {field}: sent {want!r} but the ticket is EMPTY "
                         f"— the Jira option probably does not exist")
        else:
            lines.append(f"  ! {field}: sent {want!r}, stored {got!r}")

    lines.append(f"  Client DB: {stored.get('client_db') or '(empty)'}   "
                 f"Seed DB: {stored.get('seed_db') or '(empty)'}")
    lines.append(f"  description: {len(adf_to_lines(stored.get('description_adf')))} line(s), "
                 f"omission: {len(adf_to_lines(stored.get('omission_adf')))} line(s)")
    return lines


def llm_create(pdf_path: str, model: str = DEFAULT_MODEL, effort: str = DEFAULT_EFFORT,
               live: bool = False) -> dict:
    """Extract one PDF and run it through the shared pipeline tail.

    Returns the tail's result dict, or a list of them for a split multi-page order.
    """
    print(f"\n--- {Path(pdf_path).name} ---")
    result, meta = build_result(pdf_path, model=model, effort=effort)
    _report_extraction(result, meta)

    # Multi-page: one ticket per page, except ADSTRA, matching process_pdf(). The list
    # manager is only known after extraction, so the whole-document read above decides.
    page_count = get_pdf_page_count(pdf_path)
    if page_count > 1 and result.list_manager != "ADSTRA":
        log.info("Multi-page PDF (%d pages) — one ticket per page", page_count)
        tmp_dir, page_paths = split_pdf_into_pages(pdf_path)
        results = []
        try:
            for i, page_path in enumerate(page_paths):
                log.info("--- Page %d/%d ---", i + 1, page_count)
                results.append(llm_create(page_path, model=model, effort=effort, live=live))
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return results
    if page_count > 1:
        log.info("ADSTRA multi-page PDF (%d pages) — one order", page_count)

    if not result.summary:
        raise RuntimeError("Claude returned no list name, mailer name or manager order "
                           "number — refusing to create an untitled ticket")

    out = finalize_and_create(result, pdf_path, extract_pdf_text(pdf_path),
                              dry_run=not live, verbose=True,
                              profile_blocks_to_omission=True)
    out["pdf"] = pdf_path
    out["extraction"] = meta

    if out.get("dry_run"):
        print("\n[DRY RUN] nothing created. Re-run with --live to create the ticket.")
    elif out.get("ticket_key"):
        print("\nVerifying what landed on the ticket:")
        out["verification"] = _verify_created(out["ticket_key"], result)
        for line in out["verification"]:
            print(line)
    return out


def _iter_pdfs(path: str) -> list:
    """One PDF, or every *.pdf/*.PDF directly inside a folder (non-recursive)."""
    p = Path(path)
    if p.is_file():
        return [p]
    if p.is_dir():
        # Windows globbing is case-insensitive, so de-duplicate by resolved path.
        seen, out = set(), []
        for pattern in ("*.pdf", "*.PDF"):
            for f in sorted(p.glob(pattern)):
                key = str(f.resolve()).lower()
                if key not in seen:
                    seen.add(key)
                    out.append(f)
        return out
    return []


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser(
        description="Create a DSLF ticket from a PDF using Claude for all field extraction.")
    ap.add_argument("path", help="order PDF, or a folder of them")
    ap.add_argument("--model", default=DEFAULT_MODEL,
                    help=f"Claude model to extract with (default {DEFAULT_MODEL})")
    ap.add_argument("--effort", default=DEFAULT_EFFORT, choices=_EFFORT_LEVELS,
                    help=f"reasoning effort (default {DEFAULT_EFFORT})")
    ap.add_argument("--live", action="store_true",
                    help="actually create the ticket (default is a dry run)")
    ap.add_argument("--json", metavar="FILE",
                    help="write the built fields and results to FILE as JSON")
    args = ap.parse_args()

    pdfs = _iter_pdfs(args.path)
    if not pdfs:
        log.error("No PDF found at: %s", args.path)
        return 1

    results, failed = [], 0
    for pdf in pdfs:
        try:
            results.append(llm_create(str(pdf), model=args.model, effort=args.effort,
                                      live=args.live))
        except Exception as e:
            log.error("%s: %s", pdf.name, e)
            results.append({"pdf": str(pdf), "error": str(e)})
            failed += 1

    flat = [r for item in results for r in (item if isinstance(item, list) else [item])]
    if len(flat) > 1:
        created = sum(1 for r in flat if r.get("ticket_key"))
        print(f"\n== {len(flat)} order(s): {created} created, {failed} failed ==")

    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
        print(f"Wrote {args.json}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
