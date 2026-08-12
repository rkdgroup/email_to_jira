"""
Pure-LLM DSLF ticket creation: Claude extracts every field from the order PDF and
the result is written straight to Jira. No broker parser is involved.

This exists for orders the rule-based path cannot handle at all. A PDF matching none
of the 12 fingerprints in parsers/_RULES is flagged for review and produces no ticket,
and hybrid_create.py cannot help either — it starts from process_pdf() and raises when
the rule-based parse fails. This path only needs the PDF to be readable by Claude.

    python LLM_writes.py order.pdf                 # dry run — print fields, create nothing
    python LLM_writes.py order.pdf --live          # create the ticket
    python LLM_writes.py /path/to/folder/          # every *.pdf in the folder (dry run)
    python LLM_writes.py order.pdf --enrich        # fill a blank db_code from config/*.yaml
    python LLM_writes.py order.pdf --json out.json # dump the built kwargs
    python LLM_writes.py order.pdf --live --no-attach
    python LLM_writes.py order.pdf --model claude-opus-4-8

Nothing validates a Claude extraction the way the parsers do, so the values are checked
against the pipeline's own vocabularies before anything is sent: the List Manager must be
one of the 14 known values, the three select fields must map to a real Jira option, and
dates must be YYYY-MM-DD. A value that fails is blanked and reported rather than silently
written wrong or rejected by Jira as an opaque HTTP 400.

WHAT THIS PATH GIVES UP (by design — it is the cost of skipping the parsers):
  * No client_lookup enrichment unless --enrich is passed, so db_code is Claude's best
    guess and is often "". Billable Account / Client Database / Seed Database follow it.
  * No client-profile injection — the profile's Select By, Standard Suppressions,
    Special Instructions and FLAG OMITS: blocks are NOT added to the two prose fields.
  * No per-broker requestor table; the requestor is whatever the PDF states.
  * No IBM i work order is created or linked (that step needs a billable account).
  * No multi-page split. process_pdf() makes one ticket per page for every broker except
    ADSTRA; here Claude reads the whole document as ONE order, so a 7-page AMLC PDF
    collapses into a single ticket.

Prefer parse_pipeline.py for any broker that IS recognized — it is more accurate.
"""

import re
import sys
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
from parse_pipeline import SKIP_DB_CODES, _build_adf_description
from client_lookup import enrich_fields, _MANAGER_TO_FILE
from tools_jira import (
    create_jira_ticket,
    attach_file_to_ticket,
    search_jira_tickets,
    get_ticket_qc_fields,
    _get_jira_base_url,
    AVAILABILITY_RULE_OPTIONS,
    FILE_FORMAT_OPTIONS,
    SHIPPING_METHOD_OPTIONS,
)
from tools_pdf import extract_pdf_text
from compare_extraction import adf_to_lines

# Database codes are one letter + two digits, optionally with a suffix letter (F41D).
_DB_CODE_RE = re.compile(r"^([A-Z]\d{2})[A-Z]?$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# The 14 exact List Manager values, taken from the lookup that already keys on them
# rather than re-typed here — a value outside this set skips the broker-sheet lookup
# entirely, which is what produced the wrong C69 database codes on DSLF-130..134.
_VALID_LIST_MANAGERS = frozenset(_MANAGER_TO_FILE)

# Blank values here are worth seeing before a live create — none of them are fatal,
# but each one is something the rule-based path would normally have filled in.
_WATCH_FIELDS = ("db_code", "list_manager", "mailer_po", "manager_order_number",
                 "requested_quantity")

# create_jira_ticket rewrites these from the ship-to house rules (Saturn, data-axle,
# the fixed-format email houses), so the stored value legitimately differs from what
# was sent. Post-create verification reports them without calling them a mismatch.
_HOUSE_RULE_FIELDS = {"file_format", "shipping_method", "ship_to_email",
                      "shipping_instructions"}


def _build_summary(fields: dict) -> str:
    """Title is LIST NAME - MAILER NAME - MANAGER ORDER NUMBER (never the Mailer PO)."""
    parts = [str(fields.get(k) or "").strip()
             for k in ("list_name", "mailer_name", "manager_order_number")]
    return " - ".join(p for p in parts if p)


def _billable_from_db_code(db_code: str) -> str:
    """F41D -> F41. Returns "" for a blank or unrecognized code rather than guessing."""
    code = (db_code or "").strip().upper()
    if not code:
        return ""
    m = _DB_CODE_RE.match(code)
    if not m:
        return ""
    return m.group(1)


def _validate_and_fix(kwargs: dict) -> list:
    """Blank any value Jira would reject or store wrong. Returns warning lines."""
    warnings = []

    lm = (kwargs.get("list_manager") or "").strip().upper()
    if lm and lm not in _VALID_LIST_MANAGERS:
        warnings.append(f"list_manager {kwargs['list_manager']!r} is not one of the 14 known "
                        f"values — blanked (a wrong value here is what mis-set C69 on DSLF-130)")
        kwargs["list_manager"] = ""
    else:
        kwargs["list_manager"] = lm

    for field, options in (("availability_rule", AVAILABILITY_RULE_OPTIONS),
                           ("file_format", FILE_FORMAT_OPTIONS),
                           ("shipping_method", SHIPPING_METHOD_OPTIONS)):
        value = (kwargs.get(field) or "").strip()
        if value and value not in options:
            warnings.append(f"{field} {value!r} has no Jira option "
                            f"({'/'.join(options)}) — blanked, it would be dropped anyway")
            kwargs[field] = ""

    for field in ("mail_date", "ship_by_date"):
        value = (kwargs.get(field) or "").strip()
        if value and not _DATE_RE.match(value):
            warnings.append(f"{field} {value!r} is not YYYY-MM-DD — blanked "
                            f"(Jira rejects the whole create on a bad date)")
            kwargs[field] = ""

    db_code = kwargs.get("db_code") or ""
    if db_code and not _DB_CODE_RE.match(db_code):
        warnings.append(f"db_code {db_code!r} is not the expected letter+2-digit shape — "
                        f"blanked, so Client/Seed/Billable are left empty")
        kwargs["db_code"] = ""
        kwargs["billable_account"] = ""

    return warnings


def _maybe_enrich(kwargs: dict) -> list:
    """--enrich: fill a blank db_code from config/*.yaml. Returns note lines."""
    if kwargs.get("db_code"):
        return []
    found = enrich_fields(
        list_name=kwargs.get("list_name", ""),
        mailer_name=kwargs.get("mailer_name", ""),
        list_manager=kwargs.get("list_manager", ""),
    )
    if not found:
        return ["--enrich: no config match on list/mailer name — db_code stays blank"]

    notes = []
    for key in ("db_code", "billable_account", "list_manager"):
        value = found.get(key)
        if value and not kwargs.get(key):
            kwargs[key] = value
            notes.append(f"--enrich: {key} = {value}")
    # billable_account is authoritative in config and can differ from the db_code prefix
    # by design (A52D -> A68), so only derive it when the lookup did not supply one.
    if kwargs.get("db_code") and not kwargs.get("billable_account"):
        kwargs["billable_account"] = _billable_from_db_code(kwargs["db_code"])
        notes.append(f"--enrich: billable_account = {kwargs['billable_account']} (derived)")
    return notes


def build_kwargs(pdf_path: str, model: str = ai_extract.DEFAULT_MODEL,
                 enrich: bool = False) -> tuple:
    """Claude-extract the PDF and map DSLF_SCHEMA onto create_jira_ticket kwargs.

    Returns (kwargs, meta) where meta carries usage, warnings and enrichment notes.
    """
    result = ai_extract.extract_fields_from_pdf(pdf_path, model=model)
    f = result["fields"]

    db_code = (f.get("db_code") or "").strip().upper()
    kwargs = {
        "summary":                   _build_summary(f),
        "list_name":                 f.get("list_name", ""),
        "mailer_name":               f.get("mailer_name", ""),
        "mailer_po":                 f.get("mailer_po", ""),
        "manager_order_number":      f.get("manager_order_number", ""),
        "list_manager":              f.get("list_manager", ""),
        "requestor_name":            f.get("requestor_name", ""),
        "requestor_email":           f.get("requestor_email", ""),
        "mail_date":                 f.get("mail_date", ""),
        # DSLF_SCHEMA calls the Ship By date due_date; create_jira_ticket calls it ship_by_date.
        "ship_by_date":              f.get("due_date", ""),
        "requested_quantity":        f.get("requested_quantity", 0) or 0,
        "availability_rule":         f.get("availability_rule", ""),
        "file_format":               f.get("file_format", ""),
        "ship_to_email":             f.get("ship_to_email", ""),
        "shipping_method":           f.get("shipping_method", ""),
        "shipping_instructions":     f.get("shipping_instructions", ""),
        "other_fees":                f.get("other_fees", ""),
        "key_code":                  f.get("key_code", ""),
        "db_code":                   db_code,
        "billable_account":          _billable_from_db_code(db_code),
        # Prose comes back as per-line arrays. Description goes through the same ADF builder
        # the rule-based path uses, so an indented run under a heading ("Selects:") becomes a
        # real bulletList — Jira's renderer collapses leading whitespace, so the indent has to
        # become structure or it is invisible. result is unused when segment_criteria is given.
        # Omission and seed instructions are accepted as plain strings and split per line.
        "description":               _build_adf_description(
                                         None, segment_criteria="\n".join(f.get("description") or [])),
        "omission_description":      "\n".join(f.get("omission_description") or []),
        "special_seed_instructions": "\n".join(f.get("special_seed_instructions") or []),
    }

    notes = _maybe_enrich(kwargs) if enrich else []
    warnings = _validate_and_fix(kwargs)

    meta = {"usage": result.get("usage", {}), "model": result.get("model", model),
            "warnings": warnings, "notes": notes}
    return kwargs, meta


def _duplicate_query(kwargs: dict) -> tuple:
    """Same keys parse_pipeline uses: AMLC on Manager Order #, everything else on Mailer PO."""
    if kwargs.get("list_manager") == "AMLC" and kwargs.get("manager_order_number"):
        return (f'project = DSLF AND cf[12192] = "{kwargs["manager_order_number"]}"',
                f'Manager Order # {kwargs["manager_order_number"]}')
    if kwargs.get("mailer_po"):
        return (f'project = DSLF AND cf[12193] = "{kwargs["mailer_po"]}"',
                f'PO {kwargs["mailer_po"]}')
    return None, None


def _report(kwargs: dict, meta: dict) -> None:
    print("\n== Claude-extracted ticket fields ==")
    print(f"Title: {kwargs['summary'] or '(EMPTY)'}")
    print(f"List Manager: {kwargs['list_manager']}   Mailer PO: {kwargs['mailer_po']}   "
          f"Mgr#: {kwargs['manager_order_number']}")
    print(f"Client DB/Billable: {kwargs['db_code'] or '(none)'} / "
          f"{kwargs['billable_account'] or '(none)'}")
    print(f"Requestor: {kwargs['requestor_name']} <{kwargs['requestor_email']}>")
    print(f"Qty: {kwargs['requested_quantity']}   Availability: {kwargs['availability_rule']}   "
          f"Format: {kwargs['file_format']}   Ship: {kwargs['shipping_method']}")

    print("Description:")
    for ln in adf_to_lines(kwargs["description"]):
        print(f"    {ln}")
    print("Omission Description:")
    for ln in kwargs["omission_description"].splitlines():
        if ln.strip():
            print(f"    {ln.strip()}")

    for note in meta.get("notes", []):
        print(f"  + {note}")
    for warning in meta.get("warnings", []):
        print(f"  ! {warning}")

    blanks = [k for k in _WATCH_FIELDS if not kwargs.get(k)]
    if blanks:
        print(f"  ! blank: {', '.join(blanks)} — nothing validated this extraction, "
              f"check against the PDF before creating.")
    usage = meta.get("usage", {})
    print(f"  ({meta.get('model')}: {usage.get('input_tokens')} in / "
          f"{usage.get('output_tokens')} out tokens)")


def _verify_created(ticket_key: str, kwargs: dict) -> list:
    """Re-read the ticket and report what actually landed. Never raises.

    A create writes no changelog entries — the changelog only records later changes — so
    verification has to be a field read. Select options that could not be resolved are
    dropped server-side without failing the create, which is exactly what this catches.
    """
    stored = get_ticket_qc_fields(ticket_key)
    if stored.get("error"):
        return [f"verify skipped: {stored['error']}"]

    expected = {
        "summary":           kwargs["summary"],
        "list_name":         kwargs["list_name"],
        "mailer_name":       kwargs["mailer_name"],
        "manager_order":     kwargs["manager_order_number"],
        "list_manager":      kwargs["list_manager"],
        "client_db":         kwargs["db_code"],
        "seed_db":           (kwargs["db_code"][:-1] + "S") if kwargs["db_code"] else "",
        "requested_qty":     int(kwargs["requested_quantity"] or 0),
        "availability_rule": kwargs["availability_rule"],
        # blank file_format defaults to ASCII Delimited inside create_jira_ticket
        "file_format":       kwargs["file_format"] or "ASCII Delimited",
        "shipping_method":   kwargs["shipping_method"],
        "ship_to_email":     kwargs["ship_to_email"],
    }
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

    desc = adf_to_lines(stored.get("description_adf"))
    omit = adf_to_lines(stored.get("omission_adf"))
    lines.append(f"  description: {len(desc)} line(s) stored, "
                 f"omission: {len(omit)} line(s) stored")
    return lines


def llm_create(pdf_path: str, model: str = ai_extract.DEFAULT_MODEL,
               live: bool = False, attach: bool = True, enrich: bool = False) -> dict:
    print(f"\n--- {Path(pdf_path).name} ---")
    kwargs, meta = build_kwargs(pdf_path, model=model, enrich=enrich)
    _report(kwargs, meta)

    if not kwargs["summary"]:
        raise RuntimeError("Claude returned no list name, mailer name or manager order number "
                           "— refusing to create an untitled ticket")

    if kwargs["db_code"] in SKIP_DB_CODES:
        print(f"\n[SKIPPED] db_code {kwargs['db_code']} is in SKIP_DB_CODES — no ticket.")
        return {"pdf": pdf_path, "skipped": True, "db_code": kwargs["db_code"], "kwargs": kwargs}

    if not live:
        print("\n[DRY RUN] nothing created. Re-run with --live to create the ticket.")
        return {"pdf": pdf_path, "dry_run": True, "kwargs": kwargs}

    dup_jql, dup_label = _duplicate_query(kwargs)
    if dup_jql:
        existing = search_jira_tickets(dup_jql)
        if existing.get("error"):
            raise RuntimeError(f"duplicate check failed: {existing['error']}")
        if existing.get("total", 0) > 0:
            keys = ", ".join(i["key"] for i in existing["issues"])
            print(f"\n[DUPLICATE] {dup_label} already exists on {keys} — no ticket created.")
            return {"pdf": pdf_path, "duplicate": True, "existing": keys, "kwargs": kwargs}
    else:
        log.warning("No Mailer PO or Manager Order # — duplicate check skipped")

    ticket = create_jira_ticket(**kwargs, order_text=extract_pdf_text(pdf_path))
    if "error" in ticket:
        raise RuntimeError(f"create failed: {ticket['error']}")

    key = ticket["key"]
    url = f"{_get_jira_base_url()}/browse/{key}"
    print(f"\nCREATED: {key}  {url}")
    print("  (no work order linked — this path does not run the IBM i step)")

    if attach:
        try:
            attach_file_to_ticket(key, pdf_path)
            print(f"Attached source PDF to {key}")
        except Exception as e:
            print(f"attach warning: {e}")

    print("Verifying what landed on the ticket:")
    verification = _verify_created(key, kwargs)
    for line in verification:
        print(line)

    return {"pdf": pdf_path, "ticket_key": key, "url": url,
            "verification": verification, "kwargs": kwargs}


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
    ap.add_argument("--model", default=ai_extract.DEFAULT_MODEL)
    ap.add_argument("--live", action="store_true",
                    help="actually create the ticket (default is a dry run)")
    ap.add_argument("--no-attach", action="store_true",
                    help="skip attaching the source PDF to the created ticket")
    ap.add_argument("--enrich", action="store_true",
                    help="fill a blank db_code from config/*.yaml via client_lookup")
    ap.add_argument("--json", metavar="FILE",
                    help="write the built kwargs and results to FILE as JSON")
    args = ap.parse_args()

    pdfs = _iter_pdfs(args.path)
    if not pdfs:
        log.error("No PDF found at: %s", args.path)
        return 1

    results, failed = [], 0
    for pdf in pdfs:
        try:
            results.append(llm_create(str(pdf), model=args.model, live=args.live,
                                      attach=not args.no_attach, enrich=args.enrich))
        except Exception as e:
            log.error("%s: %s", pdf.name, e)
            results.append({"pdf": str(pdf), "error": str(e)})
            failed += 1

    if len(pdfs) > 1:
        created = sum(1 for r in results if r.get("ticket_key"))
        print(f"\n== {len(pdfs)} PDF(s): {created} created, {failed} failed ==")

    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
        print(f"Wrote {args.json}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
