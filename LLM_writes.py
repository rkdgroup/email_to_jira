"""
Pure-LLM DSLF ticket creation: Claude extracts every field from the order PDF and
the result is written straight to Jira. No broker parser is involved.

This exists for orders the rule-based path cannot handle at all. A PDF matching none
of the 12 fingerprints in parsers/_RULES is flagged for review and produces no ticket,
and hybrid_create.py cannot help either — it starts from process_pdf() and raises when
the rule-based parse fails. This path only needs the PDF to be readable by Claude.

    python LLM_writes.py order.pdf                 # dry run — print fields, create nothing
    python LLM_writes.py order.pdf --live          # create the ticket
    python LLM_writes.py order.pdf --live --no-attach
    python LLM_writes.py order.pdf --model claude-opus-4-8

WHAT THIS PATH GIVES UP (by design — it is the cost of skipping the parsers):
  * No client_lookup enrichment, so db_code is Claude's best guess and is often "".
    Billable Account / Client Database / Seed Database are then left blank.
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
from tools_jira import (
    create_jira_ticket,
    attach_file_to_ticket,
    search_jira_tickets,
    _get_jira_base_url,
)
from tools_pdf import extract_pdf_text
from compare_extraction import adf_to_lines

# Database codes are one letter + two digits, optionally with a suffix letter (F41D).
_DB_CODE_RE = re.compile(r"^([A-Z]\d{2})[A-Z]?$")

# Blank values here are worth seeing before a live create — none of them are fatal,
# but each one is something the rule-based path would normally have filled in.
_WATCH_FIELDS = ("db_code", "list_manager", "mailer_po", "manager_order_number",
                 "requested_quantity")


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
        log.warning("db_code %r is not the expected letter+2-digit shape — "
                    "leaving Billable Account blank", code)
        return ""
    return m.group(1)


def build_kwargs(pdf_path: str, model: str = ai_extract.DEFAULT_MODEL) -> tuple:
    """Claude-extract the PDF and map DSLF_SCHEMA onto create_jira_ticket kwargs.

    Returns (kwargs, meta) where meta carries the extraction usage/model for reporting.
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
    meta = {"usage": result.get("usage", {}), "model": result.get("model", model),
            "raw_fields": f}
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

    blanks = [k for k in _WATCH_FIELDS if not kwargs.get(k)]
    if blanks:
        print(f"\n  ! blank: {', '.join(blanks)} — nothing validated this extraction, "
              f"check against the PDF before creating.")
    usage = meta.get("usage", {})
    print(f"  ({meta.get('model')}: {usage.get('input_tokens')} in / "
          f"{usage.get('output_tokens')} out tokens)")


def llm_create(pdf_path: str, model: str = ai_extract.DEFAULT_MODEL,
               live: bool = False, attach: bool = True) -> dict:
    kwargs, meta = build_kwargs(pdf_path, model=model)
    _report(kwargs, meta)

    if not kwargs["summary"]:
        raise RuntimeError("Claude returned no list name, mailer name or manager order number "
                           "— refusing to create an untitled ticket")

    if kwargs["db_code"] in SKIP_DB_CODES:
        print(f"\n[SKIPPED] db_code {kwargs['db_code']} is in SKIP_DB_CODES — no ticket.")
        return {"skipped": True, "db_code": kwargs["db_code"]}

    if not live:
        print("\n[DRY RUN] nothing created. Re-run with --live to create the ticket.")
        return {"dry_run": True, "kwargs": kwargs}

    dup_jql, dup_label = _duplicate_query(kwargs)
    if dup_jql:
        existing = search_jira_tickets(dup_jql)
        if existing.get("error"):
            raise RuntimeError(f"duplicate check failed: {existing['error']}")
        if existing.get("total", 0) > 0:
            keys = ", ".join(i["key"] for i in existing["issues"])
            print(f"\n[DUPLICATE] {dup_label} already exists on {keys} — no ticket created.")
            return {"duplicate": True, "existing": keys}
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

    return {"ticket_key": key, "url": url}


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser(
        description="Create a DSLF ticket from a PDF using Claude for all field extraction.")
    ap.add_argument("pdf")
    ap.add_argument("--model", default=ai_extract.DEFAULT_MODEL)
    ap.add_argument("--live", action="store_true",
                    help="actually create the ticket (default is a dry run)")
    ap.add_argument("--no-attach", action="store_true",
                    help="skip attaching the source PDF to the created ticket")
    args = ap.parse_args()

    if not Path(args.pdf).is_file():
        log.error("No such PDF: %s", args.pdf)
        return 1
    try:
        llm_create(args.pdf, model=args.model, live=args.live, attach=not args.no_attach)
    except Exception as e:
        log.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
