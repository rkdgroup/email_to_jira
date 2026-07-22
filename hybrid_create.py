"""
Hybrid DSLF ticket creation: rule-based parser for all structured fields
(db_code/Client-Seed-Billable lookup, profile omits + FLAG OMITS, requestor,
Data-Axle/Saturn house rules) + Claude PDF extraction to enrich the Description
prose. The Omission Description stays rule-based (it has the profile-injected
STANDARD/FLAG omits that the PDF alone lacks) and now renders one line per
criterion via the tools_jira ADF fix.

    python hybrid_create.py /path/to/order.pdf            # create the ticket
    python hybrid_create.py /path/to/order.pdf --dry-run  # build + show, create nothing
    python hybrid_create.py /path/to/order.pdf --no-claude # rule-based only

This writes a LIVE ticket to the production DSLF project (it bypasses the
pipeline's duplicate check — call only when you intend to create).
"""

import sys
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

_SCRIPT_DIR = Path(__file__).parent
load_dotenv(_SCRIPT_DIR / ".env")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("hybrid_create")

from parse_pipeline import process_pdf
from tools_jira import create_jira_ticket, attach_file_to_ticket, _get_jira_base_url
from tools_pdf import extract_pdf_text
import ai_extract
from compare_extraction import adf_to_lines


def _text_to_adf(lines: list) -> dict:
    content = [{"type": "paragraph", "content": [{"type": "text", "text": ln}]}
               for ln in lines if str(ln).strip()]
    return {"type": "doc", "version": 1, "content": content or [{"type": "paragraph", "content": []}]}


def build_hybrid_kwargs(pdf_path: str, model: str = "claude-opus-4-8", use_claude: bool = True) -> dict:
    """Rule-based kwargs (complete) + Claude Description prose merged in."""
    res = process_pdf(pdf_path, dry_run=True)   # dry-run => full kwargs, dup-check skipped
    if not res.get("success"):
        raise RuntimeError(f"rule-based parse failed: {res.get('errors')}")
    kwargs = dict(res["fields"])

    if use_claude:
        claude = ai_extract.extract_fields_from_pdf(pdf_path, model=model)["fields"]
        rb_desc = adf_to_lines(kwargs.get("description"))            # rule-based description lines
        extra = [l for l in claude.get("description", []) if l.strip() and l not in rb_desc]
        merged = rb_desc + extra                                     # rule-based first, Claude extras appended
        kwargs["description"] = _text_to_adf(merged)
        kwargs["_claude_meta"] = {"desc_extra_lines": extra}        # for reporting; popped before create
    return kwargs


def hybrid_create(pdf_path: str, model: str = "claude-opus-4-8",
                  use_claude: bool = True, dry_run: bool = False, attach: bool = True) -> dict:
    kwargs = build_hybrid_kwargs(pdf_path, model=model, use_claude=use_claude)
    meta = kwargs.pop("_claude_meta", {})

    print("\n== Hybrid ticket fields ==")
    print(f"Title: {kwargs.get('summary')}")
    print(f"List Manager: {kwargs.get('list_manager')}   Mailer PO: {kwargs.get('mailer_po')}   "
          f"Mgr#: {kwargs.get('manager_order_number')}")
    print(f"Client DB/Billable: {kwargs.get('db_code')} / {kwargs.get('billable_account')}")
    print(f"Requestor: {kwargs.get('requestor_name')} <{kwargs.get('requestor_email')}>")
    print("Description (merged):")
    for ln in adf_to_lines(kwargs.get("description")):
        print(f"    {ln}")
    if meta.get("desc_extra_lines"):
        print(f"  (Claude added {len(meta['desc_extra_lines'])} line(s) on top of rule-based)")
    print("Omission Description (rule-based, rendered per-line):")
    for ln in (kwargs.get("omission_description", "") or "").splitlines():
        if ln.strip():
            print(f"    {ln.strip()}")

    if dry_run:
        print("\n[DRY RUN] nothing created.")
        return {"dry_run": True, "kwargs": kwargs}

    text = extract_pdf_text(pdf_path)
    ticket = create_jira_ticket(**kwargs, order_text=text)
    if "error" in ticket:
        raise RuntimeError(f"create failed: {ticket['error']}")
    key = ticket["key"]
    url = f"{_get_jira_base_url()}/browse/{key}"
    print(f"\nCREATED: {key}  {url}")

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
    ap = argparse.ArgumentParser(description="Create a DSLF ticket via the hybrid (rule-based + Claude) path.")
    ap.add_argument("pdf")
    ap.add_argument("--model", default="claude-opus-4-8")
    ap.add_argument("--dry-run", action="store_true", help="build and print, create nothing")
    ap.add_argument("--no-claude", action="store_true", help="rule-based only (skip Claude prose)")
    ap.add_argument("--no-attach", action="store_true")
    args = ap.parse_args()
    hybrid_create(args.pdf, model=args.model, use_claude=not args.no_claude,
                  dry_run=args.dry_run, attach=not args.no_attach)
    return 0


if __name__ == "__main__":
    sys.exit(main())
