"""
Main entry point for hybrid PDF order processing.

Flow:
  1. Extract PDF text
  2. Detect broker → run rule-based parser (free, instant)
  3. No match → Claude fallback (paid, flexible)
  4. Validate extracted fields
  5. Duplicate check in Jira
  6. Create Jira ticket (or dry-run report)

Usage (run from project root):
    python JIRA_auto/parse_pipeline.py path/to/order.pdf
    python JIRA_auto/parse_pipeline.py path/to/order.pdf --dry-run
    python JIRA_auto/parse_pipeline.py path/to/order.pdf --dry-run --verbose
    python JIRA_auto/parse_pipeline.py folder/                 # process all PDFs in folder
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Ensure project root and script directory are on sys.path
_ROOT = Path(__file__).parent.parent
_SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(_SCRIPT_DIR))
sys.path.insert(0, str(_ROOT))


if not load_dotenv(_SCRIPT_DIR / ".env"):
    load_dotenv(_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _find_supplementary_files(pdf_path: str, order_number: str) -> list[Path]:
    """
    Find supplementary files (xlsx, xls, csv) in the same folder as the PDF
    whose filename contains the given order number.
    e.g. "AMLC #667855 ZipOmits.xlsx" matches order_number "667855".
    """
    if not order_number:
        return []
    folder = Path(pdf_path).parent
    matches = []
    for f in folder.iterdir():
        if (f.is_file()
                and f.suffix.lower() in (".xlsx", ".xls", ".csv")
                and order_number in f.stem):
            matches.append(f)
    return matches


def process_pdf(pdf_path: str, dry_run: bool = False, verbose: bool = False) -> dict:
    """
    Process a single PDF purchase order.
    Returns dict with keys: success, ticket_key, source, warnings, errors.
    """
    from tools_pdf import extract_pdf_text
    from parsers import detect_broker, PARSER_REGISTRY
    from claude_fallback import claude_fallback_parse
    from parse_result import validate_result
    from tools_jira import create_jira_ticket, search_jira_tickets, flag_for_review, attach_file_to_ticket
    from client_lookup import enrich_fields
    from client_profiles import find_profile

    pdf_path = str(Path(pdf_path).resolve())
    log.info("Processing: %s", pdf_path)

    # Step 1: Extract text
    text = extract_pdf_text(pdf_path)
    if text.startswith("[ERROR"):
        log.error("PDF extraction failed: %s", text)
        flag_for_review("PDF extraction failed", text)
        return {"success": False, "errors": [text]}

    if text.startswith("[WARNING"):
        log.warning("Low text extraction: %s", text[:120])

    # Step 2: Detect broker and parse
    match = detect_broker(text)
    if match:
        log.info("Broker detected: %s (confidence %.0f%%)", match.broker_key, match.confidence * 100)
        parser = PARSER_REGISTRY[match.broker_key]
        try:
            result = parser.parse(text)
        except Exception as e:
            log.warning("Rule-based parser %s failed: %s — falling back to Claude", match.broker_key, e)
            result = claude_fallback_parse(text)
    else:
        log.info("No broker match — using Claude fallback")
        result = claude_fallback_parse(text)

    if verbose or dry_run:
        _print_result(result)

    # Step 3: Validate
    validation = validate_result(result)
    if validation.warnings:
        for w in validation.warnings:
            log.warning("  %s", w)

    # If rule-based parsing failed validation, try Claude fallback before giving up
    if not validation.valid and result.source.startswith("rule:"):
        log.info("Rule-based parse failed validation — trying Claude fallback")
        result = claude_fallback_parse(text)
        if verbose or dry_run:
            _print_result(result)
        validation = validate_result(result)
        if validation.warnings:
            for w in validation.warnings:
                log.warning("  %s", w)

    if not validation.valid:
        for e in validation.errors:
            log.error("  Validation error: %s", e)
        reason = "; ".join(validation.errors)
        if not dry_run:
            flag_for_review("Validation failed", reason)
        return {"success": False, "source": result.source, "errors": validation.errors}

    if dry_run:
        log.info("[DRY RUN] Would create ticket: %s", result.summary)
        return {"success": True, "source": result.source, "dry_run": True,
                "fields": result.to_jira_kwargs(), "warnings": list(result.warnings)}

    # Step 4: Duplicate check
    jql = f'project = DSLF AND cf[12193] = "{result.mailer_po}"'
    existing = search_jira_tickets(jql)
    if existing.get("total", 0) > 0:
        keys = [i["key"] for i in existing.get("issues", [])]
        log.warning("Duplicate PO detected — existing tickets: %s", keys)
        flag_for_review("Duplicate PO", f"PO {result.mailer_po} already exists: {keys}")
        return {"success": False, "source": result.source, "errors": [f"Duplicate: {keys}"]}

    # Step 5: Enrich fields from Excel client list
    enriched = enrich_fields(
        list_name=result.list_name or "",
        mailer_name=result.mailer_name or "",
        list_manager=result.list_manager or "",
    )
    db_code_resolved = enriched.get("db_code", "")

    # Step 6: Create ticket
    kwargs = result.to_jira_kwargs()
    kwargs["description"] = _build_adf_description(result)
    if enriched.get("billable_account") and not kwargs.get("billable_account"):
        kwargs["billable_account"] = enriched["billable_account"]
    if enriched.get("list_manager") and not kwargs.get("list_manager"):
        kwargs["list_manager"] = enriched["list_manager"]
    if db_code_resolved:
        kwargs["db_code"] = db_code_resolved
    ticket = create_jira_ticket(**kwargs)

    if "error" in ticket:
        log.error("Jira create failed: %s", ticket["error"])
        return {"success": False, "source": result.source, "errors": [ticket["error"]]}

    log.info("Created ticket: %s — %s", ticket["key"], ticket.get("url", ""))

    # Step 7: Attach source PDF to ticket
    try:
        attach_file_to_ticket(ticket["key"], pdf_path)
        log.info("PDF attached to %s", ticket["key"])
    except Exception as _e:
        log.warning("Could not attach PDF: %s", _e)

    # Step 8: Attach supplementary files (e.g. zip omit xlsx) matched by order number
    for order_num in filter(None, [result.manager_order_number, result.mailer_po]):
        for supp in _find_supplementary_files(pdf_path, order_num):
            try:
                attach_file_to_ticket(ticket["key"], str(supp))
                log.info("Supplementary file attached to %s: %s", ticket["key"], supp.name)
            except Exception as _e:
                log.warning("Could not attach supplementary file %s: %s", supp.name, _e)

    # Step 9: Attach client profile document
    try:
        profile_path = find_profile(
            list_manager=result.list_manager,
            list_name=result.list_name,
            mailer_name=result.mailer_name,
            db_code=db_code_resolved,
        )
        if profile_path:
            attach_file_to_ticket(ticket["key"], str(profile_path))
            log.info("Profile attached to %s: %s", ticket["key"], profile_path.name)
        else:
            log.info("No client profile found for %s", ticket["key"])
    except Exception as _e:
        log.warning("Could not attach client profile: %s", _e)

    return {
        "success": True,
        "ticket_key": ticket["key"],
        "ticket_url": ticket.get("url"),
        "source": result.source,
        "db_code": db_code_resolved,
        "warnings": list(result.warnings),
    }


def _build_adf_description(result) -> dict:
    """Build a clean, readable Atlassian Document Format description from a ParseResult."""

    def heading(text: str, level: int = 3) -> dict:
        return {"type": "heading", "attrs": {"level": level},
                "content": [{"type": "text", "text": text}]}

    def para(*parts) -> dict:
        """Build a paragraph from alternating (text, bold) tuples or plain strings."""
        content = []
        for part in parts:
            if isinstance(part, tuple):
                txt, bold = part
                node = {"type": "text", "text": txt}
                if bold:
                    node["marks"] = [{"type": "strong"}]
                content.append(node)
            else:
                content.append({"type": "text", "text": str(part)})
        return {"type": "paragraph", "content": content}

    def bullet_list(items: list[str]) -> dict:
        return {
            "type": "bulletList",
            "content": [
                {"type": "listItem",
                 "content": [para(item)]}
                for item in items
            ],
        }

    nodes = []

    # --- Order Details ---
    nodes.append(heading("Order Details"))
    order_type = "list exchange" if result.list_manager else "list rental"
    details = (
        f"This is a {order_type} order managed by "
        f"{result.list_manager or 'the list manager'}."
    )
    if result.manager_order_number:
        details += f" Manager Order Number: {result.manager_order_number}."
    if result.mailer_po:
        details += f" Mailer PO: {result.mailer_po}."
    nodes.append(para(details))

    # --- List & Mailer ---
    nodes.append(heading("List & Mailer"))
    qty_fmt = f"{result.requested_quantity:,}" if result.requested_quantity else "unspecified"
    avail = result.availability_rule or "standard"
    list_mailer = (
        f"{result.mailer_name} is renting the {result.list_name} list. "
        f"A total of {qty_fmt} names are requested using {avail} selection."
    )
    if result.segment_criteria:
        list_mailer += f" Selection: {result.segment_criteria}."
    if result.mail_date:
        list_mailer += f" Mail date is {result.mail_date}."
    if result.key_code:
        list_mailer += f" Key code: {result.key_code}."
    nodes.append(para(list_mailer))

    # --- Shipping ---
    nodes.append(heading("Shipping"))
    ship_parts = []
    if result.shipping_method and result.ship_to_email:
        ship_parts.append(
            f"Files are to be delivered via {result.shipping_method} to {result.ship_to_email}."
        )
    elif result.shipping_method:
        ship_parts.append(f"Files are to be delivered via {result.shipping_method}.")
    if result.ship_by_date:
        ship_parts.append(f"The order must ship by {result.ship_by_date}.")
    if result.shipping_instructions:
        ship_parts.append(result.shipping_instructions)
    nodes.append(para(" ".join(ship_parts) if ship_parts else "No shipping details provided."))

    # --- Omissions ---
    if result.omission_description:
        nodes.append(heading("Omissions"))
        # Split multi-line omission text into bullet items
        lines = [ln.strip() for ln in result.omission_description.splitlines() if ln.strip()]
        if len(lines) > 1:
            nodes.append(bullet_list(lines))
        else:
            nodes.append(para(result.omission_description))

    # --- Special Instructions ---
    if result.special_seed_instructions:
        nodes.append(heading("Special Seed Instructions"))
        nodes.append(para(result.special_seed_instructions))

    # --- Other Fees ---
    if result.other_fees:
        nodes.append(heading("Other Fees"))
        nodes.append(para(result.other_fees))

    # --- Requestor ---
    nodes.append(heading("Requestor"))
    contact = result.requestor_name or "Unknown"
    if result.requestor_email:
        contact += f" — {result.requestor_email}"
    nodes.append(para(contact))

    return {"type": "doc", "version": 1, "content": nodes}


def _print_result(result) -> None:
    """Pretty-print extracted fields."""
    print("\n" + "=" * 60)
    print(f"Source   : {result.source} (confidence {result.confidence:.0%})")
    print(f"Summary  : {result.summary}")
    print("-" * 60)
    fields = [
        ("Mailer", result.mailer_name),
        ("Mailer PO", result.mailer_po),
        ("List Name", result.list_name),
        ("List Manager", result.list_manager),
        ("Quantity", result.requested_quantity),
        ("Availability", result.availability_rule),
        ("Mail Date", result.mail_date),
        ("Ship By", result.ship_by_date),
        ("Requestor", result.requestor_name),
        ("Req. Email", result.requestor_email),
        ("Ship To Email", result.ship_to_email),
        ("Key Code", result.key_code),
        ("Ship Method", result.shipping_method),
        ("Ship Instruct", result.shipping_instructions),
        ("Omissions", result.omission_description[:80] if result.omission_description else ""),
    ]
    for label, val in fields:
        if val:
            print(f"  {label:<14}: {val}")
    if result.warnings:
        print(f"\n  Warnings: {'; '.join(result.warnings)}")
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Process PO PDF(s) and create Jira DSLF tickets")
    parser.add_argument("path", help="Path to PDF file or folder of PDFs")
    parser.add_argument("--dry-run", action="store_true", help="Extract only, do not create tickets")
    parser.add_argument("--verbose", action="store_true", help="Print extracted fields")
    args = parser.parse_args()

    if not os.getenv("JIRA_API_TOKEN") and not args.dry_run:
        print("ERROR: JIRA_API_TOKEN not set in .env")
        sys.exit(1)

    target = Path(args.path)
    if target.is_dir():
        pdfs = sorted(target.glob("*.pdf")) + sorted(target.glob("*.PDF"))
        log.info("Found %d PDF(s) in %s", len(pdfs), target)
        results = []
        for pdf in pdfs:
            r = process_pdf(str(pdf), dry_run=args.dry_run, verbose=args.verbose)
            results.append((pdf.name, r))
        # Summary
        print(f"\n{'File':<45} {'Status':<10} {'Source':<20} {'Ticket/Error'}")
        print("-" * 100)
        for name, r in results:
            status = "OK" if r["success"] else "FAIL"
            source = r.get("source", "")
            detail = r.get("ticket_key") or "; ".join(r.get("errors", []))[:40]
            if args.dry_run and r["success"]:
                detail = "(dry run)"
            print(f"{name:<45} {status:<10} {source:<20} {detail}")
    elif target.is_file():
        r = process_pdf(str(target), dry_run=args.dry_run, verbose=args.verbose)
        if r["success"]:
            if args.dry_run:
                print("\nDry run complete. Fields shown above.")
            else:
                print(f"\nTicket created: {r.get('ticket_key')} — {r.get('ticket_url')}")
        else:
            print(f"\nFailed: {'; '.join(r.get('errors', ['unknown error']))}")
            sys.exit(1)
    else:
        print(f"ERROR: {args.path!r} is not a file or directory")
        sys.exit(1)


if __name__ == "__main__":
    main()
