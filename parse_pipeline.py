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


def process_pdf(pdf_path: str, dry_run: bool = False, verbose: bool = False,
                broker_hint: str = "") -> dict:
    """
    Process a single PDF purchase order.
    Returns dict with keys: success, ticket_key, source, warnings, errors.

    broker_hint: optional broker key (e.g. "kap") derived from context outside the PDF
                 (e.g. sender email domain). Used as override before text-based detection
                 so orders that lack standard header fingerprints are still routed correctly.
    """
    from tools_pdf import extract_pdf_text
    from parsers import detect_broker, PARSER_REGISTRY
    from claude_fallback import claude_fallback_parse
    from parse_result import validate_result
    from tools_jira import create_jira_ticket, search_jira_tickets, flag_for_review, attach_file_to_ticket
    from client_lookup import enrich_fields
    from client_profiles import find_profile

    from tools_pdf import get_pdf_page_count, split_pdf_into_pages

    pdf_path = str(Path(pdf_path).resolve())
    log.info("Processing: %s", pdf_path)

    # Step 0: Split multi-page PDFs — one ticket per page
    page_count = get_pdf_page_count(pdf_path)
    if page_count > 1:
        log.info("Multi-page PDF (%d pages) — creating one ticket per page", page_count)
        tmp_dir, page_paths = split_pdf_into_pages(pdf_path)
        results = []
        try:
            for i, page_path in enumerate(page_paths):
                log.info("--- Page %d/%d ---", i + 1, page_count)
                r = process_pdf(page_path, dry_run=dry_run, verbose=verbose, broker_hint=broker_hint)
                results.append(r)
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return results

    # Step 1: Extract text
    text = extract_pdf_text(pdf_path)
    if text.startswith("[ERROR"):
        log.error("PDF extraction failed: %s", text)
        flag_for_review("PDF extraction failed", text)
        return {"success": False, "errors": [text]}

    if text.startswith("[WARNING"):
        log.warning("Low text extraction: %s", text[:120])

    # Step 2: Detect broker and parse
    # broker_hint (from caller context, e.g. sender email domain) takes priority over
    # text-based fingerprint detection — useful when the PDF lacks standard header markers.
    match = None
    if broker_hint and broker_hint in PARSER_REGISTRY:
        log.info("Broker hint supplied: %s — skipping text detection", broker_hint)
        from parsers import BrokerMatch, CONFIDENCE_RULE_BASED
        match = BrokerMatch(broker_key=broker_hint, confidence=CONFIDENCE_RULE_BASED,
                            matched_patterns=("hint",))
    else:
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

    # Step 5: Enrich fields from YAML config files
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
    """Build description containing only selection criteria."""

    def para(text: str) -> dict:
        return {"type": "paragraph", "content": [{"type": "text", "text": text}]}

    if result.segment_criteria:
        first_line = result.segment_criteria.splitlines()[0].strip()
        content = [para(first_line)]
    else:
        content = [para("")]

    return {"type": "doc", "version": 1, "content": content}


def _print_result(result) -> None:
    """Pretty-print extracted fields."""
    print("\n" + "=" * 60)
    print(f"Source   : {result.source} (confidence {result.confidence:.0%})")
    print(f"Summary  : {result.summary}")
    print("-" * 60)
    fields = [
        ("Segment", result.segment_criteria.splitlines()[0].strip() if result.segment_criteria else ""),
        ("Omissions", result.omission_description),
    ]
    for label, val in fields:
        if val:
            print(f"  {label:<14}: {val}")
    if result.warnings:
        print(f"\n  Warnings: {'; '.join(result.warnings)}")

    # --- Description preview ---
    print("\n" + "-" * 60)
    print("DESCRIPTION (plain text preview):")
    print("-" * 60)
    adf = _build_adf_description(result)
    for node in adf.get("content", []):
        if node["type"] == "heading":
            text = "".join(c.get("text", "") for c in node.get("content", []))
            print(f"\n[{text}]")
        elif node["type"] == "paragraph":
            text = "".join(c.get("text", "") for c in node.get("content", []))
            print(text)
        elif node["type"] == "bulletList":
            for item in node.get("content", []):
                for child in item.get("content", []):
                    text = "".join(c.get("text", "") for c in child.get("content", []))
                    print(f"  - {text}")
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
