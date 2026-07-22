"""
compare_extraction.py — OFFLINE side-by-side of the DSLF fields currently on a Jira
ticket vs. what Claude extracts from the same order PDF.

Read-only: GETs the ticket + downloads its attachment from Jira (read-only .env token
is enough) and POSTs the PDF to the Anthropic API via ai_extract. It never writes to
Jira and is not wired into the live pipeline.

    python compare_extraction.py DSLF-916                  # ticket + its order PDF
    python compare_extraction.py DSLF-916 --pdf order.pdf  # local PDF instead of attachment
    python compare_extraction.py --pdf order.pdf           # no ticket: just Claude's read
    python compare_extraction.py DSLF-916 --md report.md   # write a Markdown side-by-side
    python compare_extraction.py DSLF-916 --json out.json  # dump the comparison as JSON
"""

import os
import re
import sys
import json
import argparse
import tempfile
import logging
from pathlib import Path

import requests
from dotenv import load_dotenv

_SCRIPT_DIR = Path(__file__).parent
load_dotenv(_SCRIPT_DIR / ".env")

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
log = logging.getLogger("compare_extraction")

import tools_jira as tj
from tools_pdf import extract_pdf_text
from parsers import detect_broker
from ai_extract import extract_fields_from_pdf, FIELD_ORDER

# ── Field comparison spec: ai_extract key -> (label, jira field id, kind) ────
# kind: text | select | number | date | adf | prose(list on Claude side)
FIELD_SPEC = [
    ("list_name",                "List Name",              "customfield_12234", "text"),
    ("mailer_name",              "Mailer Name",            "customfield_12194", "text"),
    ("manager_order_number",     "Manager Order #",        "customfield_12192", "text"),
    ("mailer_po",                "Mailer PO",              "customfield_12193", "text"),
    ("list_manager",             "List Manager",           "customfield_12231", "text"),
    ("requestor_name",           "Requestor Name",         "customfield_12232", "text"),
    ("requestor_email",          "Requestor Email",        "customfield_12233", "text"),
    ("mail_date",                "Mail Date",              "customfield_12196", "date"),
    ("due_date",                 "Due Date (Ship By)",     "duedate",           "date"),
    ("requested_quantity",       "Requested Quantity",     "customfield_12271", "number"),
    ("availability_rule",        "Availability Rule",      "customfield_12273", "select"),
    ("file_format",              "File Format",            "customfield_12274", "select"),
    ("ship_to_email",            "Ship To Email",          "customfield_12275", "text"),
    ("shipping_method",          "Shipping Method",        "customfield_12276", "select"),
    ("shipping_instructions",    "Shipping Instructions",  "customfield_12277", "text"),
    ("other_fees",               "Other Fees",             "customfield_12278", "text"),
    ("key_code",                 "Key Code",               "customfield_12195", "text"),
    ("db_code",                  "Client Database",        "customfield_12155", "select"),
    ("description",              "Description",            "description",       "adf"),
    ("omission_description",     "Omission Description",   "customfield_12270", "adf"),
    ("special_seed_instructions","Special Seed Instr.",    "customfield_12311", "text"),
]

_PROSE_KEYS = {"description", "omission_description", "special_seed_instructions"}


# ── ADF -> lines (structure-preserving) ─────────────────────────────────────
def adf_to_lines(adf) -> list:
    """Render a Jira ADF doc to display lines, preserving paragraph/list structure.
    A single run-on paragraph collapses to ONE line — an honest view of the field."""
    if not isinstance(adf, dict):
        return [str(adf)] if adf else []
    lines: list[str] = []

    def inline_text(node) -> str:
        parts: list[str] = []

        def w(n):
            if isinstance(n, list):
                for x in n:
                    w(x)
                return
            if not isinstance(n, dict):
                return
            if n.get("type") == "text":
                parts.append(n.get("text", ""))
            elif n.get("type") == "hardBreak":
                parts.append(" ")
            else:
                for c in n.get("content", []) or []:
                    w(c)

        w(node)
        return " ".join("".join(parts).split())

    def walk(node, bullet=False):
        if isinstance(node, list):
            for n in node:
                walk(n)
            return
        if not isinstance(node, dict):
            return
        t = node.get("type")
        if t in ("paragraph", "heading"):
            s = inline_text(node)
            if s:
                lines.append(("• " if bullet else "") + s)
        elif t in ("bulletList", "orderedList"):
            for li in node.get("content", []) or []:
                for c in li.get("content", []) or []:
                    walk(c, bullet=True)
        else:
            for c in node.get("content", []) or []:
                walk(c)

    walk(adf)
    return lines


# ── Current ticket read (all fields) ────────────────────────────────────────
def get_ticket_all_fields(key: str) -> dict:
    url = f"{tj._get_jira_base_url()}/rest/api/3/issue/{key}"
    r = requests.get(url, auth=tj._auth(), headers={"Accept": "application/json"}, timeout=20)
    r.raise_for_status()
    return r.json().get("fields", {})


def current_value(raw_fields: dict, jira_id: str, kind: str):
    """Return the current value as a string (scalars) or list[str] (adf)."""
    v = raw_fields.get(jira_id)
    if kind == "adf":
        return adf_to_lines(v)
    if kind == "select":
        return (v or {}).get("value", "") if isinstance(v, dict) else ""
    if kind == "number":
        return "" if v in (None, "") else str(int(float(v)))
    return "" if v is None else str(v)


# ── Comparison ──────────────────────────────────────────────────────────────
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip().upper()


def _as_lines(val) -> list:
    if isinstance(val, list):
        return [str(x) for x in val if str(x).strip()]
    return [str(val)] if str(val).strip() else []


def build_comparison(raw_fields: dict | None, claude_fields: dict) -> list:
    rows = []
    for key, label, jira_id, kind in FIELD_SPEC:
        claude_val = claude_fields.get(key, [] if key in _PROSE_KEYS else "")
        cur_val = current_value(raw_fields, jira_id, kind) if raw_fields is not None else (
            [] if kind == "adf" else "")

        if kind == "adf" or key in _PROSE_KEYS:
            cur_lines, claude_lines = _as_lines(cur_val), _as_lines(claude_val)
            match = _norm(" ".join(cur_lines)) == _norm(" ".join(claude_lines))
            rows.append({"key": key, "label": label, "prose": True,
                         "current": cur_lines, "claude": claude_lines,
                         "match": match, "has_current": raw_fields is not None})
        else:
            cur_s = "" if isinstance(cur_val, list) else str(cur_val)
            claude_s = "" if isinstance(claude_val, list) else str(claude_val)
            match = _norm(cur_s) == _norm(claude_s)
            rows.append({"key": key, "label": label, "prose": False,
                         "current": cur_s, "claude": claude_s,
                         "match": match, "has_current": raw_fields is not None})
    return rows


# ── Renderers ───────────────────────────────────────────────────────────────
def render_terminal(rows: list, has_current: bool) -> None:
    mark = {True: "==", False: "!="}
    for r in rows:
        m = "" if not has_current else f" [{mark[r['match']]}]"
        print(f"\n-- {r['label']}{m}")
        if r["prose"]:
            if has_current:
                print("  CURRENT:")
                for ln in (r["current"] or ["(blank)"]):
                    print(f"    {ln}")
            print("  CLAUDE:")
            for ln in (r["claude"] or ["(blank)"]):
                print(f"    {ln}")
        else:
            if has_current:
                print(f"  CURRENT: {r['current'] or '(blank)'}")
            print(f"  CLAUDE : {r['claude'] or '(blank)'}")


def render_markdown(rows: list, has_current: bool, meta: dict) -> str:
    out = ["# Extraction comparison", ""]
    for k, v in meta.items():
        out.append(f"- **{k}:** {v}")
    out.append("")
    scalar = [r for r in rows if not r["prose"]]
    prose = [r for r in rows if r["prose"]]

    out.append("## Scalar fields")
    out.append("")
    if has_current:
        out.append("| Field | Current | Claude | |")
        out.append("|---|---|---|---|")
        for r in scalar:
            mk = "" if r["match"] else "≠"
            out.append(f"| {r['label']} | {r['current'] or '_(blank)_'} | {r['claude'] or '_(blank)_'} | {mk} |")
    else:
        out.append("| Field | Claude |")
        out.append("|---|---|")
        for r in scalar:
            out.append(f"| {r['label']} | {r['claude'] or '_(blank)_'} |")
    out.append("")

    for r in prose:
        mk = "" if (not has_current or r["match"]) else " ≠"
        out.append(f"## {r['label']}{mk}")
        out.append("")
        if has_current:
            out.append("**Current ticket:**")
            out.append("")
            for ln in (r["current"] or ["_(blank)_"]):
                out.append(f"- {ln}")
            out.append("")
        out.append("**Claude:**")
        out.append("")
        for ln in (r["claude"] or ["_(blank)_"]):
            out.append(f"- {ln}")
        out.append("")
    return "\n".join(out)


# ── PDF selection ────────────────────────────────────────────────────────────
def find_order_pdf(ticket_key: str, tmpdir: Path):
    """Download ticket PDFs, return (path, broker_key) for the one detect_broker matches."""
    atts = tj.get_ticket_attachments(ticket_key)
    pdfs = [a for a in atts
            if a.get("filename", "").lower().endswith(".pdf")
            or "pdf" in (a.get("mimeType") or "").lower()]
    if not pdfs:
        return None, None
    first = None
    for i, a in enumerate(pdfs):
        dest = tmpdir / f"att_{i}.pdf"
        try:
            tj.download_attachment(a["content"], str(dest))
        except Exception as e:
            log.warning("download failed for %s: %s", a.get("filename"), e)
            continue
        first = first or str(dest)
        try:
            m = detect_broker(extract_pdf_text(str(dest)))
        except Exception:
            m = None
        if m is not None:
            print(f"  matched broker '{m.broker_key}' in attachment: {a.get('filename')}")
            return str(dest), m.broker_key
    print("  no attachment matched a known broker; falling back to first PDF")
    return first, None


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> int:
    try:  # Windows consoles default to cp1252; keep Unicode output from crashing
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="Compare current ticket fields vs. Claude PDF extraction.")
    ap.add_argument("ticket", nargs="?", help="DSLF ticket key (e.g. DSLF-916)")
    ap.add_argument("--pdf", help="local order PDF (overrides ticket attachment)")
    ap.add_argument("--model", default="claude-opus-4-8")
    ap.add_argument("--md", metavar="FILE", help="write Markdown report to FILE")
    ap.add_argument("--json", metavar="FILE", help="write comparison JSON to FILE")
    args = ap.parse_args()

    if not args.ticket and not args.pdf:
        ap.error("provide a ticket key and/or --pdf")

    tmp = Path(tempfile.mkdtemp(prefix="cmp_extract_"))
    raw_fields = None
    summary = ""

    if args.ticket:
        print(f"Reading ticket {args.ticket} ...")
        raw_fields = get_ticket_all_fields(args.ticket)
        summary = raw_fields.get("summary", "")
        print(f"  {summary}")

    pdf_path = args.pdf
    if not pdf_path:
        if not args.ticket:
            ap.error("--pdf is required when no ticket is given")
        print("Locating order PDF from attachments ...")
        pdf_path, _ = find_order_pdf(args.ticket, tmp)
        if not pdf_path:
            print("ERROR: no PDF attachment found; pass --pdf", file=sys.stderr)
            return 1

    print(f"Extracting with {args.model} from {Path(pdf_path).name} ...")
    result = extract_fields_from_pdf(pdf_path, model=args.model)
    claude_fields = result["fields"]
    usage = result["usage"]
    print(f"  tokens: {usage['input_tokens']} in / {usage['output_tokens']} out "
          f"(cache_read {usage.get('cache_read_input_tokens', 0)})")

    rows = build_comparison(raw_fields, claude_fields)
    has_current = raw_fields is not None
    render_terminal(rows, has_current)

    meta = {"ticket": args.ticket or "(none)", "summary": summary,
            "pdf": Path(pdf_path).name, "model": result["model"],
            "tokens": f"{usage['input_tokens']} in / {usage['output_tokens']} out"}

    if args.md:
        Path(args.md).write_text(render_markdown(rows, has_current, meta), encoding="utf-8")
        print(f"\nMarkdown report -> {args.md}")
    if args.json:
        Path(args.json).write_text(
            json.dumps({"meta": meta, "rows": rows, "claude_fields": claude_fields}, indent=2),
            encoding="utf-8")
        print(f"JSON -> {args.json}")

    if has_current:
        diffs = sum(1 for r in rows if not r["match"])
        print(f"\n{diffs}/{len(rows)} fields differ.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
