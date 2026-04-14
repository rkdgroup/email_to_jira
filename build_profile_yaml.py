"""
Build config/client_profiles.yaml from all .doc/.docx client profile sheets.

Extracts 5 fields per db_code:
  select_by, flags, dollar_cap, standard_suppressions, special_instructions

Run from project root:
    python build_profile_yaml.py

Re-run whenever profiles are added or changed.
Supersedes build_select_by_yaml.py / config/select_by.yaml.
"""

import re
import logging
from pathlib import Path
import yaml

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

_ROOT     = Path(__file__).parent
_PROFILES = _ROOT / "Client Profiles"
_OUT_YAML = _ROOT / "config" / "client_profiles.yaml"

_CODE_RE = re.compile(r"\b([A-Z]\d+[A-Z]?)\b")

# Strip Word MERGEFIELD placeholders: MERGEFIELD "FIELD_NAME"  actual_value
_MF_RE = re.compile(r'MERGEFIELD\s+"?[^""\s]+"?\s*', re.IGNORECASE)

# Single-line field patterns (value ends at 3+ spaces, known label, or line end)
_SELECT_BY_RE  = re.compile(r"SELECT BY[:\s]+(.+?)(?:\s{3,}|STANDARD\s+SUP|FILE\s+UPDATED|\*\*|$)", re.IGNORECASE)
_FLAGS_RE      = re.compile(r"\bFLAGS?:\s*(.+?)(?:\s{3,}|ALL\s*-\s*GET|$)", re.IGNORECASE)
_DOLLAR_CAP_RE = re.compile(r"\$\s*CAP[:\s]+(.+?)(?:\s{3,}|APPROVAL|$)", re.IGNORECASE)


def _strip_mergefield(s: str) -> str:
    return _MF_RE.sub("", s).strip()


def _clean_item(s: str) -> str:
    """Strip leading dash/space, MERGEFIELD, and right-column bleed from a list item."""
    s = _strip_mergefield(s)
    s = re.sub(r"^\s*[-*]\s*", "", s).strip()
    # Strip right-column content that bled in (3+ spaces separate columns)
    s = re.split(r"\s{3,}", s)[0].strip()
    return s


_GARBAGE_RE = re.compile(r"[^\x20-\x7E]{3,}")  # 3+ non-printable chars = garbage line


def _is_garbage(s: str) -> bool:
    return bool(_GARBAGE_RE.search(s)) or len(s) > 200


def _is_section_header(line: str) -> bool:
    """True if line looks like a new section label (all-caps label ending in colon)."""
    return bool(re.match(r"^[A-Z][A-Z\s\$\./]{3,}:", line.strip()))


# ---------------------------------------------------------------------------
# .docx extraction (paragraph-based — cleanest)
# ---------------------------------------------------------------------------

def _extract_docx(path: Path) -> dict:
    from docx import Document
    doc = Document(str(path))
    lines = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    return _parse_lines(lines)


# ---------------------------------------------------------------------------
# .doc extraction (binary Latin-1, \r as line separator)
# ---------------------------------------------------------------------------

def _extract_doc(path: Path) -> dict:
    with open(path, "rb") as fh:
        raw = fh.read()
    full = "".join(
        ch if 32 <= ord(ch) < 128 or ch in "\n\r" else " "
        for ch in raw.decode("latin-1", errors="replace")
    )
    lines = [ln.strip() for ln in re.split(r"[\r\n]+", full) if ln.strip()]
    return _parse_lines(lines)


# ---------------------------------------------------------------------------
# Shared line-based parser
# ---------------------------------------------------------------------------

def _parse_lines(lines: list[str]) -> dict:
    result = {
        "select_by": "",
        "flags": "",
        "dollar_cap": "",
        "standard_suppressions": [],
        "special_instructions": [],
    }

    i = 0
    while i < len(lines):
        line = lines[i]
        upper = line.upper()

        # --- SELECT BY ---
        if "SELECT BY" in upper and not result["select_by"]:
            m = _SELECT_BY_RE.search(line)
            if m:
                val = _strip_mergefield(m.group(1)).strip()
                if val:
                    result["select_by"] = val

        # --- FLAGS ---
        if re.match(r"\s*FLAGS?\s*:", line, re.IGNORECASE) and not result["flags"]:
            m = _FLAGS_RE.search(line)
            if m:
                val = _strip_mergefield(m.group(1)).strip()
                # Strip trailing approval text if leaked
                val = re.split(r"\s{2,}", val)[0].strip()
                if val:
                    result["flags"] = val

        # --- DOLLAR CAP ---
        if re.match(r"\s*\$\s*CAP\s*[:\s]", line, re.IGNORECASE) and not result["dollar_cap"]:
            m = _DOLLAR_CAP_RE.search(line)
            if m:
                val = _strip_mergefield(m.group(1)).strip()
                val = re.split(r"\s{2,}", val)[0].strip()
                if val:
                    result["dollar_cap"] = val

        # --- STANDARD SUPPRESSIONS block ---
        if re.match(r"\s*STANDARD\s+SUPR?ESSIONS?\s*[:\s]", line, re.IGNORECASE) \
                and not result["standard_suppressions"]:
            i += 1
            while i < len(lines):
                item = lines[i]
                # Stop at next major section header
                if re.match(r"\s*(SEED LIST|SPECIAL INSTRUCTIONS|HYPERLINKS|CONTACT|FILE NAME)\s*[:\s]",
                             item, re.IGNORECASE):
                    break
                # Collect lines that are suppression items (start with - or *)
                if re.match(r"\s*[-*]", item):
                    cleaned = _clean_item(item)
                    if cleaned and not _is_garbage(cleaned):
                        result["standard_suppressions"].append(cleaned)
                i += 1
            continue  # skip the i += 1 at bottom

        # --- SPECIAL INSTRUCTIONS block ---
        if re.match(r"\s*SPECIAL\s+INSTRUCTIONS?\s*[:\s]?", line, re.IGNORECASE) \
                and not result["special_instructions"]:
            i += 1
            while i < len(lines):
                item = lines[i]
                if re.match(r"\s*HYPERLINKS?\s*[:\s]", item, re.IGNORECASE):
                    break
                if re.match(r"\s*[-*]", item):
                    cleaned = _clean_item(item)
                    if cleaned and not _is_garbage(cleaned):
                        result["special_instructions"].append(cleaned)
                i += 1
            continue

        i += 1

    return result


# ---------------------------------------------------------------------------
# db_code detection from filename stem
# ---------------------------------------------------------------------------

def _extract_db_code(stem: str) -> str:
    m = re.match(r"^([A-Z]\d+[A-Z]?)\s*[-\s]", stem)
    if m:
        return m.group(1)
    in_parens = re.findall(r"\(([A-Z]\d+[A-Z]?)\)", stem)
    if in_parens:
        suffixed = [c for c in in_parens if re.match(r"[A-Z]\d+[A-Z]$", c)]
        return suffixed[0] if suffixed else in_parens[0]
    codes = _CODE_RE.findall(stem)
    if codes:
        suffixed = [c for c in codes if re.match(r"[A-Z]\d+[A-Z]$", c)]
        return suffixed[0] if suffixed else codes[0]
    return ""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build():
    if not _PROFILES.is_dir():
        log.error("Client Profiles directory not found: %s", _PROFILES)
        return

    files = [
        f for f in _PROFILES.rglob("*")
        if f.is_file()
        and not f.name.startswith("~")
        and f.suffix.lower() in (".doc", ".docx")
    ]
    log.info("Found %d profile files", len(files))

    mapping: dict[str, dict] = {}
    skipped_no_code = []
    skipped_empty   = []

    for path in sorted(files):
        stem = path.stem.upper()
        db_code = _extract_db_code(stem)
        if not db_code:
            skipped_no_code.append(path.name)
            continue
        if db_code in mapping:
            continue  # first file wins (active profiles beat OLD PROFILES/)

        try:
            if path.suffix.lower() == ".docx":
                data = _extract_docx(path)
            else:
                data = _extract_doc(path)
        except Exception as exc:
            log.warning("Cannot read %s: %s", path.name, exc)
            continue

        # Skip files with no useful content at all
        if not any([data["select_by"], data["flags"], data["dollar_cap"]]):
            skipped_empty.append(path.name)
            continue

        mapping[db_code] = data
        log.info("  %-10s select_by=%-35s flags=%-30s cap=%s  supp=%d  instr=%d",
                 db_code,
                 data["select_by"][:35],
                 data["flags"][:30],
                 data["dollar_cap"][:10],
                 len(data["standard_suppressions"]),
                 len(data["special_instructions"]))

    # Write YAML
    _OUT_YAML.parent.mkdir(exist_ok=True)
    with open(_OUT_YAML, "w", encoding="utf-8") as fh:
        fh.write("# Client profile data — db_code → {select_by, flags, dollar_cap,\n")
        fh.write("#   standard_suppressions, special_instructions}\n")
        fh.write("# Auto-generated by build_profile_yaml.py — re-run when profiles change.\n")
        fh.write("# Manually editable: add/modify entries freely.\n\n")
        yaml.dump(
            {k: mapping[k] for k in sorted(mapping)},
            fh,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

    log.info("\nWrote %d entries to %s", len(mapping), _OUT_YAML)
    if skipped_no_code:
        log.info("Skipped (no db_code in filename): %d", len(skipped_no_code))
    if skipped_empty:
        log.info("Skipped (no extractable fields):  %d", len(skipped_empty))


if __name__ == "__main__":
    build()
