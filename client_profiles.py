"""
Client profile lookup — finds matching .doc/.docx/.xlsx files from
C:\...\Client Profiles\ based on list_manager, list_name, mailer_name, db_code.

Profile folders are organised by broker/list-manager name.
Files inside are named: "DBCODE - LIST NAME.doc" or "LIST NAME (DBCODE).doc".
"""

import re
import logging
from pathlib import Path

log = logging.getLogger(__name__)

_SELECT_BY_RE = re.compile(r"SELECT BY[:\s]+(.+?)(?:\s{3,}|\n|$)", re.IGNORECASE)

_PROFILES_DIR = Path(__file__).parent / "Client Profiles"

# Map normalised list_manager values → profile subfolder name
_MANAGER_TO_FOLDER = {
    "AMLC":             "AMLC",
    "ADSTRA":           "ADSTRA",
    "AALC":             "AALC - HAWLEY",
    "CELCO":            "CELCO",
    "CONRAD":           "CONRAD DIRECT",
    "DATA-AXLE":        "DATA AXLE",
    "NEGEV":            "JWV - NEGEV DIRECT",
    "KAP":              "KEY ACQUISITION - LIST SERVICES",
    "RMI":              "RMI",
    "NAMES IN THE NEWS": "NITN",
    "WE ARE MOORE":     "WAREMOORE",
    "RKD":              "RKD GROUP",
    "WASHINGTON LISTS": "WASHINGTON LISTS MATURE HEALTH",
}

_WORD_CLEAN = re.compile(r"[^a-z0-9 ]")


def _words(s: str) -> set:
    return {w for w in _WORD_CLEAN.sub(" ", s.lower()).split() if len(w) > 2}


def _score(filename_stem: str, *candidates: str) -> float:
    """Word-overlap score between filename stem and any candidate string."""
    fn_words = _words(filename_stem)
    if not fn_words:
        return 0.0
    best = 0.0
    for c in candidates:
        if not c:
            continue
        c_words = _words(c)
        if not c_words:
            continue
        overlap = len(fn_words & c_words) / len(fn_words)
        if overlap > best:
            best = overlap
    return best


def _all_profile_files() -> list[Path]:
    """Return all non-lock profile files across every subfolder."""
    files = []
    if not _PROFILES_DIR.is_dir():
        return files
    for sub in _PROFILES_DIR.iterdir():
        if sub.is_dir():
            for f in sub.iterdir():
                if (f.is_file()
                        and not f.name.startswith("~")
                        and f.suffix.lower() in (".doc", ".docx", ".xlsx", ".xls")):
                    files.append(f)
    return files


def find_profile(
    list_manager: str,
    list_name: str = "",
    mailer_name: str = "",
    db_code: str = "",
) -> Path | None:
    """
    Find the best-matching client profile file for a ticket.

    Matching priority:
      1. db_code present in filename — searched across ALL profile folders
      2. Highest word-overlap between filename and list_name / mailer_name,
         preferring the broker's mapped folder, then all folders

    Returns Path to the file, or None if no suitable match found.
    """
    # 1. db_code match across all folders
    if db_code:
        code_variants = [db_code.upper()]
        # e.g. "N92D" → also try "N92"
        if re.match(r"^[A-Z]\d+[A-Z]$", db_code.upper()):
            code_variants.append(db_code.upper()[:-1])
        for f in _all_profile_files():
            for variant in code_variants:
                if variant in f.stem.upper():
                    log.info("Profile matched by db_code %s: %s", db_code, f.name)
                    return f

    # 2. Fuzzy name match — prefer broker folder, then all
    folder_name = _MANAGER_TO_FOLDER.get((list_manager or "").upper())
    search_pools: list[list[Path]] = []

    if folder_name:
        folder = _PROFILES_DIR / folder_name
        if folder.is_dir():
            broker_files = [
                f for f in folder.iterdir()
                if f.is_file()
                and not f.name.startswith("~")
                and f.suffix.lower() in (".doc", ".docx", ".xlsx", ".xls")
            ]
            search_pools.append(broker_files)

    search_pools.append(_all_profile_files())

    for pool in search_pools:
        if not pool:
            continue
        scored = [(f, _score(f.stem, list_name, mailer_name)) for f in pool]
        scored.sort(key=lambda x: x[1], reverse=True)
        best_file, best_score = scored[0]
        if best_score >= 0.3:
            log.info("Profile matched (score=%.2f): %s", best_score, best_file.name)
            return best_file

    log.debug("No profile match for list=%r mailer=%r db=%r", list_name, mailer_name, db_code)
    return None


def extract_select_by(profile_path: Path) -> str:
    """
    Extract the SELECT BY value from a client profile .doc or .docx file.

    For .docx: uses python-docx to read paragraph text.
    For .doc: reads raw bytes (OLE2) — the text is stored as Latin-1 ASCII runs,
              which is sufficient to locate and extract the SELECT BY value without
              launching Word.

    Returns the value after 'SELECT BY:' (e.g. 'TRANSACTION $ AND DATE'),
    or empty string if not found or the file cannot be read.
    """
    if not profile_path or not profile_path.exists():
        return ""

    suffix = profile_path.suffix.lower()

    try:
        if suffix == ".docx":
            from docx import Document as _DocxDocument
            doc = _DocxDocument(str(profile_path))
            full_text = "\n".join(p.text for p in doc.paragraphs)
        elif suffix == ".doc":
            with open(profile_path, "rb") as fh:
                raw = fh.read()
            # Strip non-printable bytes, keep ASCII printable + space/tab
            full_text = "".join(
                ch if 32 <= ord(ch) < 128 else " "
                for ch in raw.decode("latin-1", errors="replace")
            )
        else:
            return ""
    except Exception as exc:
        log.warning("extract_select_by: could not read %s: %s", profile_path.name, exc)
        return ""

    m = _SELECT_BY_RE.search(full_text)
    if not m:
        return ""

    value = m.group(1).strip()

    # Strip Word mail-merge field placeholder (e.g. MERGEFIELD "SELECT_BY"  ACTUAL)
    mf = re.match(r'MERGEFIELD\s+"?SELECT_BY"?\s+(.*)', value, re.IGNORECASE)
    if mf:
        value = mf.group(1).strip()

    return value
