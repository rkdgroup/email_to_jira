"""
Client lookup from YAML config files (config/*.yaml).

Enriches billable_account, db_code, list_manager from the client database.

Lookup order:
  1. Broker-specific YAML (e.g. config/rmi.yaml) — match rental_name against
     list_name then mailer_name (higher precision)
  2. Full client YAML (config/full_client_list.yaml) — fuzzy name match (fallback)
"""

import re
import logging
from pathlib import Path

log = logging.getLogger(__name__)

_CONFIG_DIR = Path(__file__).parent / "config"

# Map list_manager values → YAML filename (without .yaml)
_MANAGER_TO_FILE = {
    "AMLC":             "amlc",
    "ADSTRA":           "adstra",
    "AALC":             "aalc",
    "CELCO":            "celco",
    "CONRAD":           "conrad",
    "DATA-AXLE":        "data_axle",
    "KAP":              "kap",
    "MARY E GRANGER":   "mary_e_granger",
    "NEGEV":            "negev",
    "NAMES IN THE NEWS":"nitn",
    "RKD":              "rkd",
    "RMI":              "rmi",
    "WASHINGTON LISTS": "washington_list",
    "WE ARE MOORE":     "we_are_moore",
}

_sheet_cache:  dict[str, list[dict]] = {}
_client_cache: list | None = None
_WORD_CLEAN_RE = re.compile(r"[^a-z0-9 ]")


def _words(s: str) -> set:
    return {w for w in _WORD_CLEAN_RE.sub(" ", s.lower()).split() if len(w) > 2}


def _word_overlap(a: str, b: str) -> float:
    wa, wb = _words(a), _words(b)
    if not wa:
        return 0.0
    matches = sum(
        1 for w in wa
        if any(w == v or w.startswith(v) or v.startswith(w) for v in wb)
    )
    return matches / len(wa)


def _load_yaml(path: Path) -> list[dict]:
    import yaml
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or []
        return [r for r in data if r.get("db_code")]
    except Exception as e:
        log.warning("Failed to load %s: %s", path, e)
        return []


def _load_broker_sheet(list_manager: str) -> list[dict]:
    """Load broker-specific YAML. Cached."""
    file_key = _MANAGER_TO_FILE.get((list_manager or "").upper().strip())
    if not file_key:
        return []
    if file_key in _sheet_cache:
        return _sheet_cache[file_key]

    path = _CONFIG_DIR / f"{file_key}.yaml"
    if not path.exists():
        log.warning("Broker YAML not found: %s", path)
        _sheet_cache[file_key] = []
        return []

    rows = _load_yaml(path)
    log.info("Loaded %d rows from %s", len(rows), path.name)
    _sheet_cache[file_key] = rows
    return rows


def _load_all_clients() -> list[dict]:
    """Load full client list (config/full_client_list.yaml). Cached."""
    global _client_cache
    if _client_cache is not None:
        return _client_cache

    path = _CONFIG_DIR / "full_client_list.yaml"
    _client_cache = _load_yaml(path)
    log.info("Loaded %d clients from full_client_list.yaml", len(_client_cache))
    return _client_cache


def _best_match(rows: list[dict], *names: str) -> tuple[dict | None, float]:
    """Return (best_row, best_score) by matching names against rental_name and db_name."""
    best, best_score = None, 0.0
    for row in rows:
        for name in names:
            if not name:
                continue
            for field in ("rental_name", "db_name"):
                score = _word_overlap(name, row.get(field, "") or "")
                if score > best_score:
                    best_score, best = score, row
    return best, best_score


def _row_to_result(row: dict) -> dict:
    return {
        "billable_account": row.get("billing_cust") or "",
        "list_manager":     row.get("list_manager") or "",
        "db_code":          row.get("db_code") or "",
        "lm_contact":       row.get("lm_contact") or "",
    }


def enrich_fields(
    list_name:    str = "",
    mailer_name:  str = "",
    list_manager: str = "",
    db_code:      str = "",
) -> dict:
    """
    Look up db_code, billable_account, and list_manager from YAML config.

    Priority:
      1. Exact db_code match in broker YAML or full YAML
      2. Broker-specific YAML: fuzzy match on list_name, then mailer_name (threshold 0.4)
      3. Cross-broker YAMLs: fuzzy match (threshold 0.5)
      4. Full client YAML: fuzzy match on list_name (threshold 0.5)

    Returns dict with billable_account, list_manager, db_code, lm_contact.
    Empty dict if no match found.
    """
    # 1. Exact db_code match
    if db_code:
        for row in _load_broker_sheet(list_manager) + _load_all_clients():
            if row.get("db_code", "").upper().strip() == db_code.upper().strip():
                return _row_to_result(row)

    # 2. Broker-specific YAML
    broker_rows = _load_broker_sheet(list_manager)
    if broker_rows:
        best, score = _best_match(broker_rows, list_name, mailer_name)
        if best and score >= 0.4:
            log.info("Broker YAML match (score=%.2f): %s -> %s",
                     score, list_name or mailer_name, best["db_code"])
            return _row_to_result(best)

    # 3. Cross-broker fallback
    for mgr_key, file_key in _MANAGER_TO_FILE.items():
        if mgr_key == (list_manager or "").upper().strip():
            continue
        rows = _load_broker_sheet(mgr_key)
        if not rows:
            continue
        best, score = _best_match(rows, list_name, mailer_name)
        if best and score >= 0.5:
            log.info("Cross-broker YAML %r match (score=%.2f): %s -> %s",
                     file_key, score, list_name or mailer_name, best["db_code"])
            return _row_to_result(best)

    # 4. Full client YAML fallback
    best, score = _best_match(_load_all_clients(), list_name)
    if best and score >= 0.5:
        log.info("Full YAML match (score=%.2f): %s -> %s", score, list_name, best["db_code"])
        return _row_to_result(best)

    return {}


# Keep old signature working
def get_billable_account(list_name: str = "", db_code: str = "") -> dict:
    return enrich_fields(list_name=list_name, db_code=db_code)
