"""
Client lookup from YAML config files (config/*.yaml).

Enriches billable_account, db_code, list_manager from the client database.

Lookup order:
  1. Broker-specific YAML (e.g. config/rmi.yaml) — match rental_name against
     list_name then mailer_name (higher precision)
  2. Full client YAML (config/full_client_list.yaml) — fuzzy name match (fallback)
"""

import re
import json
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

_LEARNED_PATTERNS_FILE = Path(__file__).parent / "ticket_scanner" / "learned_patterns.json"

_sheet_cache:    dict[str, list[dict]] = {}
_client_cache:   list | None = None
_learned_cache:  dict | None = None
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
    for enc in ("utf-8", "cp1252"):
        try:
            with open(path, "r", encoding=enc) as f:
                data = yaml.safe_load(f) or []
            return [r for r in data if r.get("db_code")]
        except UnicodeDecodeError:
            continue
        except Exception as e:
            log.warning("Failed to load %s: %s", path, e)
            return []
    log.warning("Failed to load %s: could not decode as utf-8 or cp1252", path)
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


def _load_learned_patterns() -> dict:
    global _learned_cache
    if _learned_cache is not None:
        return _learned_cache
    if not _LEARNED_PATTERNS_FILE.exists():
        _learned_cache = {}
        return _learned_cache
    try:
        _learned_cache = json.loads(_LEARNED_PATTERNS_FILE.read_text())
    except Exception as e:
        log.warning("Could not load learned_patterns.json: %s", e)
        _learned_cache = {}
    return _learned_cache


def _learned_lookup(list_name: str, mailer_name: str) -> dict:
    """
    Fuzzy match against Jira-learned patterns (Lee's tickets).
    Patterns are keyed by List Name — matches list_name input first,
    then falls back to mailer_name.
    """
    patterns = _load_learned_patterns()
    if not patterns:
        return {}

    # Prefer list_name match (keys are list names); fall back to mailer_name
    for query in (list_name, mailer_name):
        if not query:
            continue
        best_key, best_score = None, 0.0
        for pkey in patterns:
            score = _word_overlap(query, pkey)
            if score > best_score:
                best_score, best_key = score, pkey
        if best_key and best_score >= 0.5:
            p = patterns[best_key]
            log.info("Learned pattern match (score=%.2f): %s -> %s",
                     best_score, query, p.get("client_db"))
            return {
                "billable_account": p.get("billable_account", ""),
                "list_manager":     p.get("list_manager", ""),
                "db_code":          p.get("client_db", ""),
                "lm_contact":       "",
            }
    return {}


def _row_to_result(row: dict) -> dict:
    return {
        "billable_account": row.get("billing_cust") or "",
        "list_manager":     row.get("list_manager") or "",
        "db_code":          row.get("db_code") or "",
        "lm_contact":       row.get("lm_contact") or "",
    }


def enrich_fields(
    list_name:          str = "",
    mailer_name:        str = "",
    list_manager:       str = "",
    db_code:            str = "",
    broker_only:        bool = False,
    row_manager_filter: str = "",
) -> dict:
    """
    Look up db_code, billable_account, and list_manager from YAML config.

    Priority:
      1. Exact db_code match in broker YAML or full YAML
      2. Broker-specific YAML: fuzzy match on list_name, then mailer_name (threshold 0.4)
      3. Cross-broker YAMLs: fuzzy match (threshold 0.5)  [skipped if broker_only=True]
      4. Full client YAML: fuzzy match on list_name (threshold 0.5)  [skipped if broker_only=True]

    broker_only=True: stop after step 2 — never fall through to other brokers or full list.
    row_manager_filter: if set, only match rows whose list_manager contains this string
      (e.g. "EXCHANGE" restricts AMLC lookup to AMLC EXCHANGE rows only).

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
    if row_manager_filter and broker_rows:
        broker_rows = [r for r in broker_rows
                       if row_manager_filter.upper() in (r.get("list_manager") or "").upper()]
    if broker_rows:
        best, score = _best_match(broker_rows, list_name, mailer_name)
        if best and score >= 0.4:
            log.info("Broker YAML match (score=%.2f): %s -> %s",
                     score, list_name or mailer_name, best["db_code"])
            return _row_to_result(best)

    if broker_only:
        return {}

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

    # 5. Learned patterns from Lee's Jira tickets
    result = _learned_lookup(list_name, mailer_name)
    if result:
        return result

    return {}


# Keep old signature working
def get_billable_account(list_name: str = "", db_code: str = "") -> dict:
    return enrich_fields(list_name=list_name, db_code=db_code)
