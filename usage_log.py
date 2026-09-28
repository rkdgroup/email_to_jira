"""One JSONL line per Anthropic API call -- the only record of what the key spends.

Deliberately a copy of LRF_Processing/order-processor/usage_log.py rather than
a cross-repo import: two repos, two checkouts, no sys.path coupling. Keep the
row shape identical so one reader can total both. Read it with `summarise()`.
"""
import json
import os
import time
from pathlib import Path

# logs/ is git-ignored here, matching email_scanner/ and ticket_scanner/.
LOG_PATH = Path(__file__).resolve().parent / "logs" / "api_usage.jsonl"

# Claude API rates, USD per million tokens (claude-api skill, 2026-06-24).
_RATES = {
    "claude-opus-5": (5.0, 25.0),
    "claude-opus-4-8": (5.0, 25.0),
    "claude-sonnet-5": (2.0, 10.0),
    "claude-haiku-4-5": (1.0, 5.0),
}
_CACHE_WRITE_5M = 1.25
_CACHE_WRITE_1H = 2.0
_CACHE_READ = 0.1


def row_for(module: str, resp, ref: str = "") -> dict:
    """The row this response produces -- pure, so the shape is testable offline."""
    usage = getattr(resp, "usage", None)

    def _n(field: str, src=usage) -> int:
        return int(getattr(src, field, 0) or 0)

    return {
        "at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "module": module,
        # Set per ticket by qc_checker.check_ticket (run_order.main on the other side).
        "ticket": os.environ.get("DSLF_TICKET", ""),
        "ref": ref,
        "model": getattr(resp, "model", "") or "",
        # The truncation tell: no tool_use block follows 'max_tokens'.
        "stop_reason": getattr(resp, "stop_reason", None),
        "input": _n("input_tokens"),
        "cache_write": _n("cache_creation_input_tokens"),
        "cache_write_1h": _n("ephemeral_1h_input_tokens", getattr(usage, "cache_creation", None)),
        "cache_read": _n("cache_read_input_tokens"),
        "output": _n("output_tokens"),
    }


def record(module: str, resp, ref: str = "") -> None:
    """Append one row. Never raises -- a broken log must not fail an order."""
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row_for(module, resp, ref)) + "\n")
    except Exception:      # noqa: BLE001 -- logging is never worth a failed run
        pass


def cost_of(row: dict) -> float:
    """USD for one row, or 0.0 for a model with no published rate here."""
    rate_in, rate_out = _RATES.get(row.get("model", ""), (0.0, 0.0))
    per_token = rate_in / 1_000_000
    cw_1h = row.get("cache_write_1h", 0)
    return (row.get("input", 0) * per_token
            + (row.get("cache_write", 0) - cw_1h) * per_token * _CACHE_WRITE_5M
            + cw_1h * per_token * _CACHE_WRITE_1H
            + row.get("cache_read", 0) * per_token * _CACHE_READ
            + row.get("output", 0) * rate_out / 1_000_000)


def summarise(path: Path | None = None) -> dict:
    """Totals by module and by day, plus the truncation count. Read-only."""
    rows = []
    try:
        with (path or LOG_PATH).open(encoding="utf-8") as fh:
            for line in fh:
                try:
                    rows.append(json.loads(line))
                except ValueError:      # a torn line loses itself, not the file
                    continue
    except OSError:
        return {"calls": 0, "usd": 0.0, "by_module": {}, "by_day": {}, "truncated": 0}

    by_module: dict[str, float] = {}
    by_day: dict[str, float] = {}
    for r in rows:
        usd = cost_of(r)
        by_module[r.get("module", "?")] = by_module.get(r.get("module", "?"), 0.0) + usd
        day = str(r.get("at", ""))[:10]
        by_day[day] = by_day.get(day, 0.0) + usd
    return {
        "calls": len(rows),
        "usd": sum(by_module.values()),
        "by_module": by_module,
        "by_day": by_day,
        "truncated": sum(1 for r in rows if r.get("stop_reason") == "max_tokens"),
    }


if __name__ == "__main__":
    s = summarise()
    print(f"{s['calls']} calls, ${s['usd']:.2f}, {s['truncated']} truncated")
    for day in sorted(s["by_day"]):
        print(f"  {day}  ${s['by_day'][day]:.2f}")
    for mod in sorted(s["by_module"], key=lambda m: -s["by_module"][m]):
        print(f"  {mod:28} ${s['by_module'][mod]:.2f}")
