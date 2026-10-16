"""
Monthly cost-report tests. No network, no Jira.

    python test_cost_report.py      # standalone, prints PASS / ALL PASSED
    pytest test_cost_report.py      # also works

The report reads money off the footer qc_checker.format_report writes, so the last test
renders a real report and parses it back: if the footer format changes, this fails rather
than the monthly total quietly dropping to $0.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from cost_report import comment_cost, tally, report_adf, _prev_month
from datetime import date

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


_PASS = ("QC CHECK RESULTS — DSLF-1446\nVERDICT: PASS\n\nChecked and correct — no action needed."
         "\n\nclaude-opus-5 · 29s · $0.067")
_UNV = ("QC CHECK RESULTS — DSLF-9\nVERDICT: UNVERIFIED\n\nQC did not run — NOT a pass."
        "\n\nclaude-opus-5 · 0s")


def test_footer_cost():
    check("footer with a figure", comment_cost(_PASS), 0.067)
    check("footer without a figure", comment_cost(_UNV), None)
    check("a $ inside a finding is not the footer",
          comment_cost("QC CHECK RESULTS\n1. WRONG  Select: $10+ -> $10-99.99"), None)


def test_tally_buckets_by_month():
    rows = [("DSLF-1", "2026-09-30T23:59:00.000-0500", _PASS),
            ("DSLF-1", "2026-10-01T00:01:00.000-0500", _PASS),
            ("DSLF-2", "2026-10-02T08:00:00.000-0500", _PASS),
            ("DSLF-3", "2026-10-03T08:00:00.000-0500", _UNV)]
    t = tally(rows, "2026-10")
    check("runs in month only", t["runs"], 3)
    check("distinct tickets", t["tickets"], 3)
    check("total", t["total"], 0.134)
    check("average over priced runs", t["avg"], 0.067)
    check("unpriced counted", t["unpriced"], 1)
    check("unverified counted", t["unverified"], 1)
    check("empty month", tally([], "2026-10")["avg"], 0.0)


def test_prev_month():
    check("mid-year", _prev_month(date(2026, 10, 16)), "2026-09")
    check("january wraps", _prev_month(date(2026, 1, 5)), "2025-12")


def test_adf_blank_line_is_empty_paragraph():
    doc = report_adf(["a", ""])
    check("doc type", doc["type"], "doc")
    check("blank line", doc["content"][1], {"type": "paragraph", "content": []})


def test_parses_real_format_report():
    from qc_checker import format_report
    body = format_report("DSLF-1", {"verdict": "PASS", "order": {
        "verdict": "PASS", "findings": [], "model": "claude-opus-5",
        "elapsed_s": 30, "cost_usd": 0.1234}})
    check("format_report footer round-trips", comment_cost(body), 0.123)


def main():
    for fn in sorted(
        (v for k, v in globals().items() if k.startswith("test_") and callable(v)),
        key=lambda f: f.__code__.co_firstlineno,
    ):
        fn()
    print()
    if _failures:
        print("FAILURES:")
        for f in _failures:
            print("  - " + f)
        return 1
    print("ALL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
