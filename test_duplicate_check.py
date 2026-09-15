"""
Duplicate-check key tests. No network, no Jira, no PDFs.

    python test_duplicate_check.py      # standalone, prints PASS / ALL PASSED
    pytest test_duplicate_check.py      # also works

DSLF-1211 duplicated DSLF-1209 on order DM092. The second ticket came from a follow-up
email ("RE: P.O. DM092 ... zip attachment") that carried no broker order number, so the
parsed Mailer PO was blank — and the check keyed on Mailer PO alone for non-AMLC brokers,
so it returned no query and ran nothing at all. Both tickets carry Manager Order # DM092,
which would have matched. A blank key now falls back instead of skipping.

AMLC keeps keying on Manager Order # even when a Mailer PO is present: its columnar orders
put someone else's number in that field, so matching on it finds the wrong ticket.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from parse_pipeline import _dup_check_key
from parse_result import ParseResult

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


def _r(**kw):
    return ParseResult(source="test", confidence=0.92, **kw)


def test_mailer_po_is_the_key():
    jql, label = _dup_check_key(_r(list_manager="KAP", mailer_po="132991",
                                   manager_order_number="DM092"))
    check("PO wins when present", jql, 'project = DSLF AND cf[12193] = "132991"')
    check("PO label", label, "PO 132991")


def test_blank_po_falls_back_to_manager_order():
    """The DSLF-1211 case: no PO on the follow-up email, DM092 on both tickets."""
    jql, label = _dup_check_key(_r(list_manager="KAP", mailer_po="",
                                   manager_order_number="DM092"))
    check("blank PO still queries", jql, 'project = DSLF AND cf[12192] = "DM092"')
    check("fallback label", label, "Manager Order # DM092")


def test_amlc_ignores_its_mailer_po():
    jql, _ = _dup_check_key(_r(list_manager="AMLC", mailer_po="668769",
                               manager_order_number="W74926"))
    check("AMLC keys on Manager Order #", jql,
          'project = DSLF AND cf[12192] = "W74926"')


def test_no_key_at_all_skips():
    check("nothing to match on", _dup_check_key(_r(list_manager="KAP")), (None, None))


def test_amlc_with_no_manager_order_skips():
    """AMLC's PO is the wrong ticket's number, so it is not a fallback — skip instead."""
    check("AMLC never falls back to its PO",
          _dup_check_key(_r(list_manager="AMLC", mailer_po="668769")), (None, None))


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
