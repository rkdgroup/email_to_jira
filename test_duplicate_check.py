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


def test_second_copy_in_the_same_run_is_caught_before_jira_can_see_the_first():
    """DSLF-1486/1487: same PO 134012 created 13s apart; the search had not indexed 1486.

    Jira is stubbed to never find anything, as it did then, so only the in-process record
    can stop the second create.
    """
    import os, tempfile
    import parse_pipeline as pp
    import tools_jira as tj
    created = []
    saved = (tj.create_jira_ticket, tj.search_jira_tickets, tj.attach_file_to_ticket,
             pp._create_and_link_work_order, os.environ.pop("ANTHROPIC_API_KEY", None))
    tj.create_jira_ticket = lambda **kw: created.append(kw) or {"key": f"DSLF-{9000 + len(created)}"}
    tj.search_jira_tickets = lambda jql, **kw: {"total": 0, "issues": []}
    tj.attach_file_to_ticket = lambda *a, **k: None
    pp._create_and_link_work_order = lambda **kw: ""
    pp._created_this_run.clear()
    order = _r(list_manager="ADSTRA", mailer_po="134012", manager_order_number="J5991",
               mailer_name="SILENT CRY FOUNDATION", list_name="3-CARI CHILDREN AT RISK INTL")
    pdf = os.path.join(tempfile.mkdtemp(), "order.pdf")
    try:
        first = pp.finalize_and_create(order, pdf, "")
        second = pp.finalize_and_create(order, pdf, "")
    finally:
        (tj.create_jira_ticket, tj.search_jira_tickets, tj.attach_file_to_ticket,
         pp._create_and_link_work_order, key) = saved
        if key is not None:
            os.environ["ANTHROPIC_API_KEY"] = key
        pp._created_this_run.clear()
    check("first copy creates a ticket", first.get("ticket_key"), "DSLF-9001")
    check("second copy creates nothing", len(created), 1)
    check("second copy names the first", second.get("errors"), ["Duplicate: ['DSLF-9001']"])


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
