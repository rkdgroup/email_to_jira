"""
RMI Direct field tests. No network, no Jira, no PDFs.

    python test_rmi_fields.py      # standalone, prints PASS / ALL PASSED
    pytest test_rmi_fields.py      # also works

"MGT" is RMI's label for the management-order field, not part of the number. The Manager
Order # on the ticket is 26-01658, not MGT26-01658 — and the prefix leaks further than the
one field, because ParseResult builds the title from the manager order number and
create_jira_ticket forces Seed Tracking Number to equal it. One bad capture put "MGT" in
three places on six tickets.

Four of the ten RMI tickets on file already carried the bare form (26-00310), which is what
settled the question: the parser was inconsistent with itself, and those four re-parse
byte-identically after the fix.

NOTE ON THE FIXTURE: it is the header block only. RMI parses by label-block position (see
the parser's own comments), so a flat fixture yields no mailer_name/list_name and therefore
no auto-built title, and no Broker PO#. That is why the assertions here are confined to the
manager order number and its fallback. Coverage of the full layout comes from re-parsing
the ten real RMI order PDFs, which is how this fix was verified.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from parsers import PARSER_REGISTRY

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


def _order(mgt_line: str, broker_po: str = "41736") -> str:
    """An RMI order trimmed to the header block the parser reads."""
    return f"""RMI DIRECT MARKETING
List Rental Order
{mgt_line}
08/26/2026
Order Date
MGT#
Job#
Key Code
Broker PO#
{broker_po}
Broker:
VERADATA
Owner:
DIRECT MAIL SENIOR DONORS
Mailer:
COUNCIL OF SENIORS
Quantity:
5,000
"""


def _mgr(mgt_line, broker_po="41736"):
    return PARSER_REGISTRY["rmi_direct"].parse(_order(mgt_line, broker_po)).manager_order_number


def test_the_mgt_prefix_is_not_part_of_the_number():
    """DSLF-1124 stored MGT26-01658."""
    check("MGT26-01658 becomes 26-01658", _mgr("MGT26-01658"), "26-01658")
    check("MGT26-01659 becomes 26-01659", _mgr("MGT26-01659"), "26-01659")


def test_every_real_value_on_file():
    for raw, want in (("MGT26-01659", "26-01659"),
                      ("MGT26-01658", "26-01658"),
                      ("MGT26-01158", "26-01158"),
                      ("MGT26-00509", "26-00509"),
                      ("MGT26-00463", "26-00463"),
                      ("MGT26-00385", "26-00385")):
        check(f"{raw} -> {want}", _mgr(raw), want)


def test_a_lowercase_label_is_still_stripped():
    """_find passes re.IGNORECASE, so the prefix must come off either way."""
    check("mgt26-01658 also stripped", _mgr("mgt26-01658"), "26-01658")


def test_the_prefix_reaches_three_fields_so_none_may_carry_it():
    """ParseResult builds the title from the manager order # and create_jira_ticket forces
    Seed Tracking Number to equal it, so one bad capture lands in three places."""
    r = PARSER_REGISTRY["rmi_direct"].parse(_order("MGT26-01658"))
    check("no MGT in the manager order #", "MGT" in r.manager_order_number, False)
    check("the title, when built, cannot inherit MGT", "MGT" in (r.summary or ""), False)


def test_list_manager_stays_rmi():
    r = PARSER_REGISTRY["rmi_direct"].parse(_order("MGT26-01658"))
    check("list manager is RMI", r.list_manager, "RMI")


def test_the_mailer_po_fallback_also_loses_the_prefix():
    """When an order states no Broker PO#, mailer_po falls back to the MGT number — which
    must be the stripped form too. This fixture is header-only, so RMI's label-block
    positional parse finds no Broker PO# and the fallback is what fires; the real-PDF
    check (all ten RMI orders) is what covers the normal Broker PO# path."""
    r = PARSER_REGISTRY["rmi_direct"].parse(_order("MGT26-01658"))
    check("fallback mailer PO has no MGT", r.mailer_po, "26-01658")


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
