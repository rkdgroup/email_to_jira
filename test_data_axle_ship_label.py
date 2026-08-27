"""
Data Axle / SimioCloud Ship Label tests. No network, no Jira, no PDFs.

    python test_data_axle_ship_label.py      # standalone, prints PASS / ALL PASSED
    pytest test_data_axle_ship_label.py      # also works

Found by qc_llm's ORDER check on its first live run (DSLF-1132, 2026-08-27). The label
reads "WWP f/PBC/PO# E23063/Job #54793" and the ticket stored Mailer PO 23063 — the same
number with its leading E missing.

The mechanism is worth remembering because it is silent: the PO# capture was digits-only,
so on a letter-prefixed value it did not match at all, and control fell through to the
"first 4+ digit run anywhere in the label" fallback. That fallback found the digits of the
very value the first branch had just rejected and stored them without their prefix. A
failed match becoming a plausible wrong answer, rather than a blank, is why nobody noticed.

Both branches are pinned here: the prefix must survive, and the fallback must still exist
for labels that print a bare number with no PO# marker at all.
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


def _order(ship_label: str, order_no: str = "71018-MNay") -> str:
    """A SimioCloud order trimmed to what the parser reads, label swapped in."""
    return f"""Exchange/Rental Order
SimioCloud
Order # {order_no}
Mailer:
PUBLIC BROADCASTING CONSORTIUM
List:
WOUNDED WARRIOR PROJECT
Ship Label: {ship_label}
Quantity:
22,738 All Available
Ship To:
FTP https://files.mmidirect.com
Attn:
LKA@mmidirect.com
"""


def test_letter_prefixed_po_keeps_its_prefix():
    """DSLF-1132: E23063 was stored as 23063."""
    r = PARSER_REGISTRY["simiocloud"].parse(
        _order("WWP f/PBC/PO# E23063/Job #54793/ Qty"))
    check("PO# E23063 read whole", r.mailer_po, "E23063")


def test_plain_numeric_po_is_unchanged():
    """The 4+ digit fallback and the digits-only path must behave exactly as before."""
    r = PARSER_REGISTRY["simiocloud"].parse(_order("ACF/PO# 71018/Job #101"))
    check("bare numeric PO unchanged", r.mailer_po, "71018")


def test_po_with_no_space_after_the_hash():
    r = PARSER_REGISTRY["simiocloud"].parse(_order("XYZ/PO#12345/Job #9"))
    check("PO#12345 read", r.mailer_po, "12345")


def test_label_with_no_po_marker_falls_back_to_the_digit_run():
    r = PARSER_REGISTRY["simiocloud"].parse(_order("WWP f/PBC/998877/Job #54793"))
    check("digit-run fallback still works", r.mailer_po, "998877")


def test_label_with_nothing_usable_falls_back_to_the_order_number():
    """Last resort is the Manager Order #, which is the Order# without its rep suffix —
    71018 from "71018-MNay", not the whole string. The suffix is the Key Code source."""
    r = PARSER_REGISTRY["simiocloud"].parse(_order("WWP f/PBC/no numbers here",
                                                   order_no="71018-MNay"))
    check("falls back to the manager order #", r.mailer_po, "71018")
    check("mailer PO and manager order are then identical", r.manager_order_number, "71018")


def test_prose_word_before_the_number_does_not_become_a_prefix():
    """The letter class must not swallow an English word ahead of the digits."""
    r = PARSER_REGISTRY["simiocloud"].parse(_order("WWP/PO# for 23063/Job #1"))
    check("'for' is not a PO prefix", r.mailer_po, "23063")


def test_simiocloud_list_manager_is_we_are_moore():
    """SimioCloud is We Are Moore's ordering platform, not a broker of its own."""
    r = PARSER_REGISTRY["simiocloud"].parse(_order("WWP f/PBC/PO# E23063/Job #1"))
    check("list manager is WE ARE MOORE", r.list_manager, "WE ARE MOORE")


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
