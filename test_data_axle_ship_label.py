"""
Data Axle / SimioCloud Ship Label tests. No network, no Jira, no PDFs.

    python test_data_axle_ship_label.py      # standalone, prints PASS / ALL PASSED
    pytest test_data_axle_ship_label.py      # also works

Found by qc_checker's ORDER check on its first live run (DSLF-1132, 2026-08-27). The label
reads "WWP f/PBC/PO# E23063/Job #54793" and the ticket stored Mailer PO 23063 — the same
number with its leading E missing.

The mechanism is worth remembering because it is silent: the PO# capture was digits-only,
so on a letter-prefixed value it did not match at all, and control fell through to the
"first 4+ digit run anywhere in the label" fallback. That fallback found the digits of the
very value the first branch had just rejected and stored them without their prefix. A
failed match becoming a plausible wrong answer, rather than a blank, is why nobody noticed.

Both branches are pinned here: the prefix must survive, and the fallback must still exist
for labels that print a bare number with no PO# marker at all.

The second half of the file pins the PO *forms* Suvam specified on 2026-08-27 after
DSLF-1091 came back wrong the same way. The Ship Label is a slash-separated jumble of the
mailer's reference numbers and only one of them is ours: an `E`-prefixed number is the PO
even when nothing says "PO", three letters plus two digits (CLU96) is a PO, and anything
labelled JOB belongs to the mailer and must never be chosen. Every label tested there is
real, taken from the 25 most recent WE ARE MOORE / DATA-AXLE tickets — 6 of which the old
code got wrong and 19 of which must not move.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from parsers import PARSER_REGISTRY
from parsers.data_axle import _ship_label_po

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


# ---------------------------------------------------------------------------
# The PO forms, per Suvam 2026-08-27, tested against _ship_label_po directly.
#
#   "So looking at this Ship Label: MOWP E20467/QTY/WWP/JOB 54634 you have two numbers.
#    54634 is listed as JOB but what we want is the PO. Even though E20467 doesn't
#    specifically say PO, you're going to choose that one. So if you see E-----, use that.
#    If you see one that has three letters and two numbers, like CLU56, that's your PO.
#    Anything that says JOB is for them, not us."
#
# Every label below is real, from the 25 most recent WE ARE MOORE / DATA-AXLE tickets.
# ---------------------------------------------------------------------------

def test_e_number_is_the_po_with_no_marker_at_all():
    """DSLF-1091. The E form wins even though nothing on the label says PO."""
    check("E20467 chosen over the JOB number",
          _ship_label_po("MOWP E20467/QTY/WWP/JOB 54634"), "E20467")


def test_the_job_number_is_never_the_po():
    """It is the mailer's own number. Taking it would put their reference on our ticket."""
    check("JOB not chosen when it is the only number",
          _ship_label_po("WWP/Qty/JOB 54634"), "")
    check("JOB not chosen even when it comes first",
          _ship_label_po("JOB 54634/MOWP E20467/QTY"), "E20467")
    check("lowercase Job with a hash is also excluded",
          _ship_label_po("WWP f/PBC/Job #54793"), "")


def test_merge_number_is_excluded_like_job():
    """DSLF-1117: 'Merge #54725' is the same kind of internal number."""
    check("E22163 chosen, Merge ignored",
          _ship_label_po("WWP f/F&F/PO# E22163/Merge #54725/"), "E22163")


def test_three_letters_two_digits_is_a_po():
    """DSLF-1118/1082/1077. These three had no PO at all and fell back to the order #."""
    check("CLU96 found after an empty PO#",
          _ship_label_po("WWP f/SO/PO#/CLU96/Key S98/Qty"), "CLU96")
    check("CLP78 found with no marker",
          _ship_label_po("Wounded Warrior/NYULH/Qty/CLP78"), "CLP78")
    check("CLL76 found past a Key segment",
          _ship_label_po("WWP/Qty/Key ACF/CRS/CLL76"), "CLL76")


def test_e_form_outranks_the_three_plus_two_form():
    check("E number preferred when both appear",
          _ship_label_po("SMF/E21035/Qty/CLU96"), "E21035")


def test_explicit_po_marker_outranks_both():
    check("PO# value wins over a stray CLU96",
          _ship_label_po("WWP/PO#193932/CLU96/Qty"), "193932")


def test_the_three_plus_two_form_must_match_a_whole_token():
    """DSLF-1093. An unanchored [A-Z]{3}\\d{2} finds 'AGA11' inside 'TSAGA112991' and
    invents a PO out of the middle of somebody else's number."""
    check("no PO invented from inside TSAGA112991",
          _ship_label_po("TSAGA112991-76683_Wounded Warrior_Qty"), "112991")
    check("SGK108431 likewise yields its digit run, not a fragment",
          _ship_label_po("SGK108431-76521/WWP/Qty"), "108431")


def test_values_that_must_not_move():
    """Regression guard: 19 of the 25 surveyed labels have to come out byte-identical."""
    for label, want in (
        ("WWP f/PBC/PO# E23063/Job #54793/",            "E23063"),
        ("WWP f/ASD/PO#193932/Qty",                     "193932"),
        ("FA_Wounded Warrior_69715_Key S67_Qty",        "69715"),
        ("WWP f/LELDF/CB21PH01/Qty/477353",             "477353"),
        ("WWP f/Dana Farber/PO#66835/Qty",              "66835"),
        ("70253-Guiding Light Mission-Wounded Warrior-", "70253"),
        ("UNCF/193685/Wounded Warrior/Qty",             "193685"),
        ("UICH_Wounded Warrior_69656_Qty",              "69656"),
        ("Huntington/WWP/#193561/Qty",                  "193561"),
        ("WWP f/CCAW/PO#01279 /Qty",                    "01279"),   # leading zero kept
        ("WWP f/WTTW/PO#2337780/Key WWP",               "2337780"),
        ("076283_RADY_WWP_Qty",                         "076283"),
        ("Foodbanks/DD4769/Wounded Warrior/Qty",        "4769"),     # DD is 2 letters
    ):
        check(f"unchanged: {label[:40]!r}", _ship_label_po(label), want)


def test_label_with_no_number_yields_nothing_so_the_caller_falls_back():
    for label in ("911 Membership Offer/Sept1/Wounded Warrior/",
                  "WWP f/Society St Vincent de Paul/"):
        check(f"no PO in {label[:32]!r}", _ship_label_po(label), "")


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
