"""
Qty-approval subject codes and email body. No network, no Jira, no PDFs.

    python test_qty_subject_and_body.py      # standalone, prints PASS / ALL PASSED
    pytest test_qty_subject_and_body.py      # also works

Two rules, both requested by Suvam on 2026-09-11:

  1. EVERY email's subject carries the name as an abbreviation — the LIST name for a
     single ticket, the MAILER name for a group. Both resolvers used to return "" when
     the name was not in dslf_list_and_mailer_names.txt, and the subject then came out as
     a bare "QTY APPROVAL/J2113/J2114" with nothing saying whose orders those were. The
     file is a 2026-06-17 snapshot, so any mailer newer than it fell through the hole —
     "SISTERS OF MARY - WORLD VILLAGES FOR CHILDREN" was sitting in the live queue doing
     exactly that.

  2. The body names the requestor.

_derive_acronym is checked against the names file's OWN derived entries, the ones it
marks with `*` ("derived acronym from initials"). That is the point of the function: a
mailer absent from the file today must get the same code the file would have given it, so
the code does not change under the recipient when the file is next regenerated.
"""

import sys
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import qty_approval_scanner as q

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


def _t(order, mailer="", list_name="", qty=None, email=""):
    return {"manager_order": order, "mailer_name": mailer, "list_name": list_name,
            "req_qty": qty, "requestor_email": email}


# ---------------------------------------------------------------------------
# 1. The derived acronym reproduces the names file's own convention
# ---------------------------------------------------------------------------

def test_derivation_matches_the_names_file():
    """The file's `*` rows are derived acronyms; regenerate them and compare."""
    txt = (Path(__file__).parent / "dslf_list_and_mailer_names.txt").read_text(encoding="utf-8")
    pairs = re.findall(r'^\s{2}(.+?)\s{2,}\(\*([^)]+)\)\s*$', txt, re.MULTILINE)
    check("the names file still has its derived entries", len(pairs) > 100, True)
    misses = [(n, c) for n, c in pairs if q._derive_acronym(n) != c]
    # The four known misses are two dollar-amount rows that are not really names and two
    # "U.S." initialisms the file collapses further. Anything beyond that is a regression.
    check("derivation reproduces the file", len(misses) <= 4, True)


def test_stopwords_and_company_suffixes_are_dropped():
    check("FOR dropped", q._derive_acronym("AMERICAN ACTION FUND FOR BLIND"), "AAFB")
    check("OF dropped", q._derive_acronym("BAYLOR COLLEGE OF MEDICINE"), "BCM")
    check("INC dropped", q._derive_acronym("HELP ME SEE INC"), "HMS")
    check("CO dropped", q._derive_acronym("LITTLETON COIN CO"), "LC")


def test_digits_apostrophes_and_parentheses():
    check("a leading number is its own token",
          q._derive_acronym("21st Century Conservative Donors"), "21CCD")
    check("an apostrophe does not split a word",
          q._derive_acronym("NATIONAL SHERIFF'S ASSOCIATION"), "NSA")
    # Live mailer names carry these: "Guiding Light Mission Inc (#2310077)".
    check("a parenthesised account number is dropped",
          q._derive_acronym("Guiding Light Mission Inc (#2310077)"), "GLM")


# ---------------------------------------------------------------------------
# 2. Every subject carries a code
# ---------------------------------------------------------------------------

def test_single_ticket_uses_the_list_name():
    s = q._subject_for("MERCY HOMES FOR BOYS/GIRL",
                       [], [_t("J2113", "MERCY HOMES FOR BOYS/GIRL",
                               "3-ACF ABANDONED CHILDRENS FUND")], "default")
    check("individual subject leads with the list code", s, "ACF/QTY APPROVAL/J2113")


def test_group_uses_the_mailer_name():
    ts = [_t("J2113", "HERITAGE FOUNDATION", "3-ACF X"),
          _t("J2115", "HERITAGE FOUNDATION", "3-ACF X")]
    s = q._subject_for("HERITAGE FOUNDATION", [], ts, "default")
    check("group subject leads with the mailer code", s.split("/")[0], "HF")


def test_a_mailer_missing_from_the_names_file_still_gets_a_code():
    """The live regression: this mailer is newer than the 2026-06-17 snapshot."""
    mailer = "SISTERS OF MARY - WORLD VILLAGES FOR CHILDREN"
    check("not in the names file", mailer.upper() in q._MAILER_ABBREVS, False)
    s = q._subject_for(mailer, [], [_t("J2113", mailer), _t("J2114", mailer)], "default")
    # Sisters / Mary / World / Villages / Children — OF and FOR are stopwords.
    check("the subject still names the mailer", s, "SMWVC/QTY APPROVAL/J2113-J2114")
    check("never a bare QTY APPROVAL", s.startswith("QTY APPROVAL"), False)


def test_a_list_missing_from_the_names_file_still_gets_a_code():
    s = q._subject_for("X", [], [_t("J9001", "X", "Center For Orphan Relief")], "default")
    check("list code derived when unlisted", s, "COR/QTY APPROVAL/J9001")


def test_blank_name_is_the_only_way_to_lose_the_code():
    """The --combined digest covers every mailer, so no single code describes it."""
    s = q._subject_for("", [], [_t("J1"), _t("J7")], "default")
    check("combined digest has no prefix", s, "QTY APPROVAL/J1/J7")


# ---------------------------------------------------------------------------
# 3. The body names the requestor
# ---------------------------------------------------------------------------

def test_body_ends_with_the_requestor_email():
    body = q.build_mailer_report(
        "ADSTRA",
        [{"manager_order": "J2044", "approved_qty": 3570,
          "requestor_email": "BOBBI.DURRETT@ADSTRADATA.COM"}],
        [_t("J2328", qty=5816, email="BOBBI.DURRETT@ADSTRADATA.COM")])
    check("body carries the requestor", body,
          "J2044 = 3,570\nJ2328 = 5,816\n\nRequestor: BOBBI.DURRETT@ADSTRADATA.COM")


def test_the_order_equals_qty_lines_are_untouched():
    """scan_approval_emails parses the reply with this exact shape — it must not move."""
    body = q.build_mailer_report("M", [], [_t("J2044", qty=3570, email="a@b.com")])
    first = body.splitlines()[0]
    check("first line is still '<order> = <qty>'", first, "J2044 = 3,570")
    check("the reply parser still finds it",
          bool(re.search(r'\bJ2044\s*=\s*([\d,]+)', body)), True)


def test_duplicate_requestors_collapse_and_blanks_are_skipped():
    body = q.build_mailer_report("M", [], [_t("J1", qty=1, email="a@b.com"),
                                           _t("J2", qty=2, email="a@b.com"),
                                           _t("J3", qty=3, email="")])
    check("one requestor listed once", body.count("a@b.com"), 1)
    body2 = q.build_mailer_report("M", [], [_t("J1", qty=1), _t("J2", qty=2)])
    check("no requestor line when none is known", "Requestor" in body2, False)


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
