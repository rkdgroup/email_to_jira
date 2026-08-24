"""
SELECT-PDF shipping-info regression tests. No network, no Jira, no PDFs.

    python test_qc_select_parse.py      # standalone, prints PASS / ALL PASSED
    pytest test_qc_select_parse.py      # also works

DSLF-1070's SELECT states its destination plainly — "FILENAME: CRU 924-105.ZIP" in the
NOTES block — but the scan matched the filename as one whitespace-free token, so a value
carrying the Mailer PO's own space was missed. QC then reported "Shipping info not found"
and skipped the shipping cross-checks. It is a WARN, so the ticket still passed 10/10 and
nothing in the report said a check had been skipped.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from qc_checker import _parse_shipping_info, _desc_has_dollar, _desc_has_period

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


# The NOTES block as it prints on DSLF-1070's SELECT.
_FTP_NOTES = """------------------------ N O T E S ------------------------
AID FOR STARVING CHILDREN
FILENAME: CRU 924-105.ZIP
"""

_EMAIL_NOTES = """------------------------ N O T E S ------------------------
TO: dataservices@innovairre.com
CC: rwojack@keyacquisition.com
"""


def test_filename_with_a_space():
    r = _parse_shipping_info(_FTP_NOTES)
    check("spaced filename found", r["ftp_filename"], "CRU 924-105.ZIP")
    check("spaced filename means FTP", r["shipping_method"], "FTP")
    check("no parse error on a filename QC can read", r["parse_errors"], [])


def test_single_token_filename_still_read():
    r = _parse_shipping_info("FILENAME: W466013835.ZIP\n")
    check("single-token filename still read", r["ftp_filename"], "W466013835.ZIP")


def test_email_notes_take_precedence():
    r = _parse_shipping_info(_EMAIL_NOTES)
    check("TO: address read", r["ship_to_email"], "DATASERVICES@INNOVAIRRE.COM")
    check("CC: address read", r["cc_email"], "RWOJACK@KEYACQUISITION.COM")
    check("email order carries no ftp filename", r["ftp_filename"], "")
    check("email order is Email", r["shipping_method"], "Email")


def test_bare_label_does_not_reach_down_the_page():
    """A FILENAME: label with no value must not borrow a .ZIP from a later line."""
    r = _parse_shipping_info("FILENAME:\nSOME OTHER LINE\nARCHIVE.ZIP\n")
    check("bare label finds nothing", r["ftp_filename"], "")
    check("bare label reports the miss", r["parse_errors"],
          ["Shipping info (TO:/CC:/FILENAME:) not found in SELECT PDF"])


def test_missing_shipping_info_still_reported():
    r = _parse_shipping_info("NOTHING USEFUL HERE\n")
    check("missing shipping info still reported", r["parse_errors"],
          ["Shipping info (TO:/CC:/FILENAME:) not found in SELECT PDF"])
    check("missing shipping info leaves method blank", r["shipping_method"], "")


# A SELECT prints the dollar band with the client's cap applied ("$10-99.99") while the
# order is written open-ended ("$10+"). Same ask: the cap is a per-client profile term, and
# 60 of 195 clients cap at $99.99. The Selection Criteria check used to inline its own
# regex instead of calling _desc_has_dollar, and that copy matched a SELECT range only
# literally — so it failed every capped order. Five of six KAP tickets measured on
# 2026-08-24 (DSLF-1079, -1078, -1071, -1070, -1069) failed this way, all correct pulls.

def test_capped_band_is_satisfied_by_an_open_threshold():
    check("$10-99.99 satisfied by '$10+ L12'",
          _desc_has_dollar("$10+ L12", "$10-99.99"), True)
    check("$0.01-99.99 satisfied by '$0.01+'",
          _desc_has_dollar("$0.01+ 12 MONTH", "$0.01-99.99"), True)


def test_capped_band_still_matches_when_written_in_full():
    check("explicit range still matches",
          _desc_has_dollar("12 MONTH $10-$99.99", "$10-99.99"), True)


def test_a_different_floor_is_not_satisfied():
    """The fix must not make the check unfalsifiable — a wrong floor still fails."""
    check("$25 floor does not satisfy a $10 band",
          _desc_has_dollar("$25+ L12", "$10-99.99"), False)
    check("$5+ is not satisfied by $50+",
          _desc_has_dollar("$50+ DONORS", "$5+"), False)
    check("$5+ is not satisfied by $15+",
          _desc_has_dollar("$15+ DONORS", "$5+"), False)


def test_period_helper_accepts_the_abbreviations_the_orders_use():
    for desc, tok, want, name in (
        ("3M $5+",            "L3M",  True,  "3M"),
        ("3 MOS $5+",         "L3M",  True,  "3 MOS"),
        ("12 MONTH $10+",     "L12M", True,  "12 MONTH"),
        ("$10+ L12",          "L12M", False, "bare L12 (no trailing M)"),
        ("13M $5+",           "L3M",  False, "13M must not satisfy L3M"),
    ):
        check(f"period {name}", _desc_has_period(desc, tok), want)


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
