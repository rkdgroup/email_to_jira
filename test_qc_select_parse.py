"""
SELECT-PDF shipping-info regression tests. No network, no Jira, no PDFs.

    python test_qc_select_parse.py      # standalone, prints PASS / ALL PASSED
    pytest test_qc_select_parse.py      # also works

Only _parse_shipping_info survives here. The dollar-band and time-period comparison
helpers this file also used to pin (_desc_has_dollar / _desc_has_period) were part of the
rule-based QC checker and went with it — the knowledge they encoded, that a SELECT reading
"$10-99.99" satisfies an order written "$10+" because $99.99 is that client's contracted
cap, is now stated in qc_llm's SELECT prompt under DOLLAR BANDS. A prompt cannot be pinned
with an assert; the rule is asserted instead by test_qc_llm_verdict, which checks the
prompt still carries it.

DSLF-1070's SELECT states its destination plainly — "FILENAME: CRU 924-105.ZIP" in the
NOTES block — but the scan matched the filename as one whitespace-free token, so a value
carrying the Mailer PO's own space was missed. QC then reported "Shipping info not found"
and skipped the shipping cross-checks. It is a WARN, so the ticket still passed 10/10 and
nothing in the report said a check had been skipped.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from select_pdf import _parse_shipping_info

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
