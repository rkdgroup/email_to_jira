"""
Ship-to / delivery regression tests. No network, no Jira, no PDFs.

    python test_ship_to_rules.py      # standalone, prints PASS / ALL PASSED
    pytest test_ship_to_rules.py      # also works

Guards the three faults behind DSLF-1022, where a plain email order to
mercy@mmidirect.com became an FTP + ASCII Fixed ticket:

  1. tools_jira matched the data-axle DOMAIN, so a broker rep's mailbox was treated
     as the incoming.files@ drop-box.
  2. parsers/kap.py took ship_to_email from the first page-wide "Email:", which is the
     mailer/broker contact rather than the destination.
  3. Every parser inferred FTP from a bare "FTP" anywhere on the page, including
     boilerplate offering FTP as a fallback if email fails.

Only the ship-to destination decides format and delivery. A broker, list manager or
contact from a processing house named elsewhere on the order is irrelevant.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from tools_jira import apply_ship_to_rules
from parsers import PARSER_REGISTRY

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


# ---------------------------------------------------------------------------
# 1. tools_jira ship-to house rules
# ---------------------------------------------------------------------------

def test_data_axle_staff_mailbox_is_not_a_dropbox():
    """The DSLF-1022 regression: a person at Data Axle must not force FTP."""
    check("data-axle staff mailbox left alone",
          apply_ship_to_rules("elizsa.leszczynski@data-axle.com", "", "Email"),
          ("elizsa.leszczynski@data-axle.com", "", "Email"))


def test_data_axle_dropbox_still_forces_ftp_fixed():
    check("data-axle drop-box forces FTP + ASCII Fixed",
          apply_ship_to_rules("incoming.files@data-axle.com", "", "Email"),
          ("FTP NOTIFY: incoming.files@data-axle.com", "ASCII Fixed", "FTP"))
    check("drop-box is not double-prefixed",
          apply_ship_to_rules("FTP NOTIFY: incoming.files@data-axle.com", "", "Email"),
          ("FTP NOTIFY: incoming.files@data-axle.com", "ASCII Fixed", "FTP"))


def test_fixed_format_house_stays_email():
    """These five are ASCII Fixed but emailed — only Saturn and Data Axle force FTP."""
    check("fixed-format house keeps Email delivery",
          apply_ship_to_rules("TapeLibrarian@directmail.com", "", "Email"),
          ("TapeLibrarian@directmail.com", "ASCII Fixed", "Email"))


def test_saturn_rules():
    check("saturn ship-to",
          apply_ship_to_rules("convert@saturncorp.com", "", "Email"),
          ("FTP NOTIFY: convert@saturncorp.com", "ASCII Fixed", "FTP"))
    check("saturn named only in the order body",
          apply_ship_to_rules("a@b.com", "", "Email", "PLACE ON SATURN'S FTP SITE"),
          ("FTP NOTIFY: a@b.com (SATURN CORP)", "ASCII Fixed", "FTP"))
    check("saturn body with no ship-to",
          apply_ship_to_rules("", "", "Email", "load the file to the SATURN fileshare"),
          ("PLACE ON SATURN CORP FTP FILESHARE", "ASCII Fixed", "FTP"))


def test_plain_order_untouched():
    check("plain email order untouched",
          apply_ship_to_rules("mercy@mmidirect.com", "", "Email"),
          ("mercy@mmidirect.com", "", "Email"))


# ---------------------------------------------------------------------------
# 2. Shared FTP-destination predicate (parsers/base.py)
# ---------------------------------------------------------------------------

_FALLBACK_PROSE = (
    "Please email files to cecilia@example.com. If you are unable to email records, "
    "please place on your FTP site and send retrieval instructions.",
    "Email the file. Otherwise post to our FTP.",
    "If you cannot email, use FTP.",
)
_REAL_FTP = (
    "Via: FTP",
    "Ship To: F.T.P.",
    "VIA: F.T.P.",
)

# KNOWN GAP, pre-existing and not introduced by the fallback-prose fix: an FTP upload
# described without the letters "FTP" is not detected by either the old bare-\bFTP\b scan
# or the current predicate. ADSTRA orders do this — "POST FILE TO: transfer.edatastax.com ;
# PORT: 22" is an FTP destination with no FTP token in it. Those orders get their delivery
# from the form's own Via field instead, so this only bites a parser falling back to the
# page. Left asserted so the gap stays visible rather than being quietly assumed covered.
_UNDETECTED_FTP = (
    "POST FILE TO: transfer.edatastax.com ; PORT: 22",
)


def test_ftp_fallback_prose_is_not_a_destination():
    p = PARSER_REGISTRY["kap"]
    for t in _FALLBACK_PROSE:
        check(f"fallback prose not FTP: {t[:38]!r}",
              p._text_mentions_ftp_destination(t), False)


def test_real_ftp_still_detected():
    p = PARSER_REGISTRY["kap"]
    for t in _REAL_FTP:
        check(f"real FTP detected: {t[:38]!r}",
              p._text_mentions_ftp_destination(t), True)


def test_known_gap_ftp_without_the_letters_ftp():
    """Documents a pre-existing miss — see the _UNDETECTED_FTP comment."""
    p = PARSER_REGISTRY["kap"]
    for t in _UNDETECTED_FTP:
        check(f"known gap, still undetected: {t[:34]!r}",
              p._text_mentions_ftp_destination(t), False)


# ---------------------------------------------------------------------------
# 3. KAP reads destination + delivery from the Ship To block
# ---------------------------------------------------------------------------

_KAP_ORDER = """Order Date:
KAP Order:
DL963
08/11/26
www.keyacquisitionpartners.com  Fed. ID # 27-4025351
Purchase Order
List rental
Mailer:
NATIVE AMER. HERIT. ASSOC
Broker order:
126853
 Email: hvanwyck@dmgroup.com
 Contact:Hawley Van Wyck  hvanwyck@dmgroup.com
Broker:
List:
Price:
AID FOR STARVING CHILDREN
12 MONTH $10-$99.99
Rental Qty:
6,000
All available
Material:
Price:
Ship To:
Via:
Contact:
Email
$85.00
EMAIL
Email
,
TapeLibrarian@directmail.com
Please provide the all available quantity before shipping for approval.
Please contact Jenny Gomez at Email: jgomez@keyacquisition.com
"""


def test_kap_takes_destination_not_contact():
    r = PARSER_REGISTRY["kap"].parse(_KAP_ORDER)
    check("KAP ship-to is the destination, not the mailer contact",
          r.ship_to_email, "TapeLibrarian@directmail.com")
    check("KAP delivery read from the Ship To block", r.shipping_method, "Email")


def test_kap_ignores_ftp_boilerplate():
    """The exact DSLF-1022 mechanism: fallback prose must not flip the order to FTP."""
    r = PARSER_REGISTRY["kap"].parse(
        _KAP_ORDER + "\nIf you are unable to email records, please place on your FTP site.\n")
    check("KAP stays Email despite FTP boilerplate", r.shipping_method, "Email")
    check("KAP ship-to unchanged by boilerplate",
          r.ship_to_email, "TapeLibrarian@directmail.com")


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
