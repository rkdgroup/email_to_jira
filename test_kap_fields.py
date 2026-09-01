"""
KAP field-extraction regression tests. No network, no Jira, no PDFs.

    python test_kap_fields.py      # standalone, prints PASS / ALL PASSED
    pytest test_kap_fields.py      # also works

Guards the four faults found on DSLF-1069 (DL984) and DSLF-1070 (DL995):

  1. The broker order # was matched as a single token, so "CRU 924-105" — a real value
     with a space in it — left Mailer PO blank and the ticket was created without it.
  2. The select criteria scan treated any line opening with "$" as the price column, so
     "$10+ LAST 12 MO" was read as a price and the whole selection was dropped. Three
     tickets lost their selects this way (DSLF-1053, -1070, -1071).
  3. An omit phrased without the word "omit" ("Please exclude states MN, MS, and NC")
     reached neither prose field.
  4. An FTP order that names no address in its Ship To block states the destination in
     prose ("upload file to: https://..."). That destination was dropped, leaving the
     ticket with a notify address and nowhere to send the file.

Both fixtures are the real orders, trimmed to the blocks the parser reads.
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


# DL984 / DSLF-1069 — email order, single-token broker order #, "Please exclude" omit.
_DL984 = """Order Date:
KAP Order:
9943  RW
DL984
08/17/26
www.keyacquisitionpartners.com  Fed. ID # 27-4025351
Purchase Order
List rental
Mailer:
Offer:
Key:
Category:
KIDS WISH NETWORK
FUNDRAISING
Broker order:
Wanted By:
1634 SE 47TH  ST., UNIT 9
CAPE CORAL, FL  33904
 Email: CWHITNEY@VERADATA.COM
 Contact:COLLEEN WHITNEY  CWHITNEY@VERADATA.COM
Mail Date:
VERA DATA
222889-CW
08/19/26
11/17/26
Broker:
List:
Price:
AID FOR STARVING CHILDREN
0-12 $10+ OMIT MN MS NC
$ 95.00 /M
Net Arrangement:
100%
Selects:
Rental Qty:
32,422
All available
Material:
Price:
Ship To:
Via:
Contact:
Email
$100.00
EMAIL
Email
dataservices@innovairre.com
Omit all APO, FPO, Foreign addresses DMAs panders
Please exclude states MN, MS, and NC
Please contact Robin Wojack at Email: rwojack@keyacquisition.com
"""

# DL995 / DSLF-1070 — FTP order, spaced broker order #, "$"-prefixed select, upload URL.
_DL995 = """Order Date:
KAP Order:
9943  RW
DL995
08/18/26
www.keyacquisitionpartners.com  Fed. ID # 27-4025351
Purchase Order
List rental
Mailer:
Offer:
Key:
Category:
CRU INNER CITY (FKA HERE'S LIFE INNER CITY)
FUNDRAISING
Broker order:
Wanted By:
113 E. MARKET STREET
LEESBURG, VA  20176
 Email: KVONKLEECK@RMLC.NET
 Contact:KAREN VON KLEECK  KVONKLEECK@RMLC.NET
Mail Date:
RMLC-ROBERTSON MAILING LIST CO
CRU 924-105
08/19/26
10/05/26
Broker:
List:
Price:
AID FOR STARVING CHILDREN
$10+ LAST 12 MO
$ 95.00 /M
Net Arrangement:
100%
Selects:
Rental Qty:
5,000
Nth select
Material:
Price:
Ship To:
Via:
Contact:
FTP
$100.00
FTP
FTP
Attn:
omit DMA panders, prison, deceased
upload file to:
https://ws1.lortondata.com/FileTransfer/UploadForm.aspx
Please contact Robin Wojack at Email: rwojack@keyacquisition.com
"""


def test_broker_order_number_with_a_space():
    """DSLF-1070: "CRU 924-105" is the Broker order # and was left blank."""
    r = PARSER_REGISTRY["kap"].parse(_DL995)
    check("spaced broker order # becomes Mailer PO", r.mailer_po, "CRU 924-105")


def test_single_token_broker_order_number_unchanged():
    """The 19 other KAP orders on file all carry a single-token order #."""
    r = PARSER_REGISTRY["kap"].parse(_DL984)
    check("single-token broker order # still read", r.mailer_po, "222889-CW")


def test_address_line_is_not_mistaken_for_an_order_number():
    """Allowing spaces must not let a prose or address line through."""
    r = PARSER_REGISTRY["kap"].parse(
        _DL984.replace("VERA DATA\n", "VERA DATA\n1634 SE 47TH  ST., UNIT 9\n"))
    check("address line rejected as order #", r.mailer_po, "222889-CW")


def test_select_criteria_may_open_with_a_dollar_amount():
    """DSLF-1070: "$10+ LAST 12 MO" was read as the price column and dropped."""
    r = PARSER_REGISTRY["kap"].parse(_DL995)
    check("dollar-prefixed select kept", r.segment_criteria, "$10+ LAST 12 MO")


def test_price_line_still_stops_the_select_scan():
    r = PARSER_REGISTRY["kap"].parse(_DL995.replace("$10+ LAST 12 MO\n", ""))
    check("bare price is not a select", r.segment_criteria, "")


def test_selection_line_unchanged_on_the_email_order():
    r = PARSER_REGISTRY["kap"].parse(_DL984)
    check("plain select unchanged", r.segment_criteria, "0-12 $10+ OMIT MN MS NC")


def test_exclude_phrasing_reaches_the_omission():
    """DSLF-1069: an omit that never says "omit" was reaching neither field."""
    r = PARSER_REGISTRY["kap"].parse(_DL984)
    check("'Please exclude' captured as an omit",
          r.omission_description,
          "Omit all APO, FPO, Foreign addresses DMAs panders\n"
          "Please exclude states MN, MS, and NC")
    check("three states stay under the State Omits threshold", r.other_fees, "")


def test_upload_destination_reaches_shipping_instructions():
    """DSLF-1070: the only destination the order gives is a URL in prose."""
    r = PARSER_REGISTRY["kap"].parse(_DL995)
    check("upload target appended to the cc line",
          r.shipping_instructions,
          "CC: rwojack@keyacquisition.com | "
          "UPLOAD TO: https://ws1.lortondata.com/FileTransfer/UploadForm.aspx")
    check("delivery still read from the Ship To block", r.shipping_method, "FTP")


def test_order_without_an_upload_target_is_untouched():
    r = PARSER_REGISTRY["kap"].parse(_DL984)
    check("cc-only shipping instructions unchanged",
          r.shipping_instructions, "CC: rwojack@keyacquisition.com")


# The quantity block on an exchange, as DL997 prints it: a different label from a rental,
# a price line ahead of the number, and the KAP boilerplate further down the page.
_EXCHANGE_QTY = """Exch Qty:
Price:
$0.00/M
5,000
Nth select
Material:
Price:
Ship To:
Via:
Contact:
FTP
$100.00
Please provide the all available quantity before shipping for approval.
"""


def test_exchange_quantity_label():
    """DSLF-1071: only "Rental Qty:" was read, so every exchange lost its quantity."""
    r = PARSER_REGISTRY["kap"].parse(_DL995.replace(
        "Selects:\nRental Qty:\n5,000\nNth select\n", "Selects:\n" + _EXCHANGE_QTY))
    check("exchange quantity read", r.requested_quantity, 5000)


def test_boilerplate_does_not_flip_nth_to_all_available():
    """The order asks for an Nth; the page merely offers an all-available count."""
    r = PARSER_REGISTRY["kap"].parse(_DL995.replace(
        "Selects:\nRental Qty:\n5,000\nNth select\n", "Selects:\n" + _EXCHANGE_QTY))
    check("Nth survives the all-available boilerplate", r.availability_rule, "Nth")


def test_price_in_the_window_is_not_the_quantity():
    check("price line not read as quantity",
          PARSER_REGISTRY["kap"].parse(_DL995.replace(
              "Selects:\nRental Qty:\n5,000\nNth select\n", "Selects:\n" + _EXCHANGE_QTY)
          ).requested_quantity, 5000)


def test_all_available_still_read_from_its_own_block():
    r = PARSER_REGISTRY["kap"].parse(_DL984)
    check("rental quantity still read", r.requested_quantity, 32422)
    check("All Available still read", r.availability_rule, "All Available")


def test_nth_rental_is_not_flipped_either():
    r = PARSER_REGISTRY["kap"].parse(_DL995)
    check("rental Nth unchanged", r.availability_rule, "Nth")
    check("rental quantity unchanged", r.requested_quantity, 5000)


def test_title_follows_the_list_mailer_order_rule():
    """The parser set summary itself, which suppressed ParseResult's auto-built title.

    64 of 67 KAP tickets were created as "P.O. {DL#} {list name}" — no mailer, wrong
    shape, and the manager order # in the wrong place (DSLF-1078, -1079 and back).
    Leaving summary unset is what every other broker does.
    """
    r = PARSER_REGISTRY["kap"].parse(_DL984)
    check("email order titled LIST - MAILER - MGR ORDER",
          r.summary, "AID FOR STARVING CHILDREN - KIDS WISH NETWORK - DL984")


def test_title_on_the_ftp_order_too():
    r = PARSER_REGISTRY["kap"].parse(_DL995)
    check("FTP order titled LIST - MAILER - MGR ORDER",
          r.summary,
          "AID FOR STARVING CHILDREN - "
          "CRU INNER CITY (FKA HERE'S LIFE INNER CITY) - DL995")


def test_title_never_carries_the_mailer_po():
    """The order # in the title is the Manager Order # (DL995), never the Mailer PO."""
    r = PARSER_REGISTRY["kap"].parse(_DL995)
    check("mailer PO absent from the title", r.mailer_po in r.summary, False)
    check("mailer PO still populated", r.mailer_po, "CRU 924-105")


def test_kap_order_may_use_a_dm_prefix():
    """DL and DM are the same slot under "KAP Order:", so both must be read.

    A DL-only regex left DSLF-1092 (DM009) and DSLF-1100 (DM022) with a blank Manager
    Order #, which then put the Mailer PO in the title via ParseResult's fallback and
    left Seed Tracking Number empty. Checked against 69 KAP orders: only DL and DM
    occur, and first-match still equals the Manager Order # on all 67 that had one.
    """
    r = PARSER_REGISTRY["kap"].parse(_DL984.replace("DL984", "DM022"))
    check("DM-prefixed KAP order read", r.manager_order_number, "DM022")
    check("DM order titled LIST - MAILER - MGR ORDER",
          r.summary, "AID FOR STARVING CHILDREN - KIDS WISH NETWORK - DM022")


# DSLF-1152: the destination sits ~15 lines below the "Ship To:" label, in prose, while the
# broker rep's address is printed near the TOP of the page. _ship_block is capped at 260
# chars so it did not reach the prose, and the last-resort fallback searched the WHOLE page
# and took the rep. Only the Ship To block decides the destination.
_DL_RKD = """Order Date:
KAP Order:
9943  RW
DM054
08/29/26
Purchase Order
List rental
Mailer:
MARINE TOYS FOR TOTS
Broker order:
Wanted By:
 Email: BCRABTREE@RKDGROUP.COM
 Contact:BRENDA CRABTREE  BCRABTREE@RKDGROUP.COM
Mail Date:
RKD GROUP
132700
08/30/26
10/06/26
Broker:
List:
Price:
AID FOR STARVING CHILDREN
$10 + L18 MO
$ 95.00 /M
Rental Qty:
5,000
Nth select
Material:
Price:
Ship To:
Via:
Contact:
FTP
$100.00
FTP
Attn: DATA-MANAGEMENT.COM
Please note these will ship to DMI.
Go to:  www.data-management.com
Once file has posted, send an email to  tlibrarian@data-management.com  and  bcrabtree@rkdgroup.com
contact Robin Wojack at Email: rwojack@keyacquisition.com.
"""


def test_the_broker_rep_above_the_ship_to_block_is_not_the_destination():
    """DSLF-1152 shipped to BCRABTREE@RKDGROUP.COM, the broker's own rep."""
    r = PARSER_REGISTRY["kap"].parse(_DL_RKD)
    check("destination is the DMI drop point",
          r.ship_to_email, "FTP NOTIFY: tlibrarian@data-management.com")
    check("the broker rep is not the destination",
          "RKDGROUP" in r.ship_to_email.upper(), False)


def test_send_an_email_to_x_and_y_takes_the_first():
    """The second address after "and" is the broker being copied, not the destination."""
    r = PARSER_REGISTRY["kap"].parse(_DL_RKD)
    check("first address wins", "tlibrarian@data-management.com" in r.ship_to_email, True)
    check("second address excluded", "bcrabtree" in r.ship_to_email.lower(), False)


def test_requestor_name_survives_all_three_phrasings():
    """Please...at (DL984), Please...@ (DSLF-1152), and no-Please...at (DSLF-1141)."""
    for phrase, tag in (
        ("Please contact Robin Wojack at Email: rwojack@keyacquisition.com", "Please + at"),
        ("Please contact Robin Wojack @ Email:  rwojack@keyacquisition.com", "Please + @"),
        ("contact Robin Wojack at Email: rwojack@keyacquisition.com.",       "no Please"),
    ):
        r = PARSER_REGISTRY["kap"].parse(_DL_RKD.replace(
            "contact Robin Wojack at Email: rwojack@keyacquisition.com.", phrase))
        check(f"requestor name read [{tag}]", r.requestor_name, "Robin Wojack")
        check(f"requestor email read [{tag}]", r.requestor_email,
              "rwojack@keyacquisition.com")


def test_a_non_kap_address_is_never_the_requestor():
    """Dropping the required "Please" widened the pattern, so the captured address is
    pinned to KAP's domain. DSLF-1141 also carries "contact eftaccountsetup@igxfer.com"."""
    r = PARSER_REGISTRY["kap"].parse(_DL_RKD.replace(
        "contact Robin Wojack at Email: rwojack@keyacquisition.com.",
        "To set up your FTP login, contact eftaccountsetup@igxfer.com for help."))
    check("igxfer setup address is not the requestor",
          "igxfer" in (r.requestor_email or "").lower(), False)
    check("falls back to the KAP house requestor", r.requestor_email,
          "jgomez@keyacquisition.com")


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
