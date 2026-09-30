"""
CELCO field-extraction regression tests. No network, no Jira, no PDFs.

    python test_celco_fields.py    # standalone, prints PASS / ALL PASSED
    pytest test_celco_fields.py    # also works

Guards the four faults QC found on DSLF-1366 (D04-086888-NI), CELCO's first live orders:

  1. The SEGMENT value wraps onto a second line ("... SCFS 940-941," / "943-947, 949 ONLY")
     and only the first was read, so the ticket selected two SCFs out of eight.
  2. The CONTACT AT name was found and then never used, leaving Requestor Name blank.
  3. "PLEASE SEND CONFIRMATION TO: incoming.files@..." has a colon after TO, which the
     pattern did not allow, so an FTP order went out with no destination at all.
  4. The KEYCODE box is blank; the key code is stated in the MARK FILE line instead
     ("Key Code '0845" - the apostrophe is the spreadsheet leading-zero marker).

_D04_086888 is the real order as PyMuPDF extracts it, legal boilerplate trimmed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from parsers import PARSER_REGISTRY, detect_broker

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


_D04_086888 = """ORDER #
D04-086888-NI
DATE
09/30/26
DB60845
CLIENT REF
CONTACT
AT
Page 1 of 1
LIST EXCHANGE ORDER
(703) 426-4415
Sara Ghods
sara@celcononprofit.com
CLIENT
NEXUS DIRECT
780 LYNNHAVEN PARKWAY
ALISON LASKOS
VIRGINIA BEACH, VA 23452
USER
GLIDE MEMORIAL - CLIENT ID
FUNDRAISING
OFFER
MAIL DATE
11/13/26
PHONE:(757) 340-5960 FAX:(757) 340-5980
SUITE 400
WANTED BY
10/02/26
IMPORTANT: WE WILL ASSUME PRICING AND OTHER TERMS ON
will CELCO be liable for any consequential, indirect, exemplary, special, or
incidental damages arising from or relating to the usage of the list.
PROJECT OPEN HAND
LIST
SEGMENT
$5+ 24 MONTH DONORS IN SCFS 940-941,
943-947, 949 ONLY
FORMAT
FTP
KEYCODE
Mailer (CLIENT/USER) may retain mailable information for the purposes of:
retained nor shared.
OMIT: CANADA, PUERTO RICO, FOREIGN AND MILITARY NAMES.
NOTIFY US  OF CHANGES IN QUANTITY, PRICE, DELIVERY ASAP
1,923
M
/
$ 0.00
EXCHANGE
ALL AVAILABLE
FTP
SHIP VIA
**CONTINUATION**
NOT APPLICABLE
SHIP TO
,
MARK ALL PACKAGES WITH LIST NAME, QUANTITY AND CELCO ORDER # & MAILER'S NAMES.
IMPORTANT!!!  CALL SARA @ CELCO AT ONCE IF THE QUANTITY SHOULD VARY + OR - 10% OR IF THE RETURN
DATE CANNOT BE MET.
PLEASE POST FILE TO:
https://nexusdirect.sharefile.com/r-rb12a71eb67094b40a717d7ab6d2a4565
PLEASE SEND CONFIRMATION TO: incoming.files@nexusdirect.com
PLEASE MARK FILE AS:  PO#, Mailer, List, Segment, Key Code '0845, Quantity
*PLEASE ADVISE OF QTY BEFORE SHIPPING*
"""

# Same order with a one-line segment and a filled KEYCODE box: the box must still win.
_ONE_LINE = (_D04_086888
             .replace("$5+ 24 MONTH DONORS IN SCFS 940-941,\n943-947, 949 ONLY\n",
                      "$10+ 12 MONTH DONORS\n")
             .replace("KEYCODE\nMailer", "KEYCODE\nGM1026\nMailer"))


# D04-086371-NI / D04-086791-NI tail: an EMAIL order whose "SEND FILE TO:" is a mailbox.
_EMAIL_TAIL = (_D04_086888
               .replace("FTP\nSHIP VIA", "EMAIL\nSHIP VIA")
               .replace("PLEASE POST FILE TO:\n"
                        "https://nexusdirect.sharefile.com/r-rb12a71eb67094b40a717d7ab6d2a4565\n"
                        "PLEASE SEND CONFIRMATION TO: incoming.files@nexusdirect.com\n",
                        "PLEASE SEND FILE TO: tlibrarian@data-management.com AND SEND SHIPMENT\n"
                        "NOTIFICATION TO apiper@data-management.com\n"))


def _parse(text=_D04_086888):
    return PARSER_REGISTRY["celco"].parse(text)


def test_detected_as_celco():
    m = detect_broker(_D04_086888)
    check("fingerprint picks celco", m.broker_key if m else None, "celco")


def test_wrapped_segment_is_joined():
    check("segment keeps its second line", _parse().segment_criteria,
          "$5+ 24 MONTH DONORS IN SCFS 940-941, 943-947, 949 ONLY")


def test_requestor_is_the_contact():
    r = _parse()
    check("requestor name from CONTACT AT", r.requestor_name, "Sara Ghods")
    check("requestor email from CONTACT AT", r.requestor_email, "sara@celcononprofit.com")


def test_confirmation_mailbox_is_the_ftp_notify_address():
    check("confirmation address with a colon", _parse().ship_to_email,
          "FTP NOTIFY: incoming.files@nexusdirect.com")


def test_post_link_reaches_shipping_instructions():
    check("upload link appended to the cc line", _parse().shipping_instructions,
          "CC: sara@celcononprofit.com | UPLOAD TO: "
          "https://nexusdirect.sharefile.com/r-rb12a71eb67094b40a717d7ab6d2a4565")


def test_key_code_from_the_mark_file_line():
    check("key code without its leading apostrophe", _parse().key_code, "0845")


def test_fields_that_were_already_right():
    r = _parse()
    check("manager order", r.manager_order_number, "D04-086888-NI")
    check("mailer po = order #", r.mailer_po, "D04-086888-NI")
    check("list name", r.list_name, "PROJECT OPEN HAND")
    check("mailer", r.mailer_name, "GLIDE MEMORIAL - CLIENT ID")
    check("quantity", r.requested_quantity, 1923)
    check("availability", r.availability_rule, "All Available")
    check("shipping method", r.shipping_method, "FTP")
    check("mail date", r.mail_date, "2026-11-13")
    check("wanted by", r.ship_by_date, "2026-10-02")
    check("omission", r.omission_description, "CANADA, PUERTO RICO, FOREIGN AND MILITARY NAMES.")


def test_one_line_segment_stops_at_the_next_label():
    check("one-line segment untouched", _parse(_ONE_LINE).segment_criteria, "$10+ 12 MONTH DONORS")


def test_keycode_box_beats_the_mark_file_line():
    check("KEYCODE box value wins", _parse(_ONE_LINE).key_code, "GM1026")


def test_send_file_to_a_mailbox_is_not_an_upload_link():
    r = _parse(_EMAIL_TAIL)
    check("mailbox is the destination", r.ship_to_email, "tlibrarian@data-management.com")
    check("no UPLOAD TO for a mailbox", r.shipping_instructions, "CC: sara@celcononprofit.com")


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
