"""
ADSTRA list-code extraction tests. No network, no Jira, no PDFs.

    python test_adstra_list_code.py      # standalone, prints PASS / ALL PASSED
    pytest test_adstra_list_code.py      # also works

The 5-digit list code is enrich_fields' tier-0 key — an exact match against adstra.yaml,
and the only lookup on an ADSTRA order that cannot pick the wrong client. The parser read
it from the list NAME line only, but ADSTRA prints it on its own line directly below the
name about as often, so the key went missing on 11 of the 46 ADSTRA orders on file and the
lookup fell through to fuzzy name matching.

Two tickets were created on the wrong client database that way, both since completed:
  DSLF-1016  order "3-NPTA-NAT POLICE / TROOPER AS (00564)" -> N24D (NBLPF), should be N13D
  DSLF-1030  order "3-SAVE SURVIVORS & VICTIMS EMP (00546)" -> S32D (SAVE Mission
             Recovery, code 00545), should be S30D — the two share the "SAVE" token
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


def _order(list_block: str) -> str:
    """An ADSTRA order trimmed to what the parser reads, with the List block swapped in."""
    return f"""Purchase Order
      750 College Road East, Princeton NJ 08540
Order Date:
08/13/26
Mailer:
PAWS OF HONOR
Adstra order#:
J3742  RENTAL
Ship By:
08/17/26
Mail Date:
10/29/26
Broker:
VERADATA
Broker PO:
223062
List:
{list_block}
Price
$75.00/M
Quantity:
3,500 OR ALL AVAILABLE
Ship-To:
VERADATA
VIA:
E-MAIL
ATTN:
DATA@VERADATA.COM
Contact:
MATTHEW DOTSON
MATTHEW.DOTSON@ADSTRADATA.COM
"""


def test_code_on_its_own_line():
    """The DSLF-1016 layout: name, then the code underneath."""
    r = PARSER_REGISTRY["adstra"].parse(_order("3-NPTA-NAT POLICE / TROOPER AS\n(00564)"))
    check("code read from the line below the name", r.adstra_list_code, "00564")
    check("list name has no code in it", r.list_name, "3-NPTA-NAT POLICE / TROOPER AS")


def test_code_on_the_name_line():
    r = PARSER_REGISTRY["adstra"].parse(_order("NLEOMF DONORS (49210)"))
    check("code read from the name line", r.adstra_list_code, "49210")
    check("inline code stripped from the name", r.list_name, "NLEOMF DONORS")


def test_order_with_no_code():
    r = PARSER_REGISTRY["adstra"].parse(_order("BFF- BRIGHTFOCUS FDN MF (DMI)"))
    check("no code found when the order prints none", r.adstra_list_code, "")
    check("parenthesised non-code left alone", r.list_name, "BFF- BRIGHTFOCUS FDN MF (DMI)")


def test_zip_in_the_block_is_not_a_list_code():
    """A bare 5-digit number is not a code — only a parenthesised one is."""
    r = PARSER_REGISTRY["adstra"].parse(
        _order("3-FCF FIREFIGHTERS CHARIT FND\nMANASSAS VA 20110\n(00532)"))
    check("address digits ignored, real code found", r.adstra_list_code, "00532")


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
