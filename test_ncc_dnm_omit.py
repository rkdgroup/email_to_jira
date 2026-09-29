"""The NCC do-not-mail line is client-specific, not a standard ADSTRA omit.

`parsers/adstra.py` prepends a fixed STANDARD OMITS block to every ADSTRA order, and
`NCC DNM FILE FOR LIST RENTAL (W/O 222222)` is one of its lines. N11D does not suppress
against that file (Suvam, 2026-09-29), and 54 of its 55 tickets carried the line anyway.

The correction lives in `parse_pipeline`, not in the parser: the parser runs before
enrichment, so it has no db_code to test. Hermetic — no Jira, no PDFs, no network.
"""
import parse_pipeline as pp

_BLOCK = "\n".join([
    "STANDARD OMITS (F6)",
    "NO PERSONAL NAME",
    "FED BLDGS, PRSN, LIB, SCHL, INST",
    "4-6 LINE ADDRESS",
    "NCC DNM FILE FOR LIST RENTAL (W/O 222222)",
    "STANDARD OMIT CRITERIA (Screen 1)",
    "APO, FPO",
    "OMIT: NJ",
])


def test_strips_only_the_ncc_line():
    out = pp._strip_ncc_dnm(_BLOCK)
    assert "NCC DNM" not in out
    # every other line of the block survives, in order
    kept = [ln for ln in _BLOCK.splitlines() if "NCC DNM" not in ln]
    assert out.splitlines() == kept
    print("PASS only the NCC line is removed, the rest of the block is untouched")


def test_leaves_text_without_the_line_alone():
    plain = "OMIT: NJ\nONE PER HOUSEHOLD"
    assert pp._strip_ncc_dnm(plain) == plain
    print("PASS text with no NCC line is returned byte-for-byte")


def test_n11d_is_excluded_and_is_the_only_one():
    # A wider set would silently change the other ~153 profiles that also omit this line
    # from their standard_suppressions — see the comment on the constant.
    assert pp._NCC_DNM_EXCLUDED_DB_CODES == {"N11D"}
    assert pp._NCC_DNM_LINE == "NCC DNM FILE FOR LIST RENTAL (W/O 222222)"
    print("PASS N11D is the only excluded db_code")


def test_line_still_matches_what_the_parser_emits():
    # If the parser's wording drifts, the strip silently stops working.
    import pathlib
    src = (pathlib.Path(__file__).parent / "parsers" / "adstra.py").read_text(encoding="utf-8")
    assert pp._NCC_DNM_LINE in src, "adstra.py no longer emits this exact line"
    print("PASS the stripped line is still the line parsers/adstra.py emits")


if __name__ == "__main__":
    test_strips_only_the_ncc_line()
    test_leaves_text_without_the_line_alone()
    test_n11d_is_excluded_and_is_the_only_one()
    test_line_still_matches_what_the_parser_emits()
    print("ALL PASSED")
