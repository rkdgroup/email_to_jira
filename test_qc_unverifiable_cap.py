"""With no client profile, a dollar ceiling the model says it cannot verify is a NOTE, not a FAIL (DSLF-1456)."""
from qc_checker import FAIL, PASS, _excuse_unverifiable_cap

# DSLF-1456's SELECT finding, verbatim (2026-10-16): A69D, no profile on file, include 5-49.99 L6M.
_CAP = {"field": "Dollar band ceiling", "severity": "BLOCKING-BLANK",
        "ticket_value": "$5+ DONORS", "select_value": "RECENT PAYMENT AMT. = 5.00 THRU 49.99",
        "expected": "cap per client profile", "issue": "No profile on file; ceiling cannot be verified either way."}
_FLOOR = {"field": "Dollar band", "severity": "WRONG", "ticket_value": "$5+ DONORS", "select_value": "",
          "expected": "RECENT PAYMENT AMT. from 5.00", "issue": "no amount line applied; the $5 floor was dropped"}
_TICKET = "Summary: 3-ACBF AM CHILD CAN BENV FUND - CITIZENS BEHIND THE BADGE - J5824\n5M $5+ W/GEO\n$5+ DONORS"
_NO_PROFILE = {"client_db": "ZZ9D"}


def _result(*findings, verdict=FAIL):
    return {"verdict": verdict, "findings": [dict(f) for f in findings], "blocking_count": len(findings)}


def test_dslf_1456_unverifiable_ceiling_is_a_note_and_the_select_passes():
    got = _excuse_unverifiable_cap(_result(_CAP), _NO_PROFILE, _TICKET)
    assert got["verdict"] == PASS and got["blocking_count"] == 0
    assert got["findings"][0]["severity"] == "NOTE"


def test_a_ticket_that_states_a_ceiling_still_fails():
    for band in ("$5-$24.99 DONORS", "Dollar Cap: $24.99", "RECENT PAYMENT AMT. 5.00 THRU 24.99",
                 "$5-24 DONORS", "$5 TO 24", "5.00-24.99", "$5.00 - $24.99", "DONORS UNDER $25", "UP TO $25"):
        assert _excuse_unverifiable_cap(_result(_CAP), _NO_PROFILE, _TICKET + "\n" + band)["verdict"] == FAIL, band


def test_ticket_ranges_that_are_not_dollars_do_not_block_the_excuse():
    other = _TICKET + "\n4-6 LINE ADDRESS\nOMIT ZIPS: 64098-64099\nOMIT UNDER 17\n12-24 MONTH"
    assert _excuse_unverifiable_cap(_result(_CAP), _NO_PROFILE, other)["verdict"] == PASS


def test_a_wrong_finding_is_never_excused_only_a_blank_one():
    wrong = dict(_CAP, severity="WRONG")
    assert _excuse_unverifiable_cap(_result(wrong), _NO_PROFILE, _TICKET)["verdict"] == FAIL


def test_a_client_with_a_profile_is_untouched():
    got = _excuse_unverifiable_cap(_result(_CAP), {"client_db": "N11D"}, _TICKET)
    assert got["verdict"] == FAIL and got["findings"][0]["severity"] == "BLOCKING-BLANK"


def test_a_dropped_floor_still_fails():
    got = _excuse_unverifiable_cap(_result(_CAP, _FLOOR), _NO_PROFILE, _TICKET)
    assert got["verdict"] == FAIL and got["blocking_count"] == 1


def test_a_ceiling_finding_that_claims_a_defect_still_fails():
    lost = dict(_CAP, issue="ceiling 24.99 is below the order's band; records were lost")
    assert _excuse_unverifiable_cap(_result(lost), _NO_PROFILE, _TICKET)["verdict"] == FAIL


def test_the_input_result_is_not_mutated():
    before = _result(_CAP)
    _excuse_unverifiable_cap(before, _NO_PROFILE, _TICKET)
    assert before["verdict"] == FAIL and before["findings"][0]["severity"] == "BLOCKING-BLANK"
