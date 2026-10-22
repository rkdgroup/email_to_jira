"""The REPORT title's trailing client tag ('SCF' = SILENT CRY FOUNDATION) is not a criterion (DSLF-1484)."""
from qc_checker import FAIL, PASS, _excuse_title_tag

# DSLF-1484's SELECT finding, verbatim (2026-10-21).
_SCF = {"field": "Select Criteria - geography", "severity": "BLOCKING-BLANK",
        "ticket_value": "6M $5+ W/GEO", "select_value": "ZIP CODE = 00001 THRU 99998", "expected": "",
        "issue": 'Work order reads "SCF" but no SCFs on ticket and none applied'}
_FLAG = {"field": "Flag omits", "severity": "WRONG", "ticket_value": "!", "select_value": "",
         "expected": "!", "issue": "flag ! not omitted"}
_TICKET = "Summary: 3-HOC HEAL OUR CHILDREN - SILENT CRY FOUNDATION - J5989\nCriteria: 6M $5+ W/GEO"


def _result(*findings, verdict=FAIL):
    return {"verdict": verdict, "findings": [dict(f) for f in findings],
            "blocking_count": len(findings)}


def test_dslf_1484_client_tag_finding_is_a_note_and_the_select_passes():
    got = _excuse_title_tag(_result(_SCF), "$5+L6M SCF", "SILENT CRY FOUNDATION", _TICKET)
    assert got["verdict"] == PASS and got["blocking_count"] == 0
    assert got["findings"][0]["severity"] == "NOTE" and "client's tag" in got["findings"][0]["issue"]


def test_a_ticket_that_names_the_tag_still_fails():
    asked = _TICKET + "\nSELECT SCF 900-908"
    assert _excuse_title_tag(_result(_SCF), "$5+L6M SCF", "SILENT CRY FOUNDATION", asked)["verdict"] == FAIL


def test_a_title_word_that_is_not_the_clients_initials_still_fails():
    zips = dict(_SCF, issue='title reads "ZIPS" but no zip select applied')
    assert _excuse_title_tag(_result(zips), "$5+L6M ZIPS", "SILENT CRY FOUNDATION", _TICKET)["verdict"] == FAIL


def test_another_blocking_finding_still_fails():
    got = _excuse_title_tag(_result(_SCF, _FLAG), "$5+L6M SCF", "SILENT CRY FOUNDATION", _TICKET)
    assert got["verdict"] == FAIL and got["blocking_count"] == 1


def test_a_two_letter_tag_is_never_excused_it_could_be_a_state():
    nc = dict(_SCF, ticket_value="NORTH CAROLINA ONLY", issue='title reads "NC" but no NC state select applied')
    got = _excuse_title_tag(_result(nc), "$5+L3M NC", "NATIONAL CANCER", "Criteria: NORTH CAROLINA ONLY")
    assert got["verdict"] == FAIL and got["findings"][0]["severity"] == "BLOCKING-BLANK"


def test_the_input_result_is_not_mutated():
    before = _result(_SCF)
    _excuse_title_tag(before, "$5+L6M SCF", "SILENT CRY FOUNDATION", _TICKET)
    assert before["verdict"] == FAIL and before["findings"][0]["severity"] == "BLOCKING-BLANK"


def test_a_title_with_no_tag_changes_nothing():
    got = _excuse_title_tag(_result(_SCF), "$5+L6M", "SILENT CRY FOUNDATION", _TICKET)
    assert got["verdict"] == FAIL and got["findings"][0]["severity"] == "BLOCKING-BLANK"
