"""A 1 PER HOUSEHOLD ask ships without the dedupe for a customer that owns no household set (DSLF-1463)."""
from qc_checker import FAIL, PASS, _excuse_household

# DSLF-1463's SELECT finding, verbatim (2026-10-19).
_HH = {"field": "Omission Description", "severity": "WRONG", "ticket_value": "1 PER HOUSEHOLD",
       "select_value": "4-6 LINE ADDRESS; OMIT INSTITUTIONS; NO PERSONAL NAME", "expected": "",
       "issue": "household de-dupe never applied; duplicate-household records shipped"}
_FLAG = {"field": "Flag omits", "severity": "WRONG", "ticket_value": "!", "select_value": "",
         "expected": "!", "issue": "flag ! not omitted"}


def _result(*findings, verdict=FAIL):
    return {"verdict": verdict, "findings": [dict(f) for f in findings],
            "blocking_count": sum(f["severity"] == "WRONG" for f in findings)}


def test_s05d_household_finding_is_a_note_and_the_select_passes():
    got = _excuse_household(_result(_HH), "S05D")
    assert got["verdict"] == PASS and got["blocking_count"] == 0
    assert got["findings"][0]["severity"] == "NOTE" and "no household" in got["findings"][0]["issue"]


def test_a_customer_with_a_household_set_still_fails():
    assert _excuse_household(_result(_HH), "N11D")["verdict"] == FAIL


def test_another_blocking_finding_still_fails_s05d():
    got = _excuse_household(_result(_HH, _FLAG), "S05D")
    assert got["verdict"] == FAIL and got["blocking_count"] == 1
