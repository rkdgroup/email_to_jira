"""--transition: a PASS (with a SELECT check) goes to STOREHOUSE, a FAIL goes to STUCK.

    python -m pytest -q test_qc_transition.py
"""
import pytest

import qc_checker as qc
import tools_jira


@pytest.fixture
def moves(monkeypatch):
    calls = []

    def fake(key, name):
        calls.append((key, name))
        return {"ok": True}

    monkeypatch.setattr(tools_jira, "transition_ticket", fake)
    return calls


def _out(verdict=qc.PASS, select=True):
    return {"verdict": verdict, "order": {"verdict": verdict}, "select": {"verdict": verdict} if select else None}


def test_pass_with_select_moves_to_storehouse(moves):
    assert qc._move_on_verdict("DSLF-1", _out(), qc.NEED_QC_STATUS) == "MOVED to STOREHOUSE"
    assert moves == [("DSLF-1", "STOREHOUSE")]


def test_fail_moves_to_stuck_even_without_a_select(moves):
    assert qc._move_on_verdict("DSLF-1", _out(qc.FAIL, select=False), qc.NEED_QC_STATUS) == "MOVED to STUCK"
    assert moves == [("DSLF-1", "STUCK")]


def test_unverified_stays_put(moves):
    assert qc._move_on_verdict("DSLF-1", _out(qc.UNVERIFIED), qc.NEED_QC_STATUS) is None
    assert moves == []


def test_pass_without_select_is_refused(moves):
    assert "no SELECT" in qc._move_on_verdict("DSLF-1", _out(select=False), qc.NEED_QC_STATUS)
    assert moves == []


@pytest.mark.parametrize("verdict", [qc.PASS, qc.FAIL])
def test_only_moves_out_of_needs_qc(moves, verdict):
    assert "NOT MOVED" in qc._move_on_verdict("DSLF-1", _out(verdict), "STOREHOUSE")
    assert moves == []


def test_a_failed_transition_is_reported_not_raised(monkeypatch):
    monkeypatch.setattr(tools_jira, "transition_ticket", lambda k, n: {"error": "boom"})
    assert qc._move_on_verdict("DSLF-1", _out(), qc.NEED_QC_STATUS) == "NOT MOVED: boom"


def _report(verdict, select=True):
    body = f"QC CHECK RESULTS — DSLF-1\nVERDICT: {verdict}\n\nORDER (ticket vs the broker order): {verdict}\n"
    if select:
        body += f"\nSELECT (ticket vs the pull): {verdict}\n"
    return {"created": "2026-11-13T09:30:51.000+0000", "body": body}


@pytest.mark.parametrize("verdict, select, want", [
    (qc.PASS, True, "MOVED to STOREHOUSE"),
    (qc.FAIL, True, "MOVED to STUCK"),
    (qc.PASS, False, "NOT MOVED: no SELECT check ran"),
])
def test_a_ticket_checked_before_the_flag_moves_on_its_posted_verdict(monkeypatch, moves, verdict, select, want):
    monkeypatch.setattr(qc, "_last_qc_comment", lambda k: _report(verdict, select))

    r = qc._move_from_last_report("DSLF-1", qc.NEED_QC_STATUS)

    assert r["moved"] == want and r["verdict"] == verdict


def test_scan_moves_an_unchanged_ticket_only_when_asked(monkeypatch, moves):
    issue = {"key": "DSLF-1", "fields": {"status": {"name": qc.NEED_QC_STATUS},
                                         "updated": "2026-11-13T09:30:51.000+0000"}}
    monkeypatch.setattr(tools_jira, "search_issues_paged", lambda *a, **k: [issue])
    monkeypatch.setattr(qc, "_service_account_id", lambda: "me")
    monkeypatch.setattr(qc, "_last_qc_comment", lambda k: _report(qc.PASS))

    assert qc.scan(qc.NEED_QC_STATUS) == [] and moves == []
    assert qc.scan(qc.NEED_QC_STATUS, transition=True, dry_run=True) == [] and moves == []
    out = qc.scan(qc.NEED_QC_STATUS, transition=True)

    assert out[0]["moved"] == "MOVED to STOREHOUSE" and moves == [("DSLF-1", "STOREHOUSE")]
