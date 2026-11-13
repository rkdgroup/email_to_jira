"""--transition moves a ticket to STOREHOUSE only on a clean PASS that includes a SELECT check.

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
    assert qc._move_on_pass("DSLF-1", _out(), qc.NEED_QC_STATUS) == "MOVED to STOREHOUSE"
    assert moves == [("DSLF-1", "STOREHOUSE")]


@pytest.mark.parametrize("verdict", [qc.FAIL, qc.UNVERIFIED])
def test_fail_and_unverified_stay_put(moves, verdict):
    assert qc._move_on_pass("DSLF-1", _out(verdict), qc.NEED_QC_STATUS) is None
    assert moves == []


def test_pass_without_select_is_refused(moves):
    assert "no SELECT" in qc._move_on_pass("DSLF-1", _out(select=False), qc.NEED_QC_STATUS)
    assert moves == []


def test_only_moves_out_of_needs_qc(moves):
    assert "NOT MOVED" in qc._move_on_pass("DSLF-1", _out(), "STOREHOUSE")
    assert moves == []


def test_a_failed_transition_is_reported_not_raised(monkeypatch):
    monkeypatch.setattr(tools_jira, "transition_ticket", lambda k, n: {"error": "boom"})
    assert qc._move_on_pass("DSLF-1", _out(), qc.NEED_QC_STATUS) == "NOT MOVED: boom"
