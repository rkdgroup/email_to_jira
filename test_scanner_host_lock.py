"""The email scanner runs only on the rkdc2 Jenkins host. No network: get_access_token is stubbed.

    python -m pytest -q test_scanner_host_lock.py

From 2026-10-27 to 11-10 a second runner of the Jenkins script, never located, created every
order twice (DSLF-1567/1568). It re-clones main each build, so the host check in run_scan is
what stops it: any other host exits 1 before the mailbox is touched.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "email_scanner"))
import email_scanner as es  # noqa: E402


class Reached(Exception):
    """Raised by the stub standing in for the first network call."""


def _reached():
    raise Reached


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(es, "SCANNER_ENABLED", True)
    monkeypatch.setattr(es, "get_access_token", _reached)


def test_other_host_exits_1_before_the_mailbox(monkeypatch):
    monkeypatch.setattr(es.platform, "node", lambda: "some-other-box")
    with pytest.raises(SystemExit) as e:
        es.run_scan()
    assert e.value.code == 1


def test_jenkins_host_proceeds(monkeypatch):
    monkeypatch.setattr(es.platform, "node", lambda: "rkdc2-dmiappsdev01")
    with pytest.raises(Reached):
        es.run_scan()


def test_fqdn_and_case_still_match(monkeypatch):
    monkeypatch.setattr(es.platform, "node", lambda: "RKDC2-DMIAPPSDEV01.data-management.com")
    with pytest.raises(Reached):
        es.run_scan()


def test_kill_switch_still_wins(monkeypatch):
    monkeypatch.setattr(es, "SCANNER_ENABLED", False)
    monkeypatch.setattr(es.platform, "node", lambda: "some-other-box")
    assert es.run_scan() is None
