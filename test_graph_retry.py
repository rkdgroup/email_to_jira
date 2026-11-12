"""Graph calls in the email scanner ride out 503s, throttling and read timeouts. No network.

    python -m pytest -q test_graph_retry.py

2026-11-12: Jenkins builds died on one 503, then on 20s read timeouts, at the first folder
lookups; a failed run also skips qc_checker under `sh -xe`.
"""
import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).parent / "email_scanner"))
import email_scanner as es  # noqa: E402


class Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body or {}, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)

    def json(self):
        return self._body


@pytest.fixture
def script(monkeypatch):
    """Feed requests.request a scripted series of responses/exceptions; record sleeps and calls."""
    state = {"calls": 0, "sleeps": []}

    def install(*steps):
        it = iter(steps)

        def fake(method, url, **kw):
            state["calls"] += 1
            step = next(it)
            if isinstance(step, Exception):
                raise step
            return step

        monkeypatch.setattr(es.requests, "request", fake)

    monkeypatch.setattr(es.time, "sleep", state["sleeps"].append)
    state["install"] = install
    return state


def test_get_retries_a_503_then_succeeds(script):
    script["install"](Resp(503), Resp(200, {"value": [1]}))
    assert es._get("t", "u") == {"value": [1]}
    assert script["calls"] == 2


def test_get_retries_read_timeouts(script):
    script["install"](requests.ReadTimeout(), requests.ReadTimeout(), Resp(200, {"ok": 1}))
    assert es._get("t", "u") == {"ok": 1}


def test_retry_after_is_honoured_and_capped(script):
    script["install"](Resp(429, headers={"Retry-After": "7"}), Resp(503, headers={"Retry-After": "999"}),
                      Resp(200, {}))
    es._get("t", "u")
    assert script["sleeps"] == [7, 30]


def test_gives_up_after_three_tries(script):
    script["install"](Resp(503), Resp(503), Resp(503))
    with pytest.raises(requests.HTTPError):
        es._get("t", "u")
    assert script["calls"] == 3


def test_a_404_is_never_retried(script):
    script["install"](Resp(404))
    with pytest.raises(requests.HTTPError):
        es._get("t", "u")
    assert script["calls"] == 1


def test_post_retries_a_refusal_but_not_a_timeout(script):
    script["install"](Resp(503), Resp(200, {"id": "x"}))
    assert es._post("t", "u", {}) == {"id": "x"}
    script["calls"] = 0
    script["install"](requests.ReadTimeout())
    with pytest.raises(requests.ReadTimeout):  # the move may have run: do not repeat it
        es._post("t", "u", {})
    assert script["calls"] == 1


def test_post_does_not_repeat_a_502(script):
    script["install"](Resp(502))
    with pytest.raises(requests.HTTPError):
        es._post("t", "u", {})
    assert script["calls"] == 1


def test_list_rental_is_looked_up_once_for_processed_and_failed(monkeypatch):
    seen = []
    monkeypatch.setattr(es, "_folder_cache", {})
    monkeypatch.setattr(es, "_get", lambda tok, url, params=None: (seen.append(url), {"value": [{"id": url[-40:]}]})[1])
    es._get_folder_id("t", es.SOURCE_FOLDER)
    es._get_folder_id("t", es.FAILED_FOLDER)
    es._get_folder_id("t", es.PROCESSED_FOLDER)
    assert len(seen) == 3  # List Rental, then Failed, then Processed (was 5)
