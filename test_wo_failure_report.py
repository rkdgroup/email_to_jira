"""
WO-failure self-report tests. No network, no Jira, no IBM i.

    python test_wo_failure_report.py      # standalone, prints PASS / ALL PASSED
    pytest test_wo_failure_report.py      # also works

DSLF-1342 through -1346 (2026-09-25 / -28) were created with no work order and the
Jenkins build history held no log of why: the WO step swallows its failure by design and
the log line was the only record. The reason is now also posted on the ticket itself,
along with the host that ran the pipeline, and posting it can never fail the create.
"""

import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import tools_jira
from parse_pipeline import _create_and_link_work_order

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


def _stub(create_raises=None, comment_raises=False):
    """Fake work_order + Jira calls; returns the list that records posted comments."""
    posted = []

    def create_work_order(**_kw):
        raise create_raises

    def add_comment_to_ticket(key, body, code_block=False):
        if comment_raises:
            raise ConnectionError("jira down")
        posted.append((key, body, code_block))
        return {"id": "1"}

    sys.modules["work_order"] = types.SimpleNamespace(create_work_order=create_work_order)
    tools_jira.add_comment_to_ticket = add_comment_to_ticket
    tools_jira.get_ticket_billable_account = lambda _key: "N15"
    tools_jira.update_ticket_fields = lambda *_a: {"ok": True}
    return posted


def _call(billable="N15", dry_run=False):
    return _create_and_link_work_order(
        ticket_key="DSLF-9999", billable_account=billable, mailer_name="EVERCARE PROTECTION",
        manager_order_number="J5415", mailer_po="2361585", dry_run=dry_run,
    )


def test_failure_is_posted_on_the_ticket():
    posted = _stub(create_raises=FileNotFoundError("jt400.jar not found at: /x/jt400.jar"))
    check("returns None", _call(), None)
    check("one comment", len(posted), 1)
    key, body, code_block = posted[0]
    check("on the ticket", key, "DSLF-9999")
    check("headline", body.startswith("WORK ORDER NOT CREATED"), True)
    check("carries the exception", "jt400.jar not found at: /x/jt400.jar" in body, True)
    check("names the host", "host:" in body, True)
    check("code block", code_block, True)


def test_comment_failure_cannot_break_the_create():
    _stub(create_raises=RuntimeError("boom"), comment_raises=True)
    check("still returns None", _call(), None)


def test_dry_run_posts_nothing():
    posted = _stub(create_raises=RuntimeError("boom"))
    check("dry run returns None", _call(dry_run=True), None)
    check("dry run posts no comment", posted, [])


def test_missing_billable_is_posted():
    posted = _stub()
    check("skip returns None", _call(billable=""), None)
    check("skip comment posted", len(posted), 1)
    check("skip reason", "no billable account" in posted[0][1].lower(), True)


if __name__ == "__main__":
    test_failure_is_posted_on_the_ticket()
    test_comment_failure_cannot_break_the_create()
    test_dry_run_posts_nothing()
    test_missing_billable_is_posted()
    if _failures:
        print("\n".join(_failures))
        sys.exit(1)
    print("ALL PASSED")
