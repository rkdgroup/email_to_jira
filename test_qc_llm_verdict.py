"""
qc_llm verdict-safety tests. No network, no Jira, no API calls, no PDFs.

    python test_qc_llm_verdict.py      # standalone, prints PASS / ALL PASSED
    pytest test_qc_llm_verdict.py      # also works

qc_llm is the QC verdict now, not a second opinion alongside the rule-based checks. That
promotion turns two previously-harmless behaviours into the worst bug the module could
have, and these tests exist to keep them fixed:

  1. Every failure path used to return [] — no findings. Advisory, that was safe, because
     run_qc_checks() still decided. As the verdict, "no findings" reads as "nothing wrong"
     and PASSES the ticket, so a missing API key, a timeout, an exhausted budget or an
     Anthropic outage would silently pass the entire queue. Errors must return UNVERIFIED.
  2. Nothing reconciled the model's own `verdict` field against its own findings. A model
     that lists a WRONG Client Database and then says PASS must not be able to pass the
     ticket — the gate decides, not the model.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Import with no key set, so nothing can reach the API from this file.
os.environ.pop("ANTHROPIC_API_KEY", None)

import qc_llm

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


# --- 1. no failure path may return a pass ----------------------------------

def test_missing_api_key_is_unverified():
    r = qc_llm.review("whatever.pdf", {})
    check("no API key gives UNVERIFIED", r["verdict"], qc_llm.UNVERIFIED)
    check("no API key is not a pass", r["verdict"] == qc_llm.PASS, False)


def test_unreadable_pdf_is_unverified():
    os.environ["ANTHROPIC_API_KEY"] = "sk-test-not-used"
    try:
        r = qc_llm.review("no_such_file_anywhere.pdf", {})
        check("unreadable SELECT gives UNVERIFIED", r["verdict"], qc_llm.UNVERIFIED)
        check("reason names the read failure", "cannot read" in r["unverified_reason"], True)
    finally:
        os.environ.pop("ANTHROPIC_API_KEY", None)


def test_exhausted_budget_is_unverified_not_pass():
    """Past the budget the old code returned [] and the rules covered it. Nothing does now."""
    os.environ["ANTHROPIC_API_KEY"] = "sk-test-not-used"
    saved = qc_llm._spent_s
    try:
        qc_llm._spent_s = qc_llm.QC_BUDGET_S + 1
        r = qc_llm.review("whatever.pdf", {})
        check("budget exhausted gives UNVERIFIED", r["verdict"], qc_llm.UNVERIFIED)
        check("budget exhaustion is not a pass", r["verdict"] == qc_llm.PASS, False)
        check("reason says the ticket went unchecked",
              "not checked" in r["unverified_reason"], True)
    finally:
        qc_llm._spent_s = saved
        os.environ.pop("ANTHROPIC_API_KEY", None)


def test_unverified_carries_no_findings_and_no_delivered_claim():
    """An UNVERIFIED result must not look like a clean read to a caller or a reader."""
    r = qc_llm.review("whatever.pdf", {})
    check("no findings invented", r["findings"], [])
    check("no delivery claimed", r["delivered"], "")


# --- 2. the gate overrides the model's own verdict -------------------------

def test_wrong_finding_forces_fail():
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": [
        {"field": "Client Database", "severity": "WRONG"}]})
    check("PASS plus a WRONG finding becomes FAIL", r["verdict"], qc_llm.FAIL)
    check("the override is recorded", r["verdict_forced"], True)


def test_blocking_blank_forces_fail():
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": [
        {"field": "Requested Quantity", "severity": "BLOCKING-BLANK"}]})
    check("PASS plus BLOCKING-BLANK becomes FAIL", r["verdict"], qc_llm.FAIL)


def test_note_alone_does_not_force_fail():
    """NOTE is explicitly non-blocking; promoting it would make every ticket fail."""
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": [
        {"field": "Other Fees", "severity": "NOTE"}]})
    check("PASS survives a NOTE-only finding", r["verdict"], qc_llm.PASS)
    check("no override recorded", r.get("verdict_forced"), None)
    check("nothing counted as blocking", r["blocking_count"], 0)


def test_severity_case_is_not_a_loophole():
    """A lowercase severity must not slip a blocking finding past the gate."""
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": [
        {"field": "Client Database", "severity": "wrong"}]})
    check("lowercase 'wrong' still forces FAIL", r["verdict"], qc_llm.FAIL)


def test_fail_stays_fail_with_no_findings():
    r = qc_llm._reconcile({"verdict": qc_llm.FAIL, "findings": []})
    check("model FAIL is never upgraded to PASS", r["verdict"], qc_llm.FAIL)


def test_clean_pass_is_still_possible():
    """A checker that can never pass is as useless as one that always does."""
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": []})
    check("clean PASS survives the gate", r["verdict"], qc_llm.PASS)
    check("blocking count is zero", r["blocking_count"], 0)


# --- 3. the report must not read as a pass --------------------------------

def test_unverified_report_says_it_is_not_a_pass():
    report = qc_llm.format_report(
        "DSLF-1", "x.SELECT.pdf",
        {"verdict": qc_llm.UNVERIFIED, "unverified_reason": "budget exhausted",
         "findings": [], "delivered": ""})
    check("report states the verdict", "VERDICT: UNVERIFIED" in report, True)
    check("report spells out it is not a pass", "NOT a pass" in report, True)
    check("report gives the reason", "budget exhausted" in report, True)


def test_forced_fail_is_visible_in_the_report():
    report = qc_llm.format_report(
        "DSLF-2", "y.SELECT.pdf",
        {"verdict": qc_llm.FAIL, "verdict_forced": True, "blocking_count": 1,
         "delivered": "asked 5,000, pulled 9,900", "model": "m", "elapsed_s": 1.0,
         "findings": [{"field": "Requested Quantity", "severity": "WRONG",
                       "ticket_value": "5,000", "select_value": "9,900",
                       "expected": "at most 5,000", "issue": "Nth overage"}]})
    check("forced verdict disclosed", "forced to FAIL" in report, True)
    check("both values quoted", "5,000" in report and "9,900" in report, True)


def test_findings_are_ordered_worst_first():
    report = qc_llm.format_report(
        "DSLF-3", "z.SELECT.pdf",
        {"verdict": qc_llm.FAIL, "blocking_count": 1, "delivered": "d",
         "model": "m", "elapsed_s": 1.0,
         "findings": [
             {"field": "Other Fees", "severity": "NOTE", "ticket_value": "a",
              "select_value": "b", "expected": "c", "issue": "d"},
             {"field": "Client Database", "severity": "WRONG", "ticket_value": "a",
              "select_value": "b", "expected": "c", "issue": "d"}]})
    check("WRONG is listed before NOTE",
          report.index("Client Database") < report.index("Other Fees"), True)


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
