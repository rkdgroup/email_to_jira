"""
qc_llm safety tests. No network, no Jira, no API calls, no PDFs.

    python test_qc_llm_verdict.py      # standalone, prints PASS / ALL PASSED
    pytest test_qc_llm_verdict.py      # also works

qc_llm is the only QC there is now — the 14 rule-based checks it used to sit beside are
gone. Two properties have to hold, and neither is the model's to keep:

  1. NO FAILURE PATH MAY RETURN A PASS. Every one returns UNVERIFIED, which is not a pass.
     As the advisory pass, failures returned [] and the rules still decided; as the sole
     checker that same [] reads as "nothing wrong found" and passes the whole queue.
  2. THE GATE OVERRIDES THE MODEL. A WRONG or BLOCKING-BLANK finding forces FAIL whatever
     the model wrote in `verdict`.

Plus two things that are new and write to production:

  3. AUTO-FIX REFUSES anything outside the whitelist, anything Jira would silently drop,
     and anything with no replacement value. The database triad is never writable.
  4. THE RE-RUN GUARD MUST NOT TREAT UNVERIFIED AS CHECKED, or a ticket the checker never
     read sits in the queue looking done forever.
"""

import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import qc_llm

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


# ---------------------------------------------------------------------------
# 1. No failure path returns a pass
# ---------------------------------------------------------------------------

def test_missing_api_key_is_unverified():
    saved = os.environ.pop("ANTHROPIC_API_KEY", None)
    try:
        for name, fn in (("SELECT", qc_llm.review_select), ("ORDER", qc_llm.review_order)):
            r = fn("whatever.pdf", {})
            check(f"no API key gives UNVERIFIED [{name}]", r["verdict"], qc_llm.UNVERIFIED)
            check(f"no API key is not a pass [{name}]", r["verdict"] == qc_llm.PASS, False)
    finally:
        if saved is not None:
            os.environ["ANTHROPIC_API_KEY"] = saved


def test_unreadable_pdf_is_unverified():
    os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")
    r = qc_llm.review_select("no_such_file_anywhere.pdf", {})
    check("unreadable PDF gives UNVERIFIED", r["verdict"], qc_llm.UNVERIFIED)
    check("unreadable PDF is not a pass", r["verdict"] == qc_llm.PASS, False)


def test_exhausted_budget_is_unverified():
    saved = qc_llm._spent_s
    try:
        qc_llm._spent_s = qc_llm.QC_BUDGET_S + 1
        r = qc_llm.review_select("whatever.pdf", {})
        check("budget exhausted gives UNVERIFIED", r["verdict"], qc_llm.UNVERIFIED)
        check("budget exhaustion is not a pass", r["verdict"] == qc_llm.PASS, False)
    finally:
        qc_llm._spent_s = saved


def test_unverified_carries_no_findings_and_a_reason():
    r = qc_llm._unverified("something broke", "SELECT")
    check("UNVERIFIED has no findings", r["findings"], [])
    check("UNVERIFIED states why", bool(r["unverified_reason"]), True)
    check("UNVERIFIED names the check", r["check"], "SELECT")


def test_review_never_raises_on_a_broken_ticket():
    """The prompt build reads the profile YAML and the ticket ADF; both can throw."""
    class Exploding(dict):
        def get(self, *a, **k):
            raise RuntimeError("boom")

    for name, fn in (("SELECT", qc_llm.review_select), ("ORDER", qc_llm.review_order)):
        try:
            r = fn("whatever.pdf", Exploding())
        except Exception as e:
            check(f"{name} swallowed the context error", f"raised {e}", "UNVERIFIED")
            continue
        check(f"broken ticket context gives UNVERIFIED [{name}]",
              r["verdict"], qc_llm.UNVERIFIED)


# ---------------------------------------------------------------------------
# 2. The gate overrides the model
# ---------------------------------------------------------------------------

def test_wrong_finding_forces_fail():
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": [
        {"field": "Client Database", "severity": "WRONG"}]})
    check("PASS plus a WRONG finding becomes FAIL", r["verdict"], qc_llm.FAIL)
    check("the override is recorded", r.get("verdict_forced"), True)


def test_blocking_blank_forces_fail():
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": [
        {"field": "Availability Rule", "severity": "BLOCKING-BLANK"}]})
    check("PASS plus BLOCKING-BLANK becomes FAIL", r["verdict"], qc_llm.FAIL)


def test_note_never_forces_fail():
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": [
        {"field": "Other Fees", "severity": "NOTE"}]})
    check("PASS survives a NOTE-only finding", r["verdict"], qc_llm.PASS)
    check("NOTE is not counted as blocking", r["blocking_count"], 0)


def test_severity_case_does_not_matter():
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": [
        {"field": "Seed Database", "severity": "wrong"}]})
    check("lowercase 'wrong' still forces FAIL", r["verdict"], qc_llm.FAIL)


def test_model_fail_is_never_upgraded():
    r = qc_llm._reconcile({"verdict": qc_llm.FAIL, "findings": []})
    check("model FAIL is never upgraded to PASS", r["verdict"], qc_llm.FAIL)


def test_clean_pass_survives():
    r = qc_llm._reconcile({"verdict": qc_llm.PASS, "findings": []})
    check("clean PASS survives the gate", r["verdict"], qc_llm.PASS)
    check("clean PASS is not marked forced", r.get("verdict_forced"), None)


def test_worst_verdict_combination():
    """Two checks per ticket now. Not knowing is worse than knowing it failed."""
    W = qc_llm._worst
    check("PASS + PASS", W(qc_llm.PASS, qc_llm.PASS), qc_llm.PASS)
    check("PASS + FAIL", W(qc_llm.PASS, qc_llm.FAIL), qc_llm.FAIL)
    check("FAIL + UNVERIFIED", W(qc_llm.FAIL, qc_llm.UNVERIFIED), qc_llm.UNVERIFIED)
    check("PASS + UNVERIFIED", W(qc_llm.PASS, qc_llm.UNVERIFIED), qc_llm.UNVERIFIED)
    check("one check only", W(qc_llm.PASS, None), qc_llm.PASS)
    check("no check at all is UNVERIFIED, not PASS", W(), qc_llm.UNVERIFIED)


# ---------------------------------------------------------------------------
# 3. Auto-fix refuses what it cannot safely write
# ---------------------------------------------------------------------------

def test_database_triad_is_never_writable():
    """A wrong write here sends the wrong donor file to the wrong company."""
    for field in ("client_db", "seed_db", "billable_account"):
        fid, val, reason = qc_llm._validate_fix(field, "N11D", {})
        check(f"{field} refused", fid, None)
        check(f"{field} says why", bool(reason), True)


def test_prose_fields_are_never_writable():
    for field in ("description", "omission", "description_adf", "status"):
        fid, _, reason = qc_llm._validate_fix(field, "anything", {})
        check(f"{field} refused", fid, None)


def test_empty_fix_value_is_refused():
    fid, _, reason = qc_llm._validate_fix("mailer_po", "   ", {})
    check("blank replacement refused", fid, None)
    check("blanking a field is not a fix", "no replacement value" in (reason or ""), True)


def test_select_option_must_exist():
    """Jira drops an unresolvable option WITHOUT failing the request — it looks like it worked."""
    fid, _, reason = qc_llm._validate_fix("file_format", "ASCII Fixed Length", {})
    check("unknown file format refused", fid, None)
    fid, val, reason = qc_llm._validate_fix("file_format", "ASCII Fixed", {})
    check("known file format accepted", fid, "customfield_12274")
    check("sent as an option id", val, {"id": "13238"})


def test_availability_and_shipping_options():
    fid, val, _ = qc_llm._validate_fix("availability_rule", "All Available", {})
    check("All Available maps to its id", val, {"id": "13236"})
    fid, val, _ = qc_llm._validate_fix("shipping_method", "FTP", {})
    check("FTP maps to its id", val, {"id": "13242"})
    fid, _, reason = qc_llm._validate_fix("availability_rule", "Full Run", {})
    check("'Full Run' is not a Jira option", fid, None)


def test_list_manager_must_be_one_of_the_fourteen():
    fid, _, reason = qc_llm._validate_fix("list_manager", "SIMIOCLOUD", {})
    check("unknown list manager refused", fid, None)
    fid, val, _ = qc_llm._validate_fix("list_manager", "we are moore", {})
    check("known list manager accepted, upper-cased", val, "WE ARE MOORE")


def test_dates_must_be_iso():
    fid, _, _ = qc_llm._validate_fix("mail_date", "08/19/26", {})
    check("US-format date refused", fid, None)
    fid, val, _ = qc_llm._validate_fix("mail_date", "2026-08-19", {})
    check("ISO date accepted", val, "2026-08-19")


def test_quantity_must_be_a_plausible_integer():
    check("comma quantity parsed", qc_llm._validate_fix("requested_qty", "32,422", {})[1], 32422)
    check("non-numeric refused", qc_llm._validate_fix("requested_qty", "all", {})[0], None)
    check("zero refused", qc_llm._validate_fix("requested_qty", "0", {})[0], None)


def test_seed_tracking_is_forced_to_the_manager_order():
    fields = {"manager_order": "DL995"}
    fid, val, _ = qc_llm._validate_fix("seed_tracking", "DL995", fields)
    check("matching seed tracking accepted", val, "DL995")
    fid, _, reason = qc_llm._validate_fix("seed_tracking", "CRU 924-105", fields)
    check("a different seed tracking refused", fid, None)
    check("reason names the house rule", "manager order" in (reason or "").lower(), True)


def test_apply_fixes_dry_run_writes_nothing():
    findings = [{"field": "Mailer PO", "severity": "WRONG", "ticket_value": "",
                 "fix_field": "mailer_po", "fix_value": "CRU 924-105"}]
    r = qc_llm.apply_fixes("DSLF-0", findings, {}, dry_run=True)
    check("dry run reports the fix", len(r["applied"]), 1)
    check("dry run is flagged", r.get("dry_run"), True)


def test_apply_fixes_skips_notes_and_duplicates():
    findings = [
        {"field": "Other Fees", "severity": "NOTE",
         "fix_field": "other_fees", "fix_value": "STATE OMITS"},
        {"field": "Mailer PO", "severity": "WRONG",
         "fix_field": "mailer_po", "fix_value": "A"},
        {"field": "Mailer PO again", "severity": "WRONG",
         "fix_field": "mailer_po", "fix_value": "B"},
        {"field": "Client Database", "severity": "WRONG",
         "fix_field": "client_db", "fix_value": "N11D"},
    ]
    r = qc_llm.apply_fixes("DSLF-0", findings, {}, dry_run=True)
    check("only the first real fix is applied", len(r["applied"]), 1)
    check("NOTE, duplicate and triad all refused", len(r["refused"]), 3)


def test_apply_fixes_with_nothing_to_do():
    r = qc_llm.apply_fixes("DSLF-0", [{"field": "x", "severity": "WRONG"}], {}, dry_run=True)
    check("no fix_field means no write", r["applied"], [])
    check("still reports ok", r["ok"], True)


# ---------------------------------------------------------------------------
# 4. The re-run guard must not treat UNVERIFIED as checked
# ---------------------------------------------------------------------------

def test_unverified_report_is_recognisable_to_the_rerun_guard():
    """_last_qc_comment_time greps the posted comment for this exact line.

    If the report format drifts, the guard silently starts treating unchecked tickets as
    checked and they never come back. This pins the two together.
    """
    report = qc_llm.format_report("DSLF-1", {
        "verdict": qc_llm.UNVERIFIED,
        "select": qc_llm._unverified("budget exhausted", "SELECT"),
        "select_filename": "S.pdf"})
    check("report starts with the prefix the guard looks for",
          report.startswith(qc_llm._QC_COMMENT_PREFIXES), True)
    check("guard's UNVERIFIED pattern matches the report",
          bool(re.search(r'^VERDICT:\s*UNVERIFIED', report, re.MULTILINE)), True)
    check("report says it is not a pass", "NOT a pass" in report, True)


def test_a_real_verdict_is_not_mistaken_for_unverified():
    report = qc_llm.format_report("DSLF-1", {
        "verdict": qc_llm.PASS,
        "select": {"verdict": qc_llm.PASS, "findings": [], "delivered": "d",
                   "model": "m", "elapsed_s": 1.0},
        "select_filename": "S.pdf"})
    check("a PASS report does not match the UNVERIFIED pattern",
          bool(re.search(r'^VERDICT:\s*UNVERIFIED', report, re.MULTILINE)), False)


# ---------------------------------------------------------------------------
# 5. Report contents
# ---------------------------------------------------------------------------

def test_pass_report_says_so_explicitly():
    """The ticket gets a comment even when it is clean — that is the point of posting."""
    report = qc_llm.format_report("DSLF-1", {
        "verdict": qc_llm.PASS,
        "order": {"verdict": qc_llm.PASS, "findings": [], "delivered": "matches",
                  "model": "m", "elapsed_s": 2.0},
        "order_filename": "order.pdf"})
    check("clean ticket is told so", "Checked and correct" in report, True)
    check("no-findings line present", "No discrepancies found." in report, True)


def test_nothing_attached_is_not_a_pass():
    report = qc_llm.format_report("DSLF-1", {"verdict": qc_llm.UNVERIFIED})
    check("no PDFs at all is not a pass", "NOT a pass" in report, True)


def test_forced_fail_is_disclosed():
    report = qc_llm.format_report("DSLF-1", {
        "verdict": qc_llm.FAIL,
        "select": {"verdict": qc_llm.FAIL, "verdict_forced": True, "blocking_count": 1,
                   "delivered": "d", "model": "m", "elapsed_s": 1.0,
                   "findings": [{"field": "Client Database", "severity": "WRONG",
                                 "ticket_value": "A", "select_value": "B",
                                 "expected": "A", "issue": "mismatch"}]},
        "select_filename": "S.pdf"})
    check("forced verdict disclosed", "forced to FAIL" in report, True)
    check("both sides quoted", "A" in report and "B" in report, True)


def test_fix_section_distinguishes_applied_from_refused():
    report = qc_llm.format_report("DSLF-1", {
        "verdict": qc_llm.FAIL,
        "order": {"verdict": qc_llm.FAIL, "findings": [], "delivered": "d",
                  "model": "m", "elapsed_s": 1.0},
        "order_filename": "o.pdf",
        "fixes": {"applied": ["mailer_po: (empty) -> CRU 924-105"],
                  "refused": ["client_db: not auto-fixable"]}})
    check("applied fixes listed", "APPLIED" in report, True)
    check("refused fixes listed", "NOT APPLIED" in report, True)


# ---------------------------------------------------------------------------
# 6. The prompts still carry the load-bearing domain rules
# ---------------------------------------------------------------------------

def test_profile_context_carries_the_dollar_cap():
    """Without the cap, every correctly-executed capped pull reads as lost records."""
    ctx = qc_llm._profile_context({"client_db": "W12D"})
    check("cap is in the prompt", "Dollar cap" in ctx, True)
    check("N11D's $99.99 cap reaches the prompt",
          "$99.99" in qc_llm._profile_context({"client_db": "N11D"}), True)


def test_profile_context_handles_an_unknown_database():
    ctx = qc_llm._profile_context({"client_db": "ZZ9D"})
    check("unknown db says it cannot verify", "could not be verified" in ctx
          or "cannot confirm" in ctx, True)
    check("blank db_code handled", isinstance(qc_llm._profile_context({}), str), True)


def test_select_prompt_keeps_the_rules_the_regex_checker_knew():
    s = qc_llm._SYSTEM_SELECT
    for needle, why in (
        ("10.00 THRU 99.99",     "the dollar-cap shape"),
        ("never NARROWER",       "include-set direction"),
        ("0 records",            "an empty SELECT is always a failure"),
        ("All Available",        "the availability split"),
        ("hosted",               "the host-vs-rented-list exception (DSLF-1066/1083)"),
        ("FLAGS LISTED",         "flags deferred to prose are unverifiable"),
    ):
        check(f"SELECT prompt keeps {why}", needle.lower() in s.lower(), True)


def test_order_prompt_keeps_knowledge_mds_rules():
    s = qc_llm._SYSTEM_ORDER
    for needle, why in (
        ("N13D",                 "the NPTA wrong-client incident"),
        ("S30D",                 "the SAVE wrong-client incident"),
        ("Exch Qty",             "the exchange-quantity trap"),
        ("incoming.files@data-axle.com", "the drop-box vs staff-mailbox rule"),
        ("keyacquistion.com",    "the typo'd requestor address"),
        ("needs a Jira admin",   "known-missing options are not a parse bug"),
    ):
        check(f"ORDER prompt keeps {why}", needle.lower() in s.lower(), True)


def test_order_prompt_does_not_resurrect_the_kap_title_exception():
    """knowledge.md line 161 called `P.O. {DL#} {LIST NAME}` a KAP design. It was a bug,
    fixed in 39d94bc, and 64 tickets carried it. Treating it as design hides a real defect.
    """
    s = qc_llm._SYSTEM_ORDER
    check("KAP is not exempted from the title rule",
          "kap tickets are the exception" in s.lower(), False)
    check("the old KAP title shape is called out as wrong",
          "p.o. dl984" in s.lower() or "P.O. DL" in s, True)
    check("every broker including KAP is stated",
          "including kap" in s.lower(), True)


def test_no_fix_field_leaks_into_the_select_check():
    """A bad pull needs re-running. Editing the ticket to match it erases the evidence."""
    props = qc_llm._schema(False)["properties"]["findings"]["items"]["properties"]
    check("SELECT findings have no fix_field", "fix_field" in props, False)
    props = qc_llm._schema(True)["properties"]["findings"]["items"]["properties"]
    check("ORDER findings do have fix_field", "fix_field" in props, True)


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
