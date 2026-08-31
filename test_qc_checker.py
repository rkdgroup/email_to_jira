"""
qc_checker safety tests. No network, no Jira, no API calls, no PDFs.

    python test_qc_checker.py      # standalone, prints PASS / ALL PASSED
    pytest test_qc_checker.py      # also works

qc_checker is the only QC there is now — the 14 rule-based checks it used to sit beside are
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

import qc_checker as qc

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
        for name, fn in (("SELECT", qc.review_select), ("ORDER", qc.review_order)):
            r = fn("whatever.pdf", {})
            check(f"no API key gives UNVERIFIED [{name}]", r["verdict"], qc.UNVERIFIED)
            check(f"no API key is not a pass [{name}]", r["verdict"] == qc.PASS, False)
    finally:
        if saved is not None:
            os.environ["ANTHROPIC_API_KEY"] = saved


def test_unreadable_pdf_is_unverified():
    os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")
    r = qc.review_select("no_such_file_anywhere.pdf", {})
    check("unreadable PDF gives UNVERIFIED", r["verdict"], qc.UNVERIFIED)
    check("unreadable PDF is not a pass", r["verdict"] == qc.PASS, False)


def test_unverified_carries_no_findings_and_a_reason():
    r = qc._unverified("something broke", "SELECT")
    check("UNVERIFIED has no findings", r["findings"], [])
    check("UNVERIFIED states why", bool(r["unverified_reason"]), True)
    check("UNVERIFIED names the check", r["check"], "SELECT")


def test_review_never_raises_on_a_broken_ticket():
    """The prompt build reads the profile YAML and the ticket ADF; both can throw."""
    class Exploding(dict):
        def get(self, *a, **k):
            raise RuntimeError("boom")

    for name, fn in (("SELECT", qc.review_select), ("ORDER", qc.review_order)):
        try:
            r = fn("whatever.pdf", Exploding())
        except Exception as e:
            check(f"{name} swallowed the context error", f"raised {e}", "UNVERIFIED")
            continue
        check(f"broken ticket context gives UNVERIFIED [{name}]",
              r["verdict"], qc.UNVERIFIED)


# ---------------------------------------------------------------------------
# 2. The gate overrides the model
# ---------------------------------------------------------------------------

def test_wrong_finding_forces_fail():
    r = qc._reconcile({"verdict": qc.PASS, "findings": [
        {"field": "Client Database", "severity": "WRONG"}]})
    check("PASS plus a WRONG finding becomes FAIL", r["verdict"], qc.FAIL)
    check("the override is recorded", r.get("verdict_forced"), True)


def test_blocking_blank_forces_fail():
    r = qc._reconcile({"verdict": qc.PASS, "findings": [
        {"field": "Availability Rule", "severity": "BLOCKING-BLANK"}]})
    check("PASS plus BLOCKING-BLANK becomes FAIL", r["verdict"], qc.FAIL)


def test_note_never_forces_fail():
    r = qc._reconcile({"verdict": qc.PASS, "findings": [
        {"field": "Other Fees", "severity": "NOTE"}]})
    check("PASS survives a NOTE-only finding", r["verdict"], qc.PASS)
    check("NOTE is not counted as blocking", r["blocking_count"], 0)


def test_severity_case_does_not_matter():
    r = qc._reconcile({"verdict": qc.PASS, "findings": [
        {"field": "Seed Database", "severity": "wrong"}]})
    check("lowercase 'wrong' still forces FAIL", r["verdict"], qc.FAIL)


def test_model_fail_is_never_upgraded():
    r = qc._reconcile({"verdict": qc.FAIL, "findings": []})
    check("model FAIL is never upgraded to PASS", r["verdict"], qc.FAIL)


def test_clean_pass_survives():
    r = qc._reconcile({"verdict": qc.PASS, "findings": []})
    check("clean PASS survives the gate", r["verdict"], qc.PASS)
    check("clean PASS is not marked forced", r.get("verdict_forced"), None)


def test_worst_verdict_combination():
    """Two checks per ticket now. Not knowing is worse than knowing it failed."""
    W = qc._worst
    check("PASS + PASS", W(qc.PASS, qc.PASS), qc.PASS)
    check("PASS + FAIL", W(qc.PASS, qc.FAIL), qc.FAIL)
    check("FAIL + UNVERIFIED", W(qc.FAIL, qc.UNVERIFIED), qc.UNVERIFIED)
    check("PASS + UNVERIFIED", W(qc.PASS, qc.UNVERIFIED), qc.UNVERIFIED)
    check("one check only", W(qc.PASS, None), qc.PASS)
    check("no check at all is UNVERIFIED, not PASS", W(), qc.UNVERIFIED)


# ---------------------------------------------------------------------------
# 3. Auto-fix refuses what it cannot safely write
# ---------------------------------------------------------------------------

def test_database_triad_is_never_writable():
    """A wrong write here sends the wrong donor file to the wrong company."""
    for field in ("client_db", "seed_db", "billable_account"):
        fid, val, reason = qc._validate_fix(field, "N11D", {})
        check(f"{field} refused", fid, None)
        check(f"{field} says why", bool(reason), True)


def test_prose_fields_are_never_writable():
    for field in ("description", "omission", "description_adf", "status"):
        fid, _, reason = qc._validate_fix(field, "anything", {})
        check(f"{field} refused", fid, None)


def test_empty_fix_value_is_refused():
    fid, _, reason = qc._validate_fix("mailer_po", "   ", {})
    check("blank replacement refused", fid, None)
    check("blanking a field is not a fix", "no replacement value" in (reason or ""), True)


def test_select_option_must_exist():
    """Jira drops an unresolvable option WITHOUT failing the request — it looks like it worked."""
    fid, _, reason = qc._validate_fix("file_format", "ASCII Fixed Length", {})
    check("unknown file format refused", fid, None)
    fid, val, reason = qc._validate_fix("file_format", "ASCII Fixed", {})
    check("known file format accepted", fid, "customfield_12274")
    check("sent as an option id", val, {"id": "13238"})


def test_availability_and_shipping_options():
    fid, val, _ = qc._validate_fix("availability_rule", "All Available", {})
    check("All Available maps to its id", val, {"id": "13236"})
    fid, val, _ = qc._validate_fix("shipping_method", "FTP", {})
    check("FTP maps to its id", val, {"id": "13242"})
    fid, _, reason = qc._validate_fix("availability_rule", "Full Run", {})
    check("'Full Run' is not a Jira option", fid, None)


def test_list_manager_must_be_one_of_the_fourteen():
    fid, _, reason = qc._validate_fix("list_manager", "SIMIOCLOUD", {})
    check("unknown list manager refused", fid, None)
    fid, val, _ = qc._validate_fix("list_manager", "we are moore", {})
    check("known list manager accepted, upper-cased", val, "WE ARE MOORE")


def test_dates_must_be_iso():
    fid, _, _ = qc._validate_fix("mail_date", "08/19/26", {})
    check("US-format date refused", fid, None)
    fid, val, _ = qc._validate_fix("mail_date", "2026-08-19", {})
    check("ISO date accepted", val, "2026-08-19")


def test_quantity_must_be_a_plausible_integer():
    check("comma quantity parsed", qc._validate_fix("requested_qty", "32,422", {})[1], 32422)
    check("non-numeric refused", qc._validate_fix("requested_qty", "all", {})[0], None)
    check("zero refused", qc._validate_fix("requested_qty", "0", {})[0], None)


def test_seed_tracking_is_forced_to_the_manager_order():
    fields = {"manager_order": "DL995"}
    fid, val, _ = qc._validate_fix("seed_tracking", "DL995", fields)
    check("matching seed tracking accepted", val, "DL995")
    fid, _, reason = qc._validate_fix("seed_tracking", "CRU 924-105", fields)
    check("a different seed tracking refused", fid, None)
    check("reason names the house rule", "manager order" in (reason or "").lower(), True)


def test_apply_fixes_dry_run_writes_nothing():
    findings = [{"field": "Mailer PO", "severity": "WRONG", "ticket_value": "",
                 "fix_field": "mailer_po", "fix_value": "CRU 924-105"}]
    r = qc.apply_fixes("DSLF-0", findings, {}, dry_run=True)
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
    r = qc.apply_fixes("DSLF-0", findings, {}, dry_run=True)
    check("only the first real fix is applied", len(r["applied"]), 1)
    check("NOTE, duplicate and triad all refused", len(r["refused"]), 3)


def test_apply_fixes_with_nothing_to_do():
    r = qc.apply_fixes("DSLF-0", [{"field": "x", "severity": "WRONG"}], {}, dry_run=True)
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
    report = qc.format_report("DSLF-1", {
        "verdict": qc.UNVERIFIED,
        "select": qc._unverified("budget exhausted", "SELECT"),
        "select_filename": "S.pdf"})
    check("report starts with the prefix the guard looks for",
          report.startswith(qc._QC_COMMENT_PREFIXES), True)
    check("guard's UNVERIFIED pattern matches the report",
          bool(re.search(r'^VERDICT:\s*UNVERIFIED', report, re.MULTILINE)), True)
    check("report says it is not a pass", "NOT a pass" in report, True)


def test_a_real_verdict_is_not_mistaken_for_unverified():
    report = qc.format_report("DSLF-1", {
        "verdict": qc.PASS,
        "select": {"verdict": qc.PASS, "findings": [], "delivered": "d",
                   "model": "m", "elapsed_s": 1.0},
        "select_filename": "S.pdf"})
    check("a PASS report does not match the UNVERIFIED pattern",
          bool(re.search(r'^VERDICT:\s*UNVERIFIED', report, re.MULTILINE)), False)


# ---------------------------------------------------------------------------
# 5. Report contents
# ---------------------------------------------------------------------------

def test_pass_report_says_so_explicitly():
    """The ticket gets a comment even when it is clean — that is the point of posting."""
    report = qc.format_report("DSLF-1", {
        "verdict": qc.PASS,
        "order": {"verdict": qc.PASS, "findings": [], "delivered": "matches",
                  "model": "m", "elapsed_s": 2.0},
        "order_filename": "order.pdf"})
    check("clean ticket is told so", "Checked and correct" in report, True)
    check("no-findings line present", "nothing wrong found" in report, True)


def test_nothing_attached_is_not_a_pass():
    report = qc.format_report("DSLF-1", {"verdict": qc.UNVERIFIED})
    check("no PDFs at all is not a pass", "NOT a pass" in report, True)


def test_forced_fail_is_disclosed():
    report = qc.format_report("DSLF-1", {
        "verdict": qc.FAIL,
        "select": {"verdict": qc.FAIL, "verdict_forced": True, "blocking_count": 1,
                   "delivered": "d", "model": "m", "elapsed_s": 1.0,
                   "findings": [{"field": "Client Database", "severity": "WRONG",
                                 "ticket_value": "A", "select_value": "B",
                                 "expected": "A", "issue": "mismatch"}]},
        "select_filename": "S.pdf"})
    check("forced verdict disclosed", "verdict forced" in report, True)
    check("both sides quoted", "A" in report and "B" in report, True)


def test_fix_section_distinguishes_applied_from_refused():
    report = qc.format_report("DSLF-1", {
        "verdict": qc.FAIL,
        "order": {"verdict": qc.FAIL, "findings": [], "delivered": "d",
                  "model": "m", "elapsed_s": 1.0},
        "order_filename": "o.pdf",
        "fixes": {"applied": ["mailer_po: (empty) -> CRU 924-105"],
                  "refused": ["client_db: not auto-fixable"]}})
    check("applied fixes listed", "WOULD FIX" in report or "FIXED" in report, True)
    check("refused fixes listed", "SKIPPED" in report, True)


# ---------------------------------------------------------------------------
# 6. The prompts still carry the load-bearing domain rules
# ---------------------------------------------------------------------------

def test_profile_context_carries_the_dollar_cap():
    """Without the cap, every correctly-executed capped pull reads as lost records."""
    ctx = qc._profile_context({"client_db": "W12D"})
    check("cap is in the prompt", "Dollar cap" in ctx, True)
    check("N11D's $99.99 cap reaches the prompt",
          "$99.99" in qc._profile_context({"client_db": "N11D"}), True)


def test_profile_context_handles_an_unknown_database():
    ctx = qc._profile_context({"client_db": "ZZ9D"})
    check("unknown db says it cannot verify", "could not be verified" in ctx
          or "cannot confirm" in ctx, True)
    check("blank db_code handled", isinstance(qc._profile_context({}), str), True)


def test_select_prompt_keeps_the_rules_the_regex_checker_knew():
    s = qc._SYSTEM_SELECT
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
    s = qc._SYSTEM_ORDER
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
    s = qc._SYSTEM_ORDER
    check("KAP is not exempted from the title rule",
          "kap tickets are the exception" in s.lower(), False)
    check("the old KAP title shape is called out as wrong",
          "p.o. dl984" in s.lower() or "P.O. DL" in s, True)
    check("every broker including KAP is stated",
          "including kap" in s.lower(), True)


def test_no_fix_field_leaks_into_the_select_check():
    """A bad pull needs re-running. Editing the ticket to match it erases the evidence."""
    props = qc._schema(False)["properties"]["findings"]["items"]["properties"]
    check("SELECT findings have no fix_field", "fix_field" in props, False)
    props = qc._schema(True)["properties"]["findings"]["items"]["properties"]
    check("ORDER findings do have fix_field", "fix_field" in props, True)


# ---------------------------------------------------------------------------
# 7. Exit codes — what keeps the scheduled build green
#
# The live Jenkins job is a freestyle Execute-shell step whose script lives in the Jenkins
# config, NOT in this repo's Jenkinsfile, and it calls `python qc_checker.py` under
# `sh -xe`. A non-zero exit reds the build. A ticket failing QC is a RESULT, not a build
# error — the rule-based checker this replaced returned None and so always exited 0.
# Non-zero is reserved for "the scan could not run".
# ---------------------------------------------------------------------------

def test_findings_do_not_fail_the_build():
    """DSLF-1135 fails QC right now; the cron must still go green."""
    saved = qc.scan
    try:
        qc.scan = lambda status, **kw: [
            {"ticket_key": "DSLF-1", "verdict": qc.FAIL, "report": "r"},
            {"ticket_key": "DSLF-2", "verdict": qc.UNVERIFIED, "report": "r"},
        ]
        sys.argv = ["qc_checker.py", "--dry-run"]
        check("FAIL and UNVERIFIED still exit 0", qc.main(), 0)
    finally:
        qc.scan = saved


def test_a_scan_that_cannot_run_fails_the_build():
    """config_guard exits on a bad YAML; argparse exits on bad args. Both must go red."""
    saved = qc.main
    try:
        def boom_exit():
            raise SystemExit(1)
        qc.main = boom_exit
        try:
            qc._entry()
            check("SystemExit fails the build", "returned", "SystemExit")
        except SystemExit as e:
            check("SystemExit fails the build", e.code, 1)

        def boom():
            raise RuntimeError("jira exploded")
        qc.main = boom
        check("an unexpected crash fails the build", qc._entry(), 1)
    finally:
        qc.main = saved


def test_posting_is_the_default_and_dry_run_suppresses_it():
    """`python qc_checker.py` with no arguments is exactly how the cron invokes it."""
    saved = qc.scan
    seen = {}
    try:
        qc.scan = lambda status, **kw: (seen.update(kw), [])[1]
        sys.argv = ["qc_checker.py"]
        qc.main()
        check("bare call posts", seen["post"], True)
        check("bare call does not fix", seen["fix"], False)

        sys.argv = ["qc_checker.py", "--dry-run"]
        qc.main()
        check("--dry-run suppresses posting", seen["post"], False)
        check("--dry-run is passed through", seen["dry_run"], True)
    finally:
        qc.scan = saved


def test_no_budget_cap_exists_any_more():
    """The cap was removed on request — a queue runs to completion, however long."""
    check("no QC_BUDGET_S constant", hasattr(qc, "QC_BUDGET_S"), False)
    check("no per-run spend counter", hasattr(qc, "_spent_s"), False)


def test_the_select_parser_lives_here_too():
    """One QC file: the SELECT regexes moved in when select_pdf.py was deleted."""
    for name in ("parse_select_pdf", "find_select_attachment", "_parse_shipping_info"):
        check(f"{name} is importable from qc_checker", hasattr(qc, name), True)


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
