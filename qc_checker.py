"""
Compatibility shim. The QC checker itself is `qc_llm.py` — this file only exists so the
scheduled build keeps working.

WHY THIS FILE IS HERE
The Jenkins job "DSLF-Email-Scanner" is a **freestyle Execute-shell step**, not a Pipeline
job, and its script is stored in the Jenkins job config rather than in this repo. It runs:

    rm -rf email_to_jira && git clone git@github.com:rkdgroup/email_to_jira.git
    cd email_to_jira && python3.11 -m venv env && . env/bin/activate
    pip3 install -r requirements.txt
    cp <credential> .env
    python email_scanner/email_scanner.py
    python qc_checker.py            <-- this file

**The `Jenkinsfile` in this repo is NOT what runs.** Editing it changes nothing. When the
rule-based `qc_checker.py` was deleted on 2026-08-27 the build broke on that last line, and
the fix that mattered was this shim, not the Jenkinsfile edit. Verify which config actually
drives a build before assuming the file in the repo does.

The real fix is one line in the Jenkins job config:

    QC_BUDGET_S=180 python qc_llm.py --post

Once that lands, delete this file.

TWO THINGS THIS SHIM DOES BEYOND FORWARDING

1. **It exits 0 on QC findings.** `qc_llm.main()` returns 1 when any ticket comes back FAIL
   or UNVERIFIED. The job runs under `sh -xe`, so that return code would mark the whole
   build red every time a ticket legitimately fails QC — and a QC finding is a result, not
   a build error. The old rule-based `main()` returned None and so always exited 0; this
   preserves that. A broken config or an unexpected exception still fails the build, which
   is the distinction that matters: the scan not running IS a build failure, the scan
   finding problems is not.

2. **It caps the budget.** The old checker made no API calls and finished in seconds. Two
   LLM calls per ticket at ~50s each is ~100s/ticket, so an uncapped run over a ten-ticket
   queue takes ~17 minutes on a five-minute cron. QC_BUDGET_S=180 keeps it to roughly two
   tickets per tick; the rest come back UNVERIFIED and are retried on the next run rather
   than being skipped (see _last_qc_comment_time in qc_llm). An explicit QC_BUDGET_S in the
   environment wins.
"""

import os
import sys
import logging
import traceback

_DEFAULT_BUDGET_S = "180"


def main() -> int:
    # Must be set before qc_llm is imported — it reads QC_BUDGET_S at module level.
    os.environ.setdefault("QC_BUDGET_S", _DEFAULT_BUDGET_S)

    argv = sys.argv[1:]
    # The old checker always posted its verdict. Keep that, unless the caller has already
    # said what it wants.
    if not any(a in ("--post", "--dry-run") for a in argv):
        argv = argv + ["--post"]
    sys.argv = [sys.argv[0]] + argv

    import qc_llm

    try:
        verdict_code = qc_llm.main()
    except SystemExit:
        # config_guard.validate_configs_or_exit() and argparse both exit this way. Those
        # mean the scan could not run, so let them fail the build.
        raise
    except Exception:
        traceback.print_exc()
        logging.getLogger(__name__).error("QC scan crashed — failing the build")
        return 1

    if verdict_code:
        print("\n(qc_checker: tickets failed or went unverified — that is a QC result, "
              "not a build failure; exiting 0)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
