# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Incident stories (DSLF-xxxx), measurements and dated rationale were moved to
`docs/DSLF_REFERENCE.md`. This file keeps the rules and the reason in one clause; read the
reference when a rule needs its backstory.

## Project Overview

DSLF List Rental Pipeline — processes purchase order PDFs from brokers, extracts structured fields via **rule-based** parsing (one parser per broker), enriches from `config/*.yaml` lookups, and creates DSLF tickets in Jira (rkdgroup.atlassian.net, project DSLF, issue type 11806).

**Every structured field is rule-based.** Claude is called on the default path only by `tools_polish.py`, which structurally cleans the two prose fields (Description, Omission Description) after parsing. A PDF matching none of the 12 broker fingerprints is **flagged for review and creates no ticket** — `process_pdf` has no Claude fallback.

## LLM Model Policy

**One pin for every Claude touchpoint: `claude-opus-5` at `medium` effort.** Set in `tools_polish.POLISH_MODEL` (effort is a bare `"medium"` literal in `polish_fields()`), `qc_checker.QC_MODEL`/`QC_EFFORT`, `LLM_writes.DEFAULT_MODEL`/`DEFAULT_EFFORT`, `ai_extract.DEFAULT_MODEL` (+ `extract_fields_from_pdf(effort=)`), and the `--model` defaults in `compare_extraction.py` and `hybrid_create.py` (grep `claude-opus-5`). Change them together or they drift.

- A new model needs a rate in **both** `qc_checker._RATES_PER_MTOK` (QC comment footer) and `usage_log._RATES` (usage log); a missing model prices at 0. `qc_checker` prices the model it *requested*; `usage_log` the model the API *returns* (`resp.model`), so a returned snapshot ID needs its own `usage_log._RATES` row.
- Every Claude call except `tools_polish` writes a row to `logs/api_usage.jsonl` (gitignored) via `usage_log.record()`. `usage_log.py` is a deliberate **copy** of `LRF_Processing/order-processor/usage_log.py` — keep the row shape identical.
- `tools_polish` omits the effort parameter for Haiku (4.5 predates it and errors). That guard is not a second pin.

| Where | Module | Role |
|-------|--------|------|
| Live pipeline, every ticket | `tools_polish.py` | structural prose clean, gated, falls back to parser text |
| Jenkins-scheduled QC | `qc_checker.py` | the only QC: order-vs-ticket (fixable) + ticket-vs-SELECT |
| Manual create, opt-in | `LLM_writes.py` | Claude extracts **all** fields for an unrecognized broker |
| Offline, never scheduled | `ai_extract` / `compare_extraction` / `hybrid_create` | see "AI-Assisted Offline Tools" |

## Commands

```bash
python parse_pipeline.py /path/to/order.pdf              # extract → parse → validate → dup check → create ticket
python parse_pipeline.py /path/to/folder/                # batch; globs *.pdf + *.PDF, no dedupe (on Windows every PDF runs twice)
python parse_pipeline.py /path/to/order.pdf --dry-run    # no ticket, no dup check, no INSERT
```

`--dry-run` and `--verbose` are the **only** CLI flags. `broker_hint` is a function argument (email scanner), not a flag. A dry run is **not offline**: it can still make the live `tools_polish` call and a read-only IBM i lookup of the next WO#. It returns `result["fields"]` and `result["wo_number"]`. The printout is not the ticket's Description (`_print_result` drops `profile_data` and nested bullets) — read `result["fields"]["description"]` for the real ADF.

**Windows console**: `parse_pipeline.main()` never reconfigures stdout, so printing fields can raise `UnicodeEncodeError` under cp1252. Prefix runs with `PYTHONIOENCODING=utf-8`.

**Dev is Windows, Jenkins is POSIX.** The `Jenkinsfile` uses `sh`, `python3`, `pip3`. Anything shell-shaped added to the build must be POSIX.

**Sample folders** `broker_pdf/`, `Test_pdf/`, `AMLC/` are gitignored and absent from a fresh clone — ask for sample PDFs.

### Testing

No linter and no CI test stage — Jenkins never runs the tests. 24 hermetic regression files (22 in the root, 2 in `WO#/`): no Jira, DB, PDFs or network. `pytest` is not in `requirements.txt`; install it separately.

```bash
python -m pytest -q test_*.py "WO#/"                          # everything (bash / Git Bash)
python -m pytest -q $(Get-ChildItem test_*.py -Name) "WO#/"   # the same, PowerShell
python -m pytest -q test_kap_fields.py -k below               # one file / one test
```

Eighteen files are also standalone runners (`python test_x.py` prints `PASS`/`ALL PASSED`). **Six are pytest-only** and exit 0 having run nothing under `python`: `test_adf_move.py`, `test_usage_log.py`, `test_scanner_host_lock.py`, and the three `test_qc_*` excuse files (`test_qc_household_no_set.py`, `test_qc_unverifiable_cap.py`, `test_qc_title_tag.py`). `test_qc_checker.py` is a partial runner — its `omission_move` tests sit after the `__main__` guard, so only pytest runs them. The `WO#/` tests (`test_work_order_allocation.py`, `test_ibmi_credentials.py`) run standalone too.

Run the matching test file after touching: `tools_jira.py` ship-to rules, `parsers/{kap,adstra,data_axle,rmi_direct,celco}.py`, `qc_checker.py`, `parse_pipeline` (`_build_adf_description`, `_dup_check_key`, `_create_and_link_work_order`, `_strip_ncc_dnm`, `_with_household`), `qty_approval_scanner.py`, `WO#/work_order.py`, `WO#/base.py`, `adf_move.py`, `usage_log.py`, `cost_report.py`, `build_profile_yaml.py`, `email_scanner.run_scan` / `ALLOWED_HOSTS`. Everything else is checked manually via `--dry-run` on real PDFs. `tools_polish._validate` (the live polish gate) has no test file.

### Other entry points

```bash
python email_scanner/email_scanner.py                 # one poll of the shared mailbox
python qc_checker.py [DSLF-123 ...] [--status S] [--no-fix] [--dry-run]
                     [--order-only|--select-only] [--model M] [--effort E] [--json f]
python qty_approval_scanner.py [--no-email-scan] [--combined] [--output f] [--email a] [--cc b] [--subject s]
python ticket_scanner/ticket_scanner.py [--loop N] [--reset] [--learn] [--reporter NAME]
python handoff.py [--hours N] [--send] [--to a] [--cc b]  # EOD report, Windows only, read-only against Jira
python config_guard.py        # fast syntax gate over config/*.yaml (exit 1 on parse error)
python verify_configs.py      # semantic audit of YAMLs vs source → config_audit_report.md
python build_profile_yaml.py  # OVERWRITES config/client_profiles.yaml — see Config System
python backfill_dollar_cap.py [DSLF-123 ...] [--status S] [--live]   # dry run unless --live
python cost_report.py [--month YYYY-MM] [--live]      # dry run unless --live; export JIRA_EMAIL/JIRA_API_TOKEN first (never reads .env)
python LLM_writes.py order.pdf [--live] [--model M] [--effort E] [--json f]   # INVERTED: dry run unless --live
python compare_extraction.py DSLF-916 [--pdf f] [--md f] [--json f]   # read-only diff
python hybrid_create.py order.pdf [--dry-run] [--no-claude] [--no-attach]   # --dry-run first!
```

`handoff.py --send` mails through classic Outlook (pywin32) to `HANDOFF_EMAIL_TO`/`HANDOFF_EMAIL_CC`. `cost_report.py` posts the monthly Claude spend as a DSLF Task (issue type 11772) read from the `$x.xxx` footer of QC comments (the jsonl is wiped by every Jenkins re-clone).

## Dependencies & Credentials

`pip install -r requirements.txt` — the same file Jenkins installs (`pip3 install -q -r requirements.txt`). A new runtime import missing from it breaks the scheduled run, not the local one. Not in it: `pytest`, and `pyodbc` (only for the ODBC one-off `WO#/` scripts).

| Vars | Used by |
|------|---------|
| `JIRA_BASE_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN` | Everything (Jira REST); `cost_report.py` reads process env only |
| `MS_CLIENT_ID`, `MS_CLIENT_SECRET`, `MS_SERVICE_ACCOUNT`, `MS_SERVICE_PASSWORD`, `MS_TENANT_ID` (default `common`) | email + qty scanners (MSAL ROPC); the qty scanner reads/sends from the service account's own mailbox |
| `IMAP_EMAIL` | `email_scanner` only: the shared mailbox |
| `IBMI_HOST`, `IBMI_USER`, `IBMI_PASSWORD` (+ optional `IBMI_JT400_JAR`) | work-order creation |
| `ANTHROPIC_API_KEY` | `tools_polish`, `qc_checker`, `LLM_writes`, offline AI tools |
| `HANDOFF_EMAIL_TO`, `HANDOFF_EMAIL_CC` | `handoff.py --send` recipients |

A missing `ANTHROPIC_API_KEY` degrades prose quality and makes every QC ticket `UNVERIFIED`; it does not break ticket creation. `jaydebeapi` + `JPype1` + `jt400.jar` power the IBM i step.

The `.env` Jira token **can create and edit tickets** (POST, PUT, comment, attach) but **is not the Jenkins token and they can diverge** (local 401 while Jenkins kept creating). `GET /rest/api/3/myself` before a live script that writes Jira. The Atlassian MCP connector authenticates as the same Jira Service Account and is the fallback for reads and field writes.

## Architecture

`parse_pipeline.py` has a **head** and a **shared tail**; the seam matters:

- `process_pdf()` — detect the broker and parse.
- `finalize_and_create(result, pdf_path, text, …)` — everything after a `ParseResult` exists. It never looks at the parser or broker registry, so any producer can use it (`LLM_writes.py` is the second). **Put new post-parse behavior here, not in `process_pdf`**, or the LLM path silently misses it. Its `profile_blocks_to_omission=True` flag (LLM_writes only) routes profile suppression blocks to the Omission field instead of the Description.

```
PDF → [tools_pdf] extract text (PyMuPDF; pdfminer fallback only if PyMuPDF <50 chars)
    ┄┄ process_pdf ┄┄
    → [parsers/__init__] detect_broker() — ALL of a broker's regexes must match within
      first 3000 chars; _RULES order, first full match wins. No match → flagged, no ticket.
    → [parsers/<broker>.py] rule-based parse → ParseResult (confidence 0.92)
    ┄┄ finalize_and_create ┄┄
    → validate_result()  ⚠ ADVISORY
    → duplicate check (live only)
    → [client_lookup] enrich db_code/billable/list_manager from config/*.yaml
    → [client_profiles] profile file (attached) + profile_data (select_by/flags/…)
    → [tools_polish] polish segment_criteria + omission (falls back to parser text on any failure)
    → build kwargs + ADF description + FLAG omits
    → dry-run: return {"fields", "wo_number"}  |  live: create ticket + WO# + attach PDF/profile
```

Load-bearing behaviors that are easy to get wrong:

- **Household dedupe runs for every broker, after the parser.** Parsers keep only OMIT-ish lines, so "one per household" lines were dropped. `_with_household` reads raw text with `parsers.base.HOUSEHOLD_RE` and appends `1 PER HOUSEHOLD` unless the omission already says it or the line says DO NOT. Never `2 PER HH`.
- **Validation is advisory.** `validate_result()` aborts only at `confidence == 0.0`, which no producer returns (parsers 0.92, `LLM_writes` 0.85), so a missing PO, list manager or quantity logs "proceeding" and a **partial ticket is still created**. Real blocks are upstream: no broker match, unreadable PDF, parser exception. **Exception — the title:** a blank `list_name` or `mailer_name` leaves `summary` unset, and live `create_jira_ticket(**kwargs)` raises `TypeError` outside any try (no ticket, a multi-page PDF loses later pages, email goes to Failed). `--dry-run` never shows this.
- **ADSTRA `_STANDARD_OMITS` is hardcoded in the parser** and prepended to every ADSTRA order, including `NCC DNM FILE FOR LIST RENTAL (W/O 222222)`, which **N11D does not suppress against**. `_NCC_DNM_EXCLUDED_DB_CODES` (`{"N11D"}`) strips it after enrichment (the parser has no db_code yet). Deliberately narrow, not profile-driven — profiles record this for only 41 of 195 clients. Pinned in `test_ncc_dnm_omit.py`.
- **`SKIP_DB_CODES`** (`{"A63D"}`): orders resolving to these create **no ticket** — `{"success": True, "skipped": True}`, live and dry-run. Separate `_ADSTRA_SWEEPS_EXCLUDED = {"A63D","N11D"}` only controls attaching the ADSTRA sweeps profile.
- **Multi-page PDFs**: every broker **except ADSTRA** splits into one ticket per page and `process_pdf` returns a **list** of result dicts; ADSTRA is merged into one order. Callers must handle the list.
- **Duplicate check** (live only, `_dup_check_key`): JQL on `cf[12193]` Mailer PO, falling back to `cf[12192]` Manager Order # when there is no PO — **AMLC keys on Manager Order # outright** (its columnar layout puts someone else's number in the PO). Skipped in dry-run or when neither key exists. A failed search (`search_jira_tickets` returns `{"error": …}`, no `total`) **fails closed**: no ticket, flagged, email to Failed. Jira's index lags a create by seconds, so the check also consults `_created_this_run`.
- **`search_jira_tickets()` has no real total** — `/rest/api/3/search/jql` is token-paginated; `total = len(issues)` from one page (`max_results=10`). Fine for `total > 0`, wrong for counting; use `search_issues_paged()`.
- **Description is NOT raw PDF text** — see Field Rules. The PDF is preserved by attaching it.
- **The WO step and all attach steps swallow exceptions** (log + continue): a ticket can succeed with its WO#, PDF or profile attachment silently failed.
- **Zip-omit splitting** (`tools_zip_omit.py`): a supplementary `.xlsx/.xls/.csv/.txt` with more than `ZIP_CHUNK_SIZE` (9500) zips is also attached as `{stem}_zips_{i}of{n}.xlsx` chunks; the original stays. `parse_pipeline` Step 8 picks only `.xlsx/.xls/.csv` in the PDF's folder whose name holds the Manager Order # or Mailer PO; the email scanner's `other_atts` loop passes every non-PDF attachment.

## Scheduled Automation

**Only `email_scanner` + QC are Jenkins-scheduled** — by a freestyle Execute-shell job whose script lives in the Jenkins config, **not** this repo's `Jenkinsfile`. It runs `python email_scanner/email_scanner.py` then `python qc_checker.py`. `qty_approval_scanner` is manual/emailed; `ticket_scanner` uses a Windows Task Scheduler `.bat`.

| Tool | Trigger / scope | Behavior |
|------|-----------------|----------|
| `email_scanner/email_scanner.py` | Shared-mailbox `List Rental` folder | MSAL ROPC auth → per message: if `conversationId` is in `thread_map.json`, add the HTML-stripped body as a comment on the thread's first ticket (no attachments downloaded, mail not moved); else download PDFs (or synthesize one from the body) → `process_pdf(broker_hint=SENDER_BROKER_MAP[domain])` → move to `List Rental/Processed` or `/Failed`. `broker_hint` skips fingerprint detection. |
| `qc_checker.py` | `Needs QC` tickets **assigned to the service account** (`--status` for other queues; named tickets bypass the scan) | Two LLM checks per ticket; posts a comment on every ticket checked, pass included; **never transitions**; order-check corrections are written back by default (`--no-fix` to report only). See "QC". |
| `qty_approval_scanner.py` | `Ready to Send for Qty Approval` tickets | Reads `QTY APPROVAL/<order#>` emails → sets Requested Quantity (`cf[12271]`). The SELECT `TOTAL RECORDS SELECTED` count only fills the emailed digest, never the field directly. **Never transitions.** One digest per (mailer, requestor). Subject carries a LIST code (single ticket) or MAILER code (group), resolved from `dslf_list_and_mailer_names.txt` with an initials fallback. Body is `<order#> = <qty>` lines + `Requestor: <email>` — the shape the reply scan parses back, so change one, change both. The scan reads one order per email (subject's first token); a collapsed range (`J1000-J1003`) sets none. |
| `ticket_scanner/ticket_scanner.py` | New DSLF tickets (issue# > saved state) | **Read-only** audit → `ticket_scanner/reports/`. `--learn` mines List Name→db_code into `learned_patterns.json` (enrich tier 5) but pages `/search/jql` by `startAt`/`total`, which that endpoint ignores, so each fetch stops at 50. |

`email_scanner.main()` has **no argparse** (`--loop` is a silent no-op). SKIP_DB_CODES emails are deliberately left in `List Rental`. `email_scanner.py` and `qc_checker.py` call `config_guard.validate_configs_or_exit()` first.

**⚠ The qty scanner approves from its own outbox.** `scan_approval_emails` runs `$search "subject:QTY APPROVAL"` over the whole service-account mailbox (top 100, no folder/sender filter), and `send_email` saves every digest to Sent Items in exactly the approval shape. The next run reads its own digest as an approval and writes its number to the subject's first order if still queued. The Inbox has never held a real approval (the digest's `replyTo` is the Heller address). 17 tickets got Requested Quantity written this way. Unfixed.

### Email scanner specifics

- **`SCANNER_ENABLED` is the kill switch.** `False` makes `run_scan` log a warning and return before reading mail (exit 0, so QC still runs). It reaches only runners that re-clone `main`, as Jenkins does: flip it, push, the next build picks it up.
- **`ALLOWED_HOSTS = {"rkdc2-dmiappsdev01"}`** locks the scanner to that Jenkins box. `run_scan` compares `platform.node()` (first label, lower-cased) and exits 1 on any other host before touching the mailbox (under `sh -xe` that also skips `qc_checker`). It exists because a second, never-located runner created every order twice. **Moving Jenkins means adding the new hostname first**, or the first build there goes red with `Refusing to run on host`. Pinned in `test_scanner_host_lock.py`.
- **Runtime state is two gitignored JSON files** next to the script: `processed_ids.json` and `thread_map.json` (`conversationId` → ticket key). Deleting them re-processes the whole folder and follow-ups spawn **new tickets** instead of comments.
- **Scan window**: top **50** messages in `List Rental`, `receivedDateTime asc`, no paging, minus `processed_ids.json`. **Only moved mail leaves the folder.** Thread follow-ups, SKIP_DB_CODES mail, unrecognised-broker PDFs, empty bodies and escaped errors are marked processed but stay and still hold window slots — once 50 such are the oldest, new mail is never fetched. File them by hand.
- **Non-PDF attachments** route by a service-bureau range in the filename (`AMLC #668769-668774 ZipOmits.xls` → only tickets whose `manager_order_number` is in range, `_resolve_attachment_targets`). No range → attaches to **every** ticket from that email.
- **No PDF attached** → `_generate_pdf_from_text` builds one from the plain-text body (`Prefer: outlook.body-content-type="text"` so HTML bloat cannot push the fingerprint past the 3000-char window). Over ~49 lines becomes a multi-page PDF, split per page (non-ADSTRA); later pages parse as their own orders — a partial ticket if they still yield list + mailer name, else the blank-title `TypeError` and the email goes to Failed.
- **Jenkins credentials come from a copied `.env`**: the job `cp`s a full credential file in, which supplies `MS_*` (hard requirements of `get_access_token()`, `sys.exit(1)` if missing) and `ANTHROPIC_API_KEY`. The Jenkinsfile's `environment` block is not the credential picture — nothing runs it.

## Config System

**`config/*.yaml` is the runtime source.** Runtime (`client_lookup.py`, plus `parse_pipeline`/`qc_checker` for profile and ADSTRA files) reads only the YAMLs, never the Excel. `client_profiles.find_profile` still scans the gitignored `Client Profiles/` folder to pick the file to *attach*, so a fresh clone attaches no profile file; profile data still comes from the YAML.

- **Sources**: `NEW LR CLIENT LIST 2026.xlsx` → 15 lookup YAMLs (`full_client_list.yaml` + 14 per-broker); `Client Profiles/**/*.doc(x)` → `client_profiles.yaml`; `Client Profiles/ADSTRA/…xlsx` → `adstra_omit_database.yaml`. The lookup YAMLs and ADSTRA file were built outside the repo and hand-corrected since. The only generator, `build_profile_yaml.py`, **overwrites** `client_profiles.yaml` and drops every hand curation — never run it to refresh, nor to "fix" `verify_configs` diffs.
- `config_guard.py` = fast **syntax** gate (both Jenkins entry points call it); `verify_configs.py` = offline **semantic** audit. Passing the guard does not mean the content is right.
- **Do not hand-edit these YAMLs blindly**: a valid-but-wrong edit (notably `client_profiles.yaml` 2-space list indent reparenting keys) passes `config_guard` and has broken `main`. Edit, then run `config_guard.py` + `verify_configs.py`.
- `enrich_fields()` order (`client_lookup.py`): (0) ADSTRA 5-digit list-code exact, (1) exact db_code, (1.5) abbrev-token exact, (2) broker-YAML fuzzy, (3) cross-broker fuzzy, (4) full-list fuzzy, (5) learned_patterns. **Fuzzy threshold 0.6** (0.5 learned). `broker_only=True` stops after tier 2. An AMLC order the parser marks `T11` (rental, or list name containing VIGUERIE) is forced to `T11R` with no lookup; other AMLC and ADSTRA run `broker_only=True`.
- Orphan YAMLs never read by lookup: `adrianne.yaml`, `jordan.yaml`, `leeann.yaml` — but `config_guard` still globs them, so a syntax error there aborts both Jenkins entry points.

## IBM i Work Orders

On every **live** create, `_create_and_link_work_order()` imports `WO#/work_order.py` (jt400 JDBC via `jaydebeapi`+`JPype1`), INSERTs into `DMIJOBS.ARWRKSCH`, then writes the WO# to `customfield_12089`. It re-reads the billable account from the just-created ticket; skipped if empty. **Failures are non-fatal.** **A dry run calls it too**, read-only (connects to production, reads PEPBK# + MAX(WWORKO) for `result["wo_number"]`), so `--dry-run`, `LLM_writes` without `--live` and `hybrid_create` all need IBM i creds + jt400/JPype or log a swallowed warning. JPype is blocked on the dev box by an Application Control policy — leave `WO#/` alone for that ImportError.

- **`WCCUST` = `letter_pos * 1000 + trailing`**, not a modulo (`K40` → 11040, `T11` → 20011). `WO#/fix_wccust.py` is a record of the repair, not something to re-run.
- **`(WWORKO, WSUFX)` is a DB-enforced composite key**; the pipeline writes a blank suffix. `allocate_and_create()` runs scan → insert → verify → auto-reassign on one connection and reads `PEPBK#` as a read-only allocation floor (the ARWRKSCH trigger advances it). Pinned in `WO#/test_work_order_allocation.py`.
- **A WO failure or skip is posted as a ticket comment** (`_report_wo_failure`: `WORK ORDER NOT CREATED`, host, traceback tail) in its own try/except. **Except a failed write-back**: if the INSERT succeeds but `update_ticket_fields` fails (returns `{"error"}`, never raises), only a log line records it — an ARWRKSCH row with a blank WO# field.
- **A missing WO stalls the ticket on a clock.** LRF_Processing's auto-run comments `Auto-run: NOT RUN — waiting for Work Order #`, re-checks every 15 min for 24 h, then moves it to **STUCK**. Backfill within the day. Whether a STUCK ticket resumes once the WO is filled is **not verified**; STUCK has other causes, so read the last `Auto-run:` comment first.
- **`ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0` on connect = BLANK PASSWORD** (JTOpen reads the password's first char). Confirmed cause on past outages: the Jenkins credential file parsed with no usable `IBMI_PASSWORD`. **No code change fixes it — replace the Jenkins secret file**, with one file holding a working Jira token **and** the `IBMI_*` lines (the local `.env` is not a drop-in). If `IBMI_PASSWORD` comes from credentials binding, inject `IBMI_USER` with it — a missing user falls back to `DMISUVAM`, a real failed sign-on every 5 minutes.
- **Credential resolution** (`base._credentials`): host, user, password are taken as ONE set — from the env when it holds a non-blank password, else from the first `.env` that does. `WO#/base.py` (like `parse_pipeline`, `email_scanner`) runs `load_dotenv(override=False)` at import, so an UNSET `IBMI_*` var is filled from `WO#/.env` / root `.env` first and then reads as `source=env`. The file branch fires only when the env password is still blank/unset after those loads. `get_connection` refuses empty host/user/password with a secrets-free diagnosis that `_report_wo_failure` posts. Pinned in `WO#/test_ibmi_credentials.py`.
- **`jt400.jar` is auto-discovered** (`WO#/base.py:_resolve_jt400`): `IBMI_JT400_JAR`, `/opt/jt400/jt400.jar`, `/var/lib/jenkins/workspace/DSLF-Email-Scanner/jt400.jar`, project root, then a hardcoded Windows RDi path. The jar is **not** in the repo; a new machine needs a path added or the env var set.
- Only `IBMI_PASSWORD` is truly required: `IBMI_HOST`/`IBMI_USER` default to `SYSTEM5.DATA-MANAGEMENT.COM`/`DMISUVAM`, so a missing host silently uses prod.
- **Backfilling missed WOs**: copy only `WO#/_backfill_1348_1364.py` or `_backfill_1343_1347.py` (pyodbc `_connect`, keep `allocate_and_create`, dry run unless `--live`). The five older ones INSERT and write Jira on a bare run — never rerun or copy them. Rules: check ARWRKSCH for hand-keyed rows first, matching each order value against **both** `"WXCOD#"` and `WMAILR` (staff put the broker PO in `"WXCOD#"` under a non-blank suffix; `--check` only matches one way); prove the check query finds a *recent* pipeline WO before trusting a zero; `"WXCOD#"` is CHAR(9), so a longer bound parameter fails the query with ODBC `22001` — inline vetted literals; write the ticket→WO mapping to disk the moment each INSERT returns, before touching Jira, then read both sides back.

## Prose Polish (`tools_polish.py`) — the live LLM step

Runs on every ticket inside `finalize_and_create` (so `LLM_writes` gets it too), before the ADF description is built and FLAG OMITS appended. It cleans the two parser-produced prose values (PDF text, plus ADSTRA's `_STANDARD_OMITS`) because parsers copy PDF text verbatim and inherit its line wrapping.

**Jira has two prose fields. Selection criteria (what to pull) go in Description; everything to be omitted or suppressed goes in Omission Description.**

- **Five permitted operations only**: join a wrapped line, move/split an omit criterion into the omission field, drop a redundant line, drop an empty one, and label a run of bare select criteria. **No rewording, ever.** (`_USER_TEMPLATE` and the module docstring still say "four".)
- **The `Selects:` heading is the one exception to "add nothing"**: over a run of **two or more** consecutive bare fragments (the parsers drop the header), the model may restore it and indent them two spaces. `_ALLOWED_NEW_TOKENS = {"SELECTS"}` is the complete list of words it may introduce.
- **Only parser output is sent.** The profile's `Select By`, `Standard Suppressions`, `Special Instructions` and `FLAG OMITS:` never leave the process; existing code re-attaches them. `_strip_ncc_dnm` runs after polish and matches whole lines, so polish must leave the NCC DNM line intact.
- **The gate, not the model, is the guarantee** (`_validate`): the fact-token set across *both* fields must be identical before and after (apart from `_ALLOWED_NEW_TOKENS`) — moving a criterion between fields is fine, inventing or dropping one is not. Output lines may exceed input by at most one per omit-keyword line plus one per heading emitted. A heading may appear **at most once** and **never in the omission field**.
- **Every failure returns the parser's text unchanged** (no key, budget, timeout, API error, refusal, failed validation) — an Anthropic outage cannot block creation.
- **Jenkins guards**: 20s timeout with `max_retries=1` (up to ~40s/call; a cold first call is ~14s); `POLISH_BUDGET_S` (120s) of summed call time checked before each call, after which remaining tickets skip the pass (~20 tickets/run at ~6s each; a 7-page AMLC PDF is ~40s). Raising the budget means raising the Jenkins job timeout. Watch for `Polish budget of 120s exhausted for this run — skipping`. An in-process cache covers repeated text, but only *validated* results are cached, so a rejected text is re-billed each time it repeats. Inputs under two lines skip the API.

## Pure-LLM Ticket Creation (`LLM_writes.py`) — manual only

For orders **no parser recognizes** (`process_pdf` flags those; `hybrid_create.py` can't help — it starts from `process_pdf`). **Prefer `parse_pipeline.py` for any recognized broker.** Not scheduled.

- **Dry run by default — `--live` creates** (inverse of `parse_pipeline.py` and `hybrid_create.py`).
- **Claude supplies only what is printed on the order.** db_code/Billable/Client DB/Seed DB come from `client_lookup` (Claude's db_code guess is reported, never sent), plus profile blocks, `tools_polish`, dup check, SKIP_DB_CODES, the WO and all attachment steps from the shared tail.
- `CONFIDENCE_LLM_BASED = 0.85` — only non-zero matters (`validate_result()` blocks at exactly 0.0).
- **`_validate_and_fix()` blanks values Jira would reject** before building the frozen `ParseResult`: a `list_manager` outside the 14 (read from `client_lookup._MANAGER_TO_FILE`), an unknown select-option label, a non-`YYYY-MM-DD` date.
- **`_verify_created()` re-reads the ticket after a live create** (unresolvable select options are dropped server-side without failing). `file_format`/`shipping_method`/`ship_to_email` diffs print as `~` (house rules rewrite them), others as `!`.
- **Prompt drift**: its `_SYSTEM` was transcribed from this file and went stale. Stale: Data Axle Key Code from the Order# suffix (also in `ai_extract._SYSTEM`), RMI manager order as unstripped `MGT#`, only the old Ship Label PO rule, no ADSTRA `B/19217` form, "with suffix" Data Axle/Washington Lists POs (also in `qc_checker._SYSTEM_ORDER`). **Fix a field rule in all three prompts** (`LLM_writes._SYSTEM`, `ai_extract._SYSTEM`, `qc_checker._SYSTEM_ORDER`) or the paths disagree.
- Last place to economise on the model: it reads an unrecognized order fresh every run with nothing verifying it. Override with `--model` / `--effort`.

## QC (`qc_checker.py`) — one file, all LLM, both questions

**Exactly one QC file, and it makes API calls** (`qc_llm.py` and `select_pdf.py` are gone). **The name is load-bearing**: the live Jenkins job hard-codes `python qc_checker.py`, so renaming or splitting it breaks the cron.

| | ORDER check | SELECT check |
|---|---|---|
| Question | was the ticket **created** right from the broker order? | did the **pull** deliver the order? |
| Source of truth | the order PDF attached to the ticket | the SELECT report |
| Runs when | a non-SELECT PDF matches a broker fingerprint | a `*SELECT*.pdf` is attached |
| Findings carry a fix | **yes** — `fix_field`/`fix_value`, applied by default | **no**, by design |
| Prompt | `_SYSTEM_ORDER` | `_SYSTEM_SELECT` |

The ticket verdict is the **worse** of the two; `UNVERIFIED` outranks `FAIL`. Flags: `--dry-run` (print only), `--no-fix`, `--order-only | --select-only`, `--status S`, `--model M`, `--effort E`, `--json FILE`.

- **The `Jenkinsfile` in this repo is NOT what runs.** The live job "DSLF-Email-Scanner" is a freestyle Execute-shell step: `rm -rf email_to_jira && git clone …`, `python3.11 -m venv env`, `pip3 install -r requirements.txt`, `cp <credential> .env`, then `email_scanner.py` + `qc_checker.py`. Editing the Jenkinsfile changes nothing. **Verify which config drives a build before assuming the repo file does.**
- **Posting AND fixing are the DEFAULT; `--dry-run` suppresses both.** The cron calls the script bare. The guarantee against bad writes is `_FIXABLE`, not the flag.
- **`main()` returns 0 even when tickets fail** (a failing ticket is a result; non-zero reds the build under `sh -xe`). Non-zero is reserved for "the scan could not run": `config_guard` exit, argparse exit, or an unexpected exception (`_entry` returns 1). Pinned in `test_qc_checker.py`.
- **Prompt caching: breakpoints sit on the system block and on the PDF document block, never top-level.** The request ends in the per-ticket PDF, so an automatic top-level breakpoint lands after it and pays the 1.25x write premium on bytes nothing reads back. The PDF breakpoint pays because LRF_Processing's `auto_runner._run_precheck` sends the same PDF twice. Reordering message blocks silently un-caches (cost, not correctness); `test_usage_log.py` pins both. Compare `cache_read` vs `cache_write` in `logs/api_usage.jsonl`.
- **`scan()` returns only tickets assigned to the service account**, read from `/rest/api/3/myself`, **failing closed by raising** if that read fails (cron exits 1; a caller such as `qc_gate` gets the exception, not `[]`). LRF_Processing's `qc_gate` imports `scan()` and **does transition** what it returns, so `scan()` is a cross-repo interface: keep its signature and fail-closed behaviour. A ticket someone self-assigned is never scanned; name it on the command line to check it.
- **Every report carries its cost, to three decimals** (`model · Ns · $x.xxx`; reports run $0.10–0.18, two decimals would hide the cache effect). A model with no rate on file prices at 0.0 and the footer drops the figure. `cost_report.py` parses that footer and finds comments by `_QC_COMMENT_PREFIXES` + a `"QC CHECK RESULTS"` JQL — **moving the footer or renaming the header makes the monthly report read $0.**
- **No run budget** (~45s/ticket for both checks at medium). If the build times out, raise the Jenkins job timeout rather than reintroducing a cap that silently leaves tickets unchecked.
- **A comment is posted on every ticket checked, pass included** ("Checked and correct — no action needed."). The one exception is a failed Jira read: `UNVERIFIED`, nothing posted. Silence means "not checked".
- **Comment length is enforced twice.** `_one_line` renders `N. SEVERITY  Field: have -> want (source: …)` within 110 chars after the severity (drops the source quote first, then cuts values to 40); `issue` is one clause ≤15 words, `delivered` ≤20 words. `BLOCKING-BLANK` prints as `BLANK`.
- **`UNVERIFIED` is the code's verdict** — no key, timeout, API error, refusal, unreadable/oversize PDF, failed Jira read, unassemblable prompt. It is **not a pass**, and it can still write: `apply_fixes` runs on an `UNVERIFIED` ORDER result, so the deterministic `_household_fix` / `_ship_to_house_fix` can change a ticket on a run where QC did not.
- **An `UNVERIFIED` comment does not count as "already checked"**: `_last_qc_comment_time` returns `None` when the last comment reads `VERDICT: UNVERIFIED`. The guard greps report text, so `format_report` and `_last_qc_comment_time` are coupled (a test pins them).
- **The re-run guard's grace window is 15s on purpose** (`_RERUN_GRACE_SECONDS` only absorbs the QC comment's own write). A wide window made a late-attached SELECT PDF look "unchanged" and never get select-checked.
- **The gate overrides the model**: `_reconcile()` forces `FAIL` when any finding is `WRONG` or `BLOCKING-BLANK`, records `verdict_forced`. `NOTE` never forces a fail.
- **Three code-side excuses run after the gate, SELECT check only** (`review_select`): each downgrades a known false positive to `NOTE`, and `_rescore` turns `FAIL` into `PASS` when nothing else blocks — `_excuse_household` (client DB in `NO_HOUSEHOLD_SET`: no LRF household set exists; drop a code once ops builds one), `_excuse_unverifiable_cap` (no profile and no stated ceiling), `_excuse_title_tag` (REPORT title ends in client initials like `SCF`, never a criterion). Code, not prompt text. Pinned in `test_qc_household_no_set.py`, `test_qc_unverifiable_cap.py`, `test_qc_title_tag.py`.
- **`_profile_context()` sends the client's `dollar_cap` and it is load-bearing.** `$10+` means $10 through *that client's* cap (`$99.99` for 60 clients, `$49.99` for 48, a tail higher, some `NO CAP`, 37 `VARIES PER ORDER`); `10.00 THRU 99.99` against `$10+` is **correct** for a `$99.99` client. A ceiling **below** the cap is `WRONG`, at it is fine, no profile on file is unverifiable rather than wrong.
- **Raise the effort on `QC_EFFORT` first** if QC starts missing faults — a wrong database sends the wrong donor file to the wrong company.
- **SELECT regexes live here too** (`parse_select_pdf`, `find_select_attachment`, `_parse_shipping_info`) because `qty_approval_scanner` needs the record count. `_quantity_context` turns `TOTAL RECORDS SELECTED` into a ready-made conclusion in the prompt, and `criteria_suffix` drives `_excuse_title_tag`. **Any other value read there belongs in a prompt as evidence, never in a PASS/FAIL if-statement.**
- **Known-correct patterns**: `_SYSTEM_SELECT` has a **DO NOT REPORT list** before its VERDICT section (billable-vs-Client-DB prefix mismatch, house-rule ASCII Fixed/FTP, auto STATE OMITS, blank Mail Date/File Format/Other Fees/Key Code/Special Seed, qty mismatch under All Available, profile-sourced suppressions absent from the SELECT, Seed Tracking == Manager Order #, Seed DB = Client DB + S, hosted lists). `_SYSTEM_ORDER` states its exemptions inline and grades some differently (billable mismatch = `NOTE`; Mail Date must match the order) and alone carries the in-house `tlibrarian@` + Email override of an order's `Via: FTP`. Add a new known-good pattern to whichever prompt raises the false positive, or QC fills with noise. In `_SYSTEM_ORDER`, a `FLAG OMITS` **code satisfies the requirement it encodes** (`$` = DMA pander) — the wording need not appear too.

### What `_SYSTEM_SELECT` must keep saying

Never reintroduce an absolute pass threshold (the old rule-based checker used `pass_count >= 4 and not hard_fails`, so 4 passes / 5 fails still passed). `test_qc_checker.py` asserts most of this, but **not** the asymmetric-omit rule, the Nth "must NOT EXCEED" line or the ADSTRA defaults.

- a completed SELECT never legitimately returns **0 records**
- **Nth**: count must not exceed requested quantity; **All Available** skips the comparison
- the **include set** is judged one way only — a standing universe is routinely *wider* than the order; only *narrower* (higher floor, shorter window) is wrong
- **flags/states/zips are asymmetric**: a required omit the SELECT skipped is `WRONG`; an extra omit is a `NOTE`
- flags deferred to prose (`FLAG OMITS: FLAGS LISTED BELOW IN SPECIAL INST.`) are `BLOCKING-BLANK`, not a pass
- when two priced lines name different recencies, **the base `W/GEO` line governs** (`3 MONTH HOTLINE` under `4M $5+ W/GEO` is a 4-month order)
- ADSTRA's per-seed-database flag defaults load from `config/adstra_omit_database.yaml` via `_adstra_flag_context`, appended to the SELECT user message as NOTE-level context (untested)
- **Hosted lists are the one legitimate List Name mismatch** (SELECT prints the host/master account): the database **code** must still match; a name-only difference is a `NOTE`

### Auto-fix (default on; `--no-fix` disables) — order-check findings only

`apply_fixes()` collects findings whose `fix_field` is in `_FIXABLE` and writes them in **one PUT**. The refusals are the design:

- **`client_db` / `seed_db` / `billable_account` are never writable** — they come from `client_lookup`, are select fields resolved against live createmeta, and a wrong write is the worst outcome here. Reported only.
- **`ship_to_email` is never overwritten — it is the destination.** `Ship to: FTP <host>` followed lines later by a notify mailbox is the **normal** shape on WE ARE MOORE / DATA-AXLE orders, and that mailbox is often at a different company, so a model reading one page proposes replacing it with the FTP host and is wrong. **The field holds the notify address; FTP is carried by Shipping Method.** Destination rules belong in `apply_ship_to_rules`. Sanctioned exceptions:
  - **`_ship_to_fill`**: on a ticket already `FTP`, exactly one address the order prints that is not the Requestor Email, written as `FTP NOTIFY: <addr>` and run through `apply_ship_to_rules` (adds `(SATURN CORP)`); refused if those rules would change Shipping Method or File Format, or Requestor Email is blank; dropped when a Shipping Method fix lands in the same pass. It replaces only what is provably not the destination (blank, same address, the Requestor, an address the order never prints); a filled address the order prints is never overwritten (`_ship_to_blocker`). An FTP order with a host/folder and no mailbox carries `FTP NOTIFY: <requestor>` (`tools_jira.ftp_notify_default` at creation; the ORDER prompt exempts it).
  - **`_ship_to_house_fix`**: rewrites an FTP Ship To into house wording with no model finding, only where `apply_ship_to_rules` would reword it (`FTP NOTIFY:` + `(SATURN CORP)` on Saturn orders, the prefix on `incoming.files@data-axle.com`); refuses any change of address, method or format.
- **`description` / `omission` are never overwritten** (a field-level overwrite flattens the ADF bullet structure). Three sanctioned edits:
  - `fix_field: "omission_add"` (`_omission_add`): appends one omit the order prints **verbatim** (ignoring case, list markers, line wraps) that carries an OMIT/SUPPRESS/EXCLUDE/DEDUPE/DUPE/PANDER word, no dollar amount or recency, no DO NOT / DON'T, and is not already there.
  - `_household_fix`: appends `1 PER HOUSEHOLD` when `parsers.base.household_asked` finds it on the order and the Omission lacks it; reported as `household:` so the auto-runner never counts it as curing another Omission finding.
  - `fix_field: "omission_move"` (`adf_move.plan_move`): drops exactly one matching node from the Description and appends it as one paragraph to the Omission in the same PUT. One move per pass, never at `NOTE`; a line that is absent, appears more than once, or is already in the Omission is refused. Pinned in `test_adf_move.py`.
- A **select option** Jira cannot resolve is dropped server-side *without failing*, so `_validate_fix` checks every option against `AVAILABILITY_RULE_OPTIONS` / `FILE_FORMAT_OPTIONS` / `SHIPPING_METHOD_OPTIONS` and sends `{"id": ...}`.
- `list_manager` must be one of the 14, dates `YYYY-MM-DD`, quantities plausible positive integers, `seed_tracking` must equal the ticket's current Manager Order # (unchecked when blank).
- **`NOTE` findings are never applied**, an empty `fix_value` is refused, a second fix for the same field is ignored.
- Every refusal prints as `SKIPPED  <field>: <reason>` after the `FIXED` lines (`WOULD FIX` under `--dry-run`).

`knowledge.md` (a spec for a separate hosted checker) was **absorbed** into `_SYSTEM_ORDER`. Its claim that KAP titles are `P.O. {DL#} {LIST NAME}` "by design" was a bug and is deliberately not carried over; `test_qc_checker.py` asserts it never returns.

## AI-Assisted Offline Tools

Auxiliary, **not part of the live pipeline**; all need `ANTHROPIC_API_KEY` and follow the model policy.

| Tool | Purpose |
|------|---------|
| `ai_extract.py` | `extract_fields_from_pdf()` — base64 PDF → Claude structured output (`DSLF_SCHEMA`). Module import only; rejects PDFs > 32 MB. |
| `compare_extraction.py` | **Read-only**: a ticket's fields + PDF → Claude → field-by-field diff. Never writes to Jira. |
| `hybrid_create.py` | `process_pdf(dry_run=True)` for rule-based kwargs, merges Claude's Description prose → **live create**. |

⚠ **`hybrid_create.py` (non-dry-run) calls `create_jira_ticket` directly**: no duplicate check, no work order (LRF stalls it), no profile/zip attachments; it crashes on a multi-page non-ADSTRA PDF and on a SKIP_DB_CODES order. **Always `--dry-run` first.**

## Field Rules

- **Title**: `{LIST NAME} - {MAILER NAME} - {MANAGER ORDER NUMBER}` (never Mailer PO while a Manager Order # exists; `ParseResult` falls back to the PO when it is blank, and leaves the title empty when list or mailer name is blank). e.g. `JUDICIAL WATCH DONORS - HERITAGE FOUNDATION - W74926JW`
- **Description**: an **ADF document** of `segment_criteria` plus the profile's `Select By`, `Dollar Cap`, `Standard Suppressions`, `Special Instructions`. **Not raw PDF text** — raw text goes separately as `create_jira_ticket(order_text=…)` and is used only for the Saturn rule.
  - **`Dollar Cap:`** is written on every ticket with one on file, right after `Select By:`, verbatim from the profile's `dollar_cap` (`$99.99`, `NO CAP`, `VARIES PER ORDER` — never normalised). Without it neither a human nor QC can tell a correct capped pull from one that lost donors. No cap on file → no line. `backfill_dollar_cap.py` corrected older tickets.
  - **Indentation in `segment_criteria` is structural.** In `_build_adf_description` a run of indented lines becomes an ADF `bulletList` under the paragraph above (Jira collapses leading whitespace, so a plain-string indent is invisible). Polish strips input indentation and keeps only the model's own indents, so a parser-indented run (KAP's BELOW SCFs) stays a bullet list after polish only if the model re-indents it.
- **Omission Description** (`cf[12270]`, ADF): flags, states, zips/SCFs, "OMIT PREVIOUS ORDER", "1 PER HOUSEHOLD", plus profile `FLAG OMITS:`. Takes a pre-built ADF dict **or** a plain string (split into one paragraph per line).
- **List Manager** = one of exactly: ADSTRA, AALC, AMLC, CELCO, CONRAD, DATA-AXLE, KAP, MARY E GRANGER, NEGEV, NAMES IN THE NEWS, RKD, RMI, WASHINGTON LISTS, WE ARE MOORE
- **Mailer Name** = organization sending the mail; **List Name** = donor list being rented, stored as the abbreviation (FAIR = Federation for American Immigration Reform). Never swap. Order forms use "Mailer" and "Broker" interchangeably — read the value, not the label.
- **Availability Rule**: "Full Run" = "All Available", "NTH NAME" = "Nth".
- **Other Fees**: parsers write `State Omits` when their omission has 6+ states/zips/SCFs (state count + 3-5-digit-number count; OR/IN/ME/HI count as states) — automatic; QC/`ai_extract` prompts say `STATE OMITS`, so both casings occur. Data Axle / SimioCloud keep the order's printed `Other Fees:` first. Counted once per parser on raw tokens, never redone after polish or FLAG OMITS.
- **File Format**: `create_jira_ticket` defaults to **ASCII Delimited** (after the ASCII-Fixed forcing rules).
- **Special Seed Instructions**: only "Insert:" lines, never FTP/email info, blank for most orders. That is the rule, not the code: `_extract_special_seed_instructions` takes the first non-blank text after the first `Insert` / `SEED INSTRUCTION(S)`, any case, no FTP/email filter, called only by ADSTRA, RKD/AMLC, CONRAD, NIN and WE ARE MOORE.
- **Status on creation**: always "Needs Assignment". Never transition on creation.

**KAP ship-to**: only the Ship To block decides the destination (it can be 15 lines below the label), except the real FTP drop-box `incoming.files@data-axle.com`, which wins from anywhere on the page. `_ship_block` is capped at 260 chars for the Via/format tokens, but the address hunt runs from `Ship To:` to end of page and never above it (an `Email:` near the top is the broker's own rep). `send an email to X and Y` takes X. Pinned in `test_kap_fields.py`, `test_ship_to_rules.py`.

**KAP "BELOW" references**: a select like `24 MO $10-99.99 IN SCF'S BELOW` points at codes printed elsewhere. `_resolve_below_reference` matches on the subject named before `BELOW` and appends every other page line starting with that subject and holding a digit (keeping unrelated prose, ship-label line and URLs out). Those codes are an include, so they **stay in the Description**; `_detect_state_omits` counts off the omission field and cannot fire State Omits on them. A fallback also catches "use the link below to upload the file…" with the URL on the next line. Pinned in `test_kap_fields.py`.

## Ship-To House Rules (`tools_jira.py`)

Run at the top of `create_jira_ticket` and **override** the parser, in this order (put destination rules here, never in a parser — this covers all twelve brokers):

1. **Saturn** — bare substring `saturn` (any case) in `ship_to_email` OR `order_text` → File Format ASCII Fixed, Method FTP, ship-to rewritten to `FTP NOTIFY: … (SATURN CORP)`. Reads the whole order: unless the ship-to already names Saturn or is in-house, the first `@saturncorp.com` address anywhere replaces it; a still-blank ship-to becomes `PLACE ON SATURN CORP FTP FILESHARE`.
2. **Data Axle drop-box** — ship-to containing `incoming.files@data-axle.com` (`_DATA_AXLE_DROPBOXES`) → ASCII Fixed + FTP + `FTP NOTIFY:` prefix. The mailbox, never the domain (staff addresses are left alone).
3. `_FIXED_FORMAT_EMAILS` (data@trylondm.com, data@talonmm.com, data@rkdgroup.com, tisdata@trinitydirect.net, tapelibrarian@directmail.com) → File Format ASCII Fixed only; Shipping Method untouched (a parsed FTP stays FTP, though `_SYSTEM_ORDER` says "delivery stays Email" and would call that WRONG). The destination address decides format, not the delivery method.
4. **IN-HOUSE — runs LAST, overrides 1 and 2.** Any ship-to at `data-management.com` is our own address: destination forced to `tlibrarian@data-management.com` and Shipping Method to **Email**, **even when the order says FTP**; any DMI mailbox (`smondal@`) normalises to `tlibrarian@`. `file_format` is left alone (defaults to ASCII Delimited).

Data Axle as *broker* is irrelevant to delivery — only the Ship To block decides format and email/FTP/in-house.

## Mailer PO and Manager Order # by Broker

| Broker | Mailer PO source | Manager Order # source |
|--------|-----------------|----------------------|
| ADSTRA | `Broker PO:` — 6-digit, BRK-prefixed, or slashed (`B/19217`); **value can sit on the line below the label** | J-prefix or I-prefix |
| RMI | Broker PO# field | MGT# — **number only, prefix stripped**: `MGT26-01658` → `26-01658` |
| WE ARE MOORE | Ship Label number | Order# |
| Data Axle | Ship Label via `_ship_label_po` (rules below); a `-RN` suffix is cut (`58364-RN` → `58364`) | Order# |
| WASHINGTON LISTS | Client Reference: first all-digit 4+ value within 7 lines of the label (a suffixed value is skipped) | Order Number |
| KAP | Broker order # value | the `KAP ORDER:` value, first `D[LM]\d+` on the page |
| CONRAD DIRECT | BROK/MAIL PO: field | PURCHASE ORDER NO |
| Names in News | 6-7 digit number | LR # |
| CELCO | ORDER # as printed (`D04-086888-NI`) | the ORDER #'s digit group only (`086888`), so Title and Seed Tracking carry it; an ORDER # with no hyphen-delimited 5+ digit group is kept as printed |
| SimioCloud | Ship Label via `_ship_label_po` | Order# (inherits `DataAxleParser.parse`) |
| RKD / AMLC | `Client P.O.:` — in AMLC's columnar layout the value can sit up to 25 lines *below* its label | first 5-6 digit number in the first 10 lines (Service Bureau No. / Purchase Order No.) |

- **RMI**: the capture sits *inside* the prefix (`MGT(\d{2}-\d+)`) but line-matching still looks for the whole `MGT26-…` token to locate the line. One bad capture reaches Title, Manager Order # and Seed Tracking (forced equal to it). `test_rmi_fields.py`.
- **A truncated Mailer PO poisons the duplicate check** (every order truncating to the same stub matches every other). In `parsers/adstra.py`: `/` is part of the value, the gap to the value is at most one newline (`_find` passes `re.DOTALL`, so a bare `\s*` runs down the page), and an all-letter match is rejected (every real PO has a digit). DSLF-722/723/724 still carry the bare `B`. `test_adstra_list_code.py`.

### Ship Label PO (`parsers/data_axle._ship_label_po`)

Used by `DataAxleParser` and `SimioCloudParser` (SimioCloud tickets carry List Manager WE ARE MOORE; the `we_are_moore` parser has its own `W\d+` regex and none of these rules). The label is slash-separated with several of the mailer's own reference numbers; priority order (per Lee Ann's team):

1. An explicit `PO#` / `PO:` marker with a value, **letter prefix included**.
2. An **`E`-prefixed number is the PO even when nothing says "PO"** (`MOWP E20467/QTY/WWP/JOB 54634` → `E20467`).
3. **Three letters + two digits** is a PO (`CLU96`, `CLP78`).
4. Only then the old guess: the first 4+ digit run. Blank falls back to Manager Order #.

A `JOB` or `MERGE` number is the mailer's own and is never the PO (stripped before digits are searched). **Match whole tokens** — unanchored `[A-Z]{3}\d{2}` finds `AGA11` inside `TSAGA112991`. **A failed match becomes a plausible wrong answer, not a blank** (it falls through to the digit-run fallback and stores the digits without their prefix), so re-run the **whole** `test_data_axle_ship_label.py` after any change. **Shapes deliberately left alone** (no stated rule; guessing is worse): `DD4769`, `TSAGA112991`, `SGK108431`, `CB21PH01` — all keep the bare digit run. **Ask before extending the token forms.**

### Data Axle / SimioCloud omits (`_collect_omit_clauses`)

These orders state omits up to three times: a `*Omit Prior 66088` tail on `Base:`, `State=OMIT States: …` rows under `Selects:`, and a full restatement under `Special Instructions:`. Keeping only the first loses whatever it pointed at, and Other Fees goes blank (`_detect_state_omits` counts off the omission). `_collect_omit_clauses` takes every line's text from its first `OMIT` token on, joins a `*Omit Prior` wrapped onto a bare number line, drops a clause with an unclosed `(` (the wrapped half of a forward reference), and de-duplicates case-insensitively. Pinned in `test_data_axle_ship_label.py`.

## Requestor by Broker

Only `WeAreMooreParser` (always) and `KapParser` (fallback "Jenny Gomez") hard-code a requestor. The rest read it off the order (Data Axle / SimioCloud fall back to the ship-to address) or leave it blank; the ADSTRA/RMI/CONRAD rows are the defaults the `LLM_writes._SYSTEM` and `_SYSTEM_ORDER` prompts assume.

| Broker | Requestor | Email |
|--------|-----------|-------|
| ADSTRA | BOBBI DURRETT | BOBBI.DURRETT@ADSTRADATA.COM |
| RMI | ALICIA GALLAGHER | AGALLAGHER@RMIDIRECT.COM |
| WE ARE MOORE | MICHELLE NAY | MNAY@WEAREMOORE.COM |
| KAP | Jenny Gomez | jgomez@keyacquisition.com — house default only; the order's own `contact <Name> at\|@ Email: <addr>` line wins, pinned to `@keyacquisition.com` / `@keyacquisitionpartners.com` (a domain-agnostic pattern took the FTP-setup address `eftaccountsetup@igxfer.com`) |
| CONRAD DIRECT | Brenda Gundlah | bgundlah@conraddirect.com |
| CELCO | an `email` / `Email to` / `PLEASE EMAIL` address and the SHIP TO name first; else the CONTACT AT rep (Sara Ghods) | that address first; else sara@celcononprofit.com |

## DSLF Custom Field IDs

| Field | ID | Type | Notes |
|-------|-----|------|-------|
| Work Order | customfield_12089 | text | Set by IBM i step |
| Client Database | customfield_12155 | select | = full db_code; derived in create |
| Seed Database | customfield_12156 | select | = db_code[:-1] + "S"; derived in create |
| Billable Account | customfield_12191 | select | config `billing_cust` (or learned / AMLC `T11`), sent as stored |
| Manager Order Number | customfield_12192 | text | Used in title; AMLC dup key |
| Mailer PO | customfield_12193 | text | Duplicate-check field (non-AMLC) |
| Mailer Name | customfield_12194 | text | |
| Key Code | customfield_12195 | text | |
| Mail Date | customfield_12196 | date | YYYY-MM-DD |
| List Manager | customfield_12231 | text | Broker company |
| Requestor Name | customfield_12232 | text | |
| Requestor Email | customfield_12233 | text | |
| List Name | customfield_12234 | text | Abbreviation |
| Omission Description | customfield_12270 | ADF | |
| Requested Quantity | customfield_12271 | number | Integer |
| Seed Tracking Number | customfield_12272 | text | Forced = Manager Order # |
| Availability Rule | customfield_12273 | select | Nth=13235, All Available=13236 |
| File Format | customfield_12274 | select | ASCII Delimited=13237, ASCII Fixed=13238, Excel=13239, Other=13240 |
| Ship To Email | customfield_12275 | text | |
| Shipping Method | customfield_12276 | select | Email=13241, FTP=13242, Other=13243 |
| Shipping Instructions | customfield_12277 | text | Defaults to `CC: {requestor_email}` |
| Other Fees | customfield_12278 | text | |
| Special Seed Instructions | customfield_12311 | text | Only "Insert:" lines |
| Due Date | duedate | date | Ship By date |

Fill all fields on creation. Unknown select-option labels (availability/file format/shipping) are silently dropped with a log warning; Billable/Client/Seed DB options are resolved via a live createmeta lookup and dropped if unmatched.

## Billable Account / Client DB / Seed DB

From db_code (e.g. `F41D`): Client Database = full db_code; Seed Database = db_code with `S` suffix (`F41S`). **Billable Account is never derived from db_code** — it is the config row's `billing_cust` exactly as stored (usually the base `F41`, but some rows hold `M24R` or `T11 (A42D)`), a learned tier-5 value, or `T11` from `AmlcParser`. It legitimately differs from the Client-DB prefix by config design; treat `billing_cust` as authoritative. `A18` has a hard-coded option id; any other label must match a createmeta option (case-insensitive) or it is dropped.

## Key Code Patterns

| Broker | Source |
|--------|--------|
| Conrad Direct | Text after "And"/"&" on the MATERIAL line (not always present): `...PO# L50278HF & HF Thirteen Star Flag #2215A` → `HF Thirteen Star Flag #2215A` |
| Data Axle / SimioCloud | The order's `Key Code:` field, else a `Key <value>` segment in the Ship Label. **Never the Order # suffix** |
| Others | Extracted from the order if present |

**The Order # suffix is a rep name, not a key code** (`70853-MNay`, `2341064-Laura`). When a key code exists the order states it in `Key Code:`; blank is right otherwise (optional field). **Underscore ends a Ship Label field just as `/` does** (`FA_Wounded Warrior_69715_Key S67_Qty` → `S67`, not `S67_Qty`). Pinned in `test_data_axle_ship_label.py`.

## Supported Brokers (12) & Parser Internals

12 brokers in 10 files; two files host two parsers via inheritance:
- `data_axle.py`: `DataAxleParser` + `SimioCloudParser` — ⚠ SimioCloud's **List Manager is WE ARE MOORE** (its ordering platform), not DATA-AXLE.
- `amlc.py`: `RkdGroupParser` (→ RKD) + `AmlcParser` (→ AMLC; `billable_account=T11` for a rental or a list name containing VIGUERIE, then `finalize_and_create` forces db_code `T11R` with no lookup).

`PARSER_REGISTRY` keys: adstra, data_axle, simiocloud, rmi_direct, celco, rkd_group, amlc, kap, washington_lists, conrad_direct, names_in_news, we_are_moore.

- `_RULES` order is **load-bearing**: `amlc` precedes `rkd_group` (RKD-serviced AMLC orders contain "RKD GROUP"); `simiocloud`/`data_axle` share the "Exchange/Rental Order" anchor and are told apart only by the second pattern.
- Detection (`parsers/__init__.py`): pre-compiled regexes over `text[:3000]`, all must match, first full match wins; `detect_broker()` returns a frozen `BrokerMatch(broker_key, confidence, matched_patterns)` or `None`.
- **`CONFIDENCE_RULE_BASED = 0.92` is defined in BOTH `parsers/base.py` and `parsers/__init__.py`** (`detect_broker` uses the `__init__` copy). Edit both.
- `BaseBrokerParser` (`parsers/base.py`) helpers: `_find()`, `_find_date()`, `_normalize_date()`, `_find_quantity()`, `_map_shipping_method()`, `_detect_file_format()`, `_detect_state_omits()`, `_state_codes_from_omit()`, `_collect_continuation_block()`, `_is_saturn_order()`, `_extract_special_seed_instructions()`, `_find_email()`. Reuse them before adding new ones.

### Adding a new broker parser

1. Create `parsers/my_broker.py` inheriting `BaseBrokerParser`; implement `parse(text) -> ParseResult`.
2. Register in `PARSER_REGISTRY` in `parsers/__init__.py`.
3. Add the detection regex to `_RULES`, **positioned** so it neither shadows nor is shadowed by an existing broker (more specific/ambiguous formats first).

## Project Subagents (`.claude/agents/`)

`jira_Auto` (general DSLF pipeline work) and `bff_agent` (BrightFocus Foundation orders only: ADSTRA-brokered ADR/MDR/NGR programs) are both `model: opus` and **write-capable** — they can create and edit live Jira tickets through `tools_jira`. Prefer `bff_agent` for BFF orders. **Only `jira_Auto` is tracked in git**: `.claude/` is gitignored, so a fresh clone has no `bff_agent` and BFF work there falls back to `jira_Auto`.

## Github Rules

- When a change is made in any file, push it to GitHub with a proper message. "Commit to github" means commit **and push**. All pushes go to `main`.
- **"Always push" means the code you changed, not the whole working tree.** Stage named paths; never `git add -A` / `git add .`. Anything already modified when you started is someone else's change — surface it, don't sweep it in.
- **Never commit, even if it appears staged or someone asks:** `.env` / `*.env`, `email_scanner/thread_map.json`, `processed_ids.json`, `token_cache.bin` (runtime state — losing them spawns duplicate tickets), source Excel workbooks and `Client Profiles/` (client data), and `.claude/settings.local.json` (per-machine permissions).
- ⚠ **Two files are tracked despite the ignore rules** (committed before the rules existed): `NEW LR CLIENT LIST 2026.xlsx` and `.claude/settings.local.json`. The `.claude/` ignore entry cannot untrack the latter, so it shows as modified on every machine and any `git add` naming it sweeps it in. Always check `git status` before committing.
