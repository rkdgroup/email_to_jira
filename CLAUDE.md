# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DSLF List Rental Pipeline — processes purchase order PDFs from brokers, extracts structured fields via **rule-based** parsing (one parser per broker), enriches from generated YAML lookups, and creates DSLF tickets in Jira (rkdgroup.atlassian.net, project DSLF, issue type 11806).

**On the default path every structured field is rule-based.** `parse_pipeline.py` uses one parser per broker and calls Claude only for `tools_polish.py`, which structurally cleans the two prose fields (Description, Omission Description) after parsing — see "Prose Polish". A PDF matching none of the 12 broker fingerprints is **flagged for review and creates no ticket** — `process_pdf` has no Claude fallback.

## API cost is set to $100, use opus 5 model (medium effort) for all LLM related tasks and don't worry about the API cost. 

There are three other Claude touchpoints; know which is which before changing one:

| Where | Module | Role |
|-------|--------|------|
| Live pipeline, every ticket | `tools_polish.py` | structural prose clean, gated, falls back to parser text |
| Jenkins-scheduled QC | `qc_llm.py` | the only QC. Two checks/ticket: order-vs-ticket (fixable) + ticket-vs-SELECT |
| Manual create, opt-in | `LLM_writes.py` | Claude extracts **all** fields for an unrecognized broker |
| Offline, never scheduled | `ai_extract` / `compare_extraction` / `hybrid_create` | see "AI-Assisted Offline Tools" |

## Commands

```bash
# Core pipeline — single PDF (extract → parse → validate → dup check → create ticket)
python parse_pipeline.py /path/to/order.pdf

# Batch folder (globs *.pdf/*.PDF, non-recursive)
python parse_pipeline.py /path/to/folder/

# Dry-run: extract + parse + validate + enrich, NO ticket, NO dup check.
# Returns the full ticket kwargs under result["fields"]. --verbose prints parsed fields.
python parse_pipeline.py /path/to/order.pdf --dry-run --verbose
```

`--dry-run` and `--verbose` are the **only** two CLI flags for `parse_pipeline.py`. `broker_hint` is a function argument (used by the email scanner), not a flag.

**Testing**: there is no linter and no CI test stage — Jenkins never runs these, so they only
protect you if you run them. Eight regression files, each a standalone runner that prints
`PASS` lines and `ALL PASSED` (also collectible by pytest). All eight are hermetic: no
Jira, no DB, no PDFs, no network — the QC tests never call the API.

```bash
python test_ship_to_rules.py         # ship-to house rules + KAP FTP-boilerplate false positive
python test_kap_fields.py            # KAP exchange qty, spaced order #, $-prefixed select
python test_adstra_list_code.py      # ADSTRA 5-digit list code vs address digits
python test_qc_select_parse.py       # select_pdf SELECT-PDF parsing (spaced filenames)
python test_qc_llm_verdict.py        # qc_llm fail-closed, verdict gate, auto-fix whitelist
python test_dollar_cap_backfill.py   # Dollar Cap placement + no-duplicate re-run
python test_data_axle_ship_label.py  # Ship Label PO# prefix vs the digit-run fallback
python "WO#/test_work_order_allocation.py"   # WO collision loop, fake cursor
```

Run the matching file after touching `tools_jira.py` ship-to rules, `parsers/kap.py`,
`parsers/adstra.py`, `parsers/data_axle.py`, `qc_llm.py`, `select_pdf.py`,
`parse_pipeline._build_adf_description`, or `WO#/work_order.py`. Verified all eight pass
2026-08-27. Everything else is tested manually via `--dry-run --verbose` against real
broker PDFs.
The `broker_pdf/`, `Test_pdf/`, and `AMLC/` sample folders are **gitignored and not present
in a fresh clone** — ask for sample PDFs or point at a downloaded order instead of assuming
those paths exist.

**Windows console**: `--verbose` prints ligature-normalized PDF text that cp1252 cannot
encode (`UnicodeEncodeError`). Prefix runs with `PYTHONIOENCODING=utf-8`.

**Dev is Windows, Jenkins is POSIX.** Development happens on Windows/PowerShell, but the
`Jenkinsfile` uses `sh` steps and calls `python3` / `pip3`. Anything shell-shaped you add to
the build must be POSIX, and a path or command that only works in PowerShell will pass locally
and fail on the agent.

**`README.md` is a lighter, partially-stale duplicate of this file** — its Quick Start `pip`
line omits `anthropic`/`jaydebeapi`/`JPype1`/`xlrd`, and its flow diagram and project tree
predate `tools_polish.py`, `tools_zip_omit.py`, and the `WO#/` step. Treat CLAUDE.md as
authoritative and update README only when a change is user-facing.

```bash
# Scheduled automation (see "Scheduled Automation")
python email_scanner/email_scanner.py                 # one poll of the shared mailbox
python qc_llm.py [DSLF-123 ...] [--status S] [--post] [--fix] [--dry-run]
                 [--order-only|--select-only] [--model M] [--effort E] [--json f]
python qty_approval_scanner.py [--no-email-scan] [--combined] [--output f] [--email a] [--cc b] [--subject s]
python ticket_scanner/ticket_scanner.py [--loop N] [--reset] [--learn] [--reporter NAME]

# Config tooling (see "Config System")
python config_guard.py        # fast syntax gate over config/*.yaml (exit 1 on parse error)
python verify_configs.py      # deep audit of YAMLs vs source Excel/docs → config_audit_report.md
python build_profile_yaml.py  # regenerate config/client_profiles.yaml from Client Profiles/

# One-off correction — adds the client Dollar Cap line to existing ticket descriptions.
# DRY RUN by default; --live writes. The durable fix is in _build_adf_description.
python backfill_dollar_cap.py [DSLF-123 ...] [--status S] [--live]

# Pure-LLM create — unrecognized brokers only (see "Pure-LLM Ticket Creation")
# NOTE: inverted default — this one is a DRY RUN unless you pass --live.
python LLM_writes.py order.pdf [--live] [--model M] [--effort low|medium|high|xhigh|max] [--json f]

# Offline AI tools (see "AI-Assisted Offline Tools")
python compare_extraction.py DSLF-916 [--pdf f] [--md f] [--json f]   # read-only diff
python hybrid_create.py order.pdf [--dry-run] [--no-claude] [--no-attach]  # --dry-run first!
```

## Dependencies & Credentials

```bash
pip install anthropic requests pymupdf pdfminer.six pymupdf4llm python-dotenv msal pyyaml \
            openpyxl xlrd jaydebeapi JPype1 python-docx
```

- `requirements.txt` now covers **every** runtime import, including `python-docx` (added in `86d03d0`; needed by `client_profiles.py`, `build_profile_yaml.py`, `verify_configs.py`) and `openpyxl`/`xlrd` for the zip-omit splitter. Jenkins installs from this file *only* (`pip3 install -q -r requirements.txt`), so a new runtime import that isn't added here breaks the scheduled run, not the local one.
- `anthropic` is imported by the offline AI tools (`ai_extract.py`), by `tools_polish.py` in the live pipeline, and by `qc_llm.py` on the Jenkins cron — so `ANTHROPIC_API_KEY` is load-bearing for scheduled runs twice over. A missing key degrades prose quality and returns `UNVERIFIED` for every QC ticket; it does not break ticket creation.
- `jaydebeapi` + `JPype1` (+ `jt400.jar`) power the IBM i work-order step.

`.env` credentials by consumer:

| Vars | Used by |
|------|---------|
| `JIRA_BASE_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN` | Everything (Jira REST) |
| `MS_CLIENT_ID`, `MS_CLIENT_SECRET`, `MS_SERVICE_ACCOUNT`, `MS_SERVICE_PASSWORD`, `MS_TENANT_ID`, `IMAP_EMAIL` | email + qty scanners (MSAL ROPC auth) |
| `IBMI_HOST`, `IBMI_USER`, `IBMI_PASSWORD` | work-order creation |
| `ANTHROPIC_API_KEY` | `tools_polish` (live pipeline) + `qc_llm` (QC) + `LLM_writes` + offline AI tools |

The `JIRA_API_TOKEN` in `.env` **can create and edit tickets** — `tools_jira` uses it to create issues (POST), update fields (`update_ticket_fields`, PUT → 204), comment, and attach. (Verified 2026-07-27: created DSLF-919, updated DSLF-936.) The Atlassian MCP connector is an optional alternative for interactive edits under the user's own account, not a requirement.

## Architecture

`parse_pipeline.py` is split into a **head** and a **shared tail**, and the seam matters:

- `process_pdf()` — detect the broker and parse. Everything above the seam.
- `finalize_and_create(result, pdf_path, text, …)` — everything after a `ParseResult` exists.
  It never looks at the parser or the broker registry, so **any** producer of a `ParseResult`
  can use it. `LLM_writes.py` is the second producer. Put new post-parse behavior here, not
  in `process_pdf`, or the LLM path silently misses it.
- Its `profile_blocks_to_omission=True` flag (used only by `LLM_writes`) routes the profile's
  suppression blocks into the Omission field instead of the Description.

```
PDF → [tools_pdf] extract text (PyMuPDF primary; pdfminer fallback only if PyMuPDF <50 chars)
    ┄┄ process_pdf ┄┄
    → [parsers/__init__] detect_broker() — ALL of a broker's regexes must match within
      first 3000 chars; rules tried in _RULES order, first fully-matching broker wins.
      No match → flagged for review (no ticket).
    → [parsers/<broker>.py] rule-based parse → ParseResult (confidence 0.92)
    ┄┄ finalize_and_create — shared with LLM_writes ┄┄
    → [parse_result] validate_result()  ⚠ ADVISORY — see below
    → duplicate check (skipped in dry-run)
    → [client_lookup] enrich db_code/billable/list_manager from config/*.yaml
    → [client_profiles] resolve profile file (attached) + profile_data (select_by/flags/…)
    → [tools_polish] structural polish of segment_criteria + omission (LLM; falls back
      to the parser's text on any failure)
    → build kwargs + ADF description + FLAG omits
    → dry-run: return {"fields": kwargs}    |    live: create ticket + WO# + attach PDF/profile
```

Load-bearing behaviors that are easy to get wrong:

- **Validation is advisory.** `validate_result()` errors only abort when `result.confidence == 0.0`. Rule-based parsers always return 0.92, so missing required fields (mailer_name, mailer_po, list_name, list_manager, requested_quantity) just log "proceeding" and a **partial ticket is still created**. Only a totally-unparsed PDF is blocked.
- **SKIP_DB_CODES** (`parse_pipeline.py:78`, currently `{"A63D"}`): orders resolving to these db_codes are extracted/validated but create **no ticket** — returns `{"success": True, "skipped": True}`, in both live and dry-run. (Separate `_ADSTRA_SWEEPS_EXCLUDED = {"A63D","N11D"}` only controls whether the ADSTRA sweeps profile is attached.)
- **Multi-page PDFs**: every broker **except ADSTRA** splits into one ticket per page, and `process_pdf` then returns a **list** of per-page result dicts. ADSTRA multi-page is merged into one order. Callers must handle the list case.
- **Duplicate check** (live only): JQL on `cf[12193]` Mailer PO — **except AMLC**, which keys on `cf[12192]` Manager Order #. Skipped entirely in dry-run or when neither key is populated.
- **Description is NOT raw PDF text** — see Field Rules. The PDF is preserved by **attaching the file**.
- **`search_jira_tickets()` does not return a real total.** `/rest/api/3/search/jql` is token-paginated and omits `total`, so the helper reports `total = len(issues)` from a single page (`max_results=10` by default). Fine for the duplicate check (`total > 0`), wrong for counting. Use `search_issues_paged()` when you need every match.
- The work-order step and all attach steps **swallow exceptions** (log + continue): a ticket can succeed with its WO#, PDF, or profile attachment silently failed.
- **Zip-omit attachments are split** (`tools_zip_omit.py`): a supplementary `.xlsx/.xls/.csv/.txt` holding **more than `ZIP_CHUNK_SIZE` (9500)** zips also gets attached as zip-only `.xlsx` files of 9500 each, named `{original stem}_zips_{i}of{n}.xlsx` (45,000 zips → 5 files). The original attachment is still attached untouched, source order and duplicates are preserved, and a file at or under 9500 produces nothing extra. Runs in both attach paths — `parse_pipeline` Step 8 and the email scanner's `other_atts` loop.

## Scheduled Automation

Four independent entry points share the pipeline and `.env`. **Only `email_scanner` + `qc_llm` are Jenkins-scheduled** (Jenkinsfile, cron `H/5 * * * *`, **15-min** timeout, `QC_BUDGET_S=420`). `qty_approval_scanner` is run manually / emailed; `ticket_scanner` uses a Windows Task Scheduler `.bat`.

| Tool | Trigger / scope | Behavior |
|------|-----------------|----------|
| `email_scanner/email_scanner.py` | Shared-mailbox `List Rental` folder | MSAL ROPC auth → per message: if `conversationId` in `thread_map.json`, add a comment to the existing ticket; else download PDFs (or synthesize one from the body) → `process_pdf(broker_hint=SENDER_BROKER_MAP[domain])` → move mail to `List Rental/Processed` or `/Failed`. `broker_hint` short-circuits fingerprint detection. |
| `qc_llm.py` | `Needs QC` tickets (`--status` for any other queue) | Two LLM checks per ticket — was it **created** right from the broker order, and did the **SELECT** deliver it. Posts a comment on every ticket checked, pass included. **Never transitions.** `--fix` writes the order check's field corrections back (not enabled on the cron). Verdict is the worse of the two; `UNVERIFIED` means QC did not run and is **not** a pass. See "QC" below. |
| `qty_approval_scanner.py` | `Ready to Send for Qty Approval` tickets | Reads `QTY APPROVAL/<order#>` emails → sets Requested Quantity (`cf[12271]`); SELECT-PDF `TOTAL RECORDS SELECTED` fallback. **Never transitions.** Emails a per-mailer qty digest; single-card subjects prefix the list short code via `resolve_list_code` (from `dslf_list_and_mailer_names.txt`). |
| `ticket_scanner/ticket_scanner.py` | New DSLF tickets (issue# > saved state) | **Read-only** audit → report under `ticket_scanner/reports/`. `--learn` mines List Name→db_code patterns into `learned_patterns.json` (enrich tier 5). |

Notes: `email_scanner.main()` has **no argparse** — `run_email_scanner.bat --loop` is a silent no-op (single scan). SKIP_DB_CODES emails are deliberately **left in `List Rental`** for manual handling (not moved). `email_scanner.py` and `qc_llm.py` call `config_guard.validate_configs_or_exit()` before doing work.

### Email scanner specifics

- **Runtime state lives in two gitignored JSON files** next to the script:
  `processed_ids.json` (message IDs already handled) and `thread_map.json`
  (`conversationId` → ticket key). Deleting them is not harmless — the scanner will
  re-process the whole folder and follow-up emails will spawn **new tickets** instead of
  becoming comments on the existing one.
- **Scan window**: the top **50** messages in `List Rental`, ordered `receivedDateTime asc`
  (oldest first), minus anything in `processed_ids.json`. A backlog larger than 50 drains
  over successive runs.
- **Non-PDF attachments** are routed by a service-bureau range in the filename:
  `AMLC #668769-668774 ZipOmits.xls` attaches only to tickets whose
  `manager_order_number` falls in 668769–668774 (`_resolve_attachment_targets`). With no
  range in the name it attaches to **every** ticket created from that email.
- **No PDF attached** → the scanner synthesizes one from the plain-text email body
  (`_generate_pdf_from_text`, `Prefer: outlook.body-content-type="text"` so HTML `<style>`
  bloat cannot push the fingerprint past the 3000-char detection window).

### ⚠ Jenkins credential gap

`Jenkinsfile` injects only `MS_CLIENT_ID`, `MS_TENANT_ID`, and `IMAP_EMAIL`. But
`email_scanner.get_access_token()` also hard-requires **`MS_CLIENT_SECRET`,
`MS_SERVICE_ACCOUNT`, `MS_SERVICE_PASSWORD`** and calls `sys.exit(1)` if any is missing —
so scheduled runs authenticate only because a `.env` file exists in the Jenkins agent
workspace, not because Jenkins supplies those three. Do not assume the Jenkinsfile is the
complete credential picture. (`ANTHROPIC_API_KEY` **is** now used by scheduled runs — the
`tools_polish` step in the live pipeline — so that one is load-bearing rather than spare.)

## Config System

**`config/*.yaml` is a GENERATED CACHE, not hand-authored source.** Runtime (`client_lookup.py`) reads only the YAMLs — it never opens the Excel.

- **Sources**: `NEW LR CLIENT LIST 2026.xlsx` → the 15 Excel-lookup YAMLs (`full_client_list.yaml` + 14 per-broker); `Client Profiles/**/*.doc(x)` → `client_profiles.yaml` (via `build_profile_yaml.py`); `Client Profiles/ADSTRA/…xlsx` → `adstra_omit_database.yaml`.
- `config_guard.py` = fast fail-fast **syntax** gate (Jenkins entry points call it). `verify_configs.py` = offline **semantic** audit vs source. Passing config_guard does **not** mean content is correct.
- **Do not hand-edit these YAMLs blindly** — a structurally-valid but semantically-wrong edit (notably `client_profiles.yaml` 2-space list indent reparenting keys) passes config_guard and has broken `main` before. Regenerate or verify instead. `client_profiles.yaml` is a hand-curated superset; do not regenerate it to "fix" verify_configs field diffs.
- `enrich_fields()` lookup order (`client_lookup.py`): (0) ADSTRA 5-digit list-code exact, (1) exact db_code, (1.5) abbrev-token exact, (2) broker-YAML fuzzy, (3) cross-broker fuzzy, (4) full-list fuzzy, (5) learned_patterns. **Fuzzy threshold is 0.6** (0.5 for learned) — not the "50%" older docs claim. `broker_only=True` stops after tier 2.
- Orphan YAMLs not referenced by any code: `adrianne.yaml`, `jordan.yaml`, `leeann.yaml`.

## IBM i Work Orders

On every **live** create, `_create_and_link_work_order()` imports `WO#/work_order.py` (jt400 JDBC via `jaydebeapi`+`JPype1`) to INSERT into `DMIJOBS.ARWRKSCH`, then writes the WO# to `customfield_12089`. It re-reads the billable account from the just-created ticket. Requires `IBMI_*` env + `jt400.jar`. **Failures are non-fatal** (logged, ticket still succeeds); skipped entirely if billable_account is empty.

- **`WCCUST` is `letter_pos * 1000 + trailing`**, not a modulo (`_billable_to_wccust`:
  `K40` -> 11040, `T11` -> 20011). An earlier modulo version wrote wrong billing codes on
  live work orders; `WO#/fix_wccust.py` is the one-off repair script that corrected them and
  is a record of which WOs were affected, not something to re-run.
- **`(WWORKO, WSUFX)` is a DB-enforced composite key** and the pipeline always writes a blank
  suffix, so the collision guarded against is a human keying the same WWORKO with a
  *different* suffix. `allocate_and_create()` runs scan -> insert -> verify -> auto-reassign
  on one connection; it also reads the shop's `PEPBK#` counter as an allocation floor (read
  only — the ARWRKSCH trigger advances it) so it won't take a number order-entry reserved
  ahead of the committed MAX. `WO#/test_work_order_allocation.py` pins this loop.
- **`jt400.jar` is auto-discovered, not configured** (`WO#/base.py:_resolve_jt400`): `IBMI_JT400_JAR`
  first, then `/opt/jt400/jt400.jar`, the Jenkins workspace
  `/var/lib/jenkins/workspace/DSLF-Email-Scanner/jt400.jar`, the project root, and finally a
  hardcoded Windows RDi plugin path. Jenkinsfile sets `IBMI_JT400_JAR = "${WORKSPACE}/jt400.jar"`.
  Adding a machine means adding a path here or setting the env var — the jar is **not** in the repo.
- Only `IBMI_PASSWORD` is truly required: `IBMI_HOST` and `IBMI_USER` fall back to hardcoded
  defaults (`SYSTEM5.DATA-MANAGEMENT.COM`, `DMISUVAM`), so a missing host silently uses prod.
- There is a **second** `WO#/requirements.txt` (`jaydebeapi`/`JPype1`/`python-dotenv`) that
  Jenkins never installs — it runs `pip3 install -r requirements.txt` on the root file only.
  Keep the three pins in sync with root or the scheduled run misses them.

## Prose Polish (`tools_polish.py`) — the live LLM step

Runs on every ticket inside `process_pdf`, between kwargs assembly and the FLAG OMITS append.
Cleans the two PDF-derived prose values structurally, because parsers copy PDF text verbatim
and inherit its line wrapping (DSLF-967: one sentence wrapped across two lines, with an omit
criterion stranded in the Description and duplicated into Omission).

There are 2 parts in Jira - Description and ommision description. So the pull descriptions and all that are not to be ommited are to be places in description and everything that are to be ommited goes to ommision decription part. 

- **Five permitted operations only**: join a wrapped line, move/split an omit criterion into
  the omission field, drop a redundant line, drop an empty one, and label a run of bare
  select criteria (see below). **No rewording, ever.**
- **The `Selects:` heading is the one exception to "add nothing".** Broker PDFs list the
  priced selects under a `Selects:` header that the parsers drop, keeping only the values —
  DSLF-967's Description read `$10+` / `12 MOS HOTLINE` / `GENDER` as three bare lines with
  nothing saying what they were. The model may restore that heading over a run of **two or
  more** consecutive bare fragments and indent them by two spaces. `_ALLOWED_NEW_TOKENS =
  {"SELECTS"}` is the complete list of words it may introduce.
- **Only PDF-derived prose is sent.** `Select By`, `Standard Suppressions`, `Special
  Instructions`, and `FLAG OMITS:` are config-sourced and never leave the process — the
  existing code re-attaches them around the cleaned text.
- **The gate, not the model, is the guarantee** (`_validate`): the fact-token set across
  *both* fields must be identical before and after, apart from `_ALLOWED_NEW_TOKENS` —
  moving a criterion between fields is allowed, inventing or dropping one is not. Output
  lines may exceed input lines by at most one per line containing an omit keyword (a mixed
  line legitimately splits in two) plus one per heading actually emitted. Two rules the
  token set alone cannot enforce, because a repeated heading introduces no new token: a
  heading may appear **at most once**, and **never in the omission field**.
- **Every failure returns the parser's text unchanged** — no API key, budget exhausted,
  timeout, API error, refusal, or failed validation. A ticket is never worse than before, and
  an Anthropic outage cannot block creation.
- **Model is `claude-haiku-4-5`**, not Opus. Measured on DSLF-967, all three tiers scored 5/5
  after the prompt was tightened; Haiku did it at ~2.5s vs 6.0s (Opus 5) and 7.9s (Sonnet 5).
  The work is mechanical re-arrangement — the cheapest tier is also the fastest.
- **Jenkins guards**: 20s per-call timeout, `POLISH_BUDGET_S` (120s) per-process wall clock
  after which remaining tickets skip the pass, and an in-process cache so repeated text in a
  multi-page or batched order costs one call. A 7-page AMLC PDF is ~18s of polish.
- Inputs under two lines total skip the API entirely.

## Pure-LLM Ticket Creation (`LLM_writes.py`) — live creates, manual only

For orders **no parser recognizes**. `process_pdf` flags those for review and creates nothing,
and `hybrid_create.py` can't help either (it starts from `process_pdf` and raises when the
parse fails). This path only needs the PDF to be readable by Claude. **Prefer
`parse_pipeline.py` for any broker that IS recognized** — a parser reads a known layout
exactly, where this reads it fresh every run. Not scheduled; nothing calls it automatically.

- **The default is a dry run — `--live` creates.** This is the inverse of `parse_pipeline.py`
  and `hybrid_create.py`, both of which create unless told not to.
- **Claude supplies only what is printed on the order.** Everything else comes from the shared
  tail: db_code/Billable/Client DB/Seed DB from `client_lookup` (Claude's own db_code guess is
  *reported and never sent* — config is authoritative), the profile blocks, `tools_polish`, the
  duplicate check, SKIP_DB_CODES, the work order, and all four attachment steps.
- `CONFIDENCE_LLM_BASED = 0.85` — only its being non-zero matters, since `validate_result()`
  blocks only at exactly 0.0. The lower number records that nothing verified this read.
- **`_validate_and_fix()` blanks values Jira would reject or store wrong** *before* building the
  frozen `ParseResult`: a `list_manager` outside the 14 known values (the DSLF-130..134 C69
  bug), an unknown select-option label, a non-`YYYY-MM-DD` date (which would fail the whole
  create). `_VALID_LIST_MANAGERS` is read from `client_lookup._MANAGER_TO_FILE`, not re-typed.
- **`_verify_created()` re-reads the ticket after a live create**, because a create writes no
  changelog — unresolvable select options are dropped server-side without failing the create.
  Differences on `file_format`/`shipping_method`/`ship_to_email` are reported as `~` (expected:
  ship-to house rules rewrite them), everything else as `!`.
- Its `_SYSTEM` prompt layers broker rules on top of `ai_extract._SYSTEM` and is **transcribed
  from this file** (per-broker PO sources, the AMLC columnar trap, the SimioCloud→WE ARE MOORE
  list manager, KAP's "Email to:" line, the `Selects:` indent contract). Fix a field rule here
  *and* there, or the two paths disagree.
- Model pin is `claude-sonnet-5` @ `medium` effort (structured transcription against a fixed
  schema, not open-ended reasoning); `ai_extract`'s own Opus/high defaults are left alone for
  `compare_extraction` and `hybrid_create`. Override with `--model` / `--effort`.

## QC (`qc_llm.py`) — one file, all LLM, both questions

**There is exactly one QC checker and it makes API calls.** The 14 rule-based checks in
`qc_checker.run_qc_checks()` are gone; `qc_checker.py` is deleted. Its SELECT-PDF regexes
survive in `select_pdf.py`, which reads printed values and makes **no judgements** — do not
grow a comparison back into it.

`qc_llm.py` asks two questions about the same ticket, in one run, with one LLM call each:

| | ORDER check | SELECT check |
|---|---|---|
| Question | was the ticket **created** right from the broker order? | did the **pull** deliver the order? |
| Source of truth | the order PDF attached to the ticket | the SELECT report |
| Runs when | a non-SELECT PDF matches a broker fingerprint | a `*SELECT*.pdf` is attached |
| Findings carry a fix | **yes** — `fix_field` / `fix_value`, applied with `--fix` | **no**, by design |
| Prompt | `_SYSTEM_ORDER` (absorbed from `knowledge.md`) | `_SYSTEM_SELECT` |

The ticket verdict is the **worse** of the two, and `UNVERIFIED` outranks `FAIL` — not
knowing is worse than knowing it failed.

```bash
python qc_llm.py                              # scan Needs QC, print only
python qc_llm.py DSLF-1075 DSLF-1082          # named tickets
python qc_llm.py --status "Needs Assignment"  # the creation-check queue
python qc_llm.py --post                       # comment on every ticket checked
python qc_llm.py --post --fix                 # also write the order-check corrections
python qc_llm.py --order-only | --select-only | --dry-run
python qc_llm.py --model M --effort low|medium|high|xhigh|max --json FILE
```

- **Jenkins runs `python3 qc_llm.py --post`**, not `--fix`. Two LLM calls per ticket:
  measured **50.7s** for one ORDER check on DSLF-1132 (opus-5 @ high), so budget ~100s per
  ticket for both. The build timeout went **4 min → 15 min** and the Jenkinsfile sets
  `QC_BUDGET_S=420`, which is about **4 tickets per run** — enough for a normal cron tick,
  and anything past it comes back `UNVERIFIED` and is retried next run rather than being
  skipped. The cap also stops QC starving the email scanner queued behind it in the same
  build. `disableConcurrentBuilds()` means an overrunning build makes the next cron tick
  skip, not pile up.
- **First live run (2026-08-27, DSLF-1132) found a real parser defect**: Mailer PO stored
  as `23063` where the Ship Label reads `PO# E23063` — the leading letter was dropped. The
  ORDER check proposed `mailer_po: 23063 -> E23063` and `--fix --dry-run` validated it.
  That is a `SimioCloudParser`/`DataAxleParser` Ship-Label bug, not a QC bug — the parser
  fell through to the "first 4+ digit run in the label" branch instead of taking the `PO#`
  value whole.
- **A comment is posted on every ticket checked, pass included** — a clean ticket ends with
  "Checked and correct — no action needed." Silence used to mean "clean"; now it means
  "not checked".
- **Three verdicts and the third is the point.** `PASS`/`FAIL` are the model's;
  **`UNVERIFIED` is the code's** and is returned by every failure path — no API key,
  timeout, exhausted budget, API error, refusal, unreadable or oversize PDF, failed Jira
  read, and a prompt that could not be assembled. `UNVERIFIED` is **not a pass**: QC did
  not run. `test_qc_llm_verdict.py` pins every path.
- **An `UNVERIFIED` comment does not count as "already checked".** `_last_qc_comment_time`
  returns `None` when the last QC comment reads `VERDICT: UNVERIFIED`, so a ticket the
  budget cut off comes back next run. Without that, the re-run guard would see an unchanged
  ticket carrying a QC comment and skip it forever. The guard greps the report text, so
  `format_report` and `_last_qc_comment_time` are coupled — a test pins them together.
- **The gate overrides the model, not the reverse.** `_reconcile()` forces `FAIL` whenever
  any finding is `WRONG` or `BLOCKING-BLANK`, whatever the model wrote in `verdict`, and
  records `verdict_forced`. `NOTE` never forces a fail. Same philosophy as
  `tools_polish._validate`.
- **`_profile_context()` sends the client's `dollar_cap` and it is load-bearing.** `$10+` on
  an order is **not** an open-ended floor — it means $10 through *that client's* contracted
  cap (60 clients at `$99.99`, 48 at `$49.99`, a tail at `$249.99`/`$499.99`/`$999.99`, some
  `NO CAP`, 37 `VARIES PER ORDER`). A SELECT reading `10.00 THRU 99.99` against a `$10+`
  order is **correct** for a `$99.99` client. Without the cap every correctly-executed pull
  reads as lost records — that is exactly what the first live run did, failing
  DSLF-1077/1075/1073/1082 on it. The band test runs in the direction that loses records: a
  ceiling **below** the cap is `WRONG`, at it is fine, and no profile on file means
  unverifiable rather than wrong. The cap is now also written onto the ticket itself — see
  Field Rules.
- **`claude-opus-5` @ high effort.** A wrong database sends the wrong donor file to the
  wrong company, so accuracy beats speed and cost. `QC_BUDGET_S` defaults to 900s locally.
- Both prompts carry a **do-not-report list** for the known-correct-by-design cases
  (billable-vs-Client-DB prefix mismatch, house-rule ASCII Fixed/FTP, auto STATE OMITS,
  blank Mail Date/File Format/Other Fees/Key Code, qty mismatch under All Available,
  profile-sourced suppressions absent from the SELECT, Seed Tracking == Manager Order #,
  Seed DB = Client DB + S, and the hosted-list case below). Add new known-good patterns
  there or QC fills with noise.

### What the deleted rule checker knew, and where it went

Its verdict was `pass_count >= 4 and not hard_fails`. **Failures were never counted and
never subtracted**, so a ticket could carry any number of non-hard FAILs and still pass on
four passes — reproduced at **4 passes / 5 fails → `QC PASSED`**. The denominator moved too:
WARN rows were dropped entirely, so `total_checks` ranged 9–15 and a fixed absolute
threshold meant different things on different tickets. That is why it is gone rather than
patched.

Everything below it knew is now prompt text in `_SYSTEM_SELECT`, and
`test_qc_llm_verdict.py` asserts each one is still present:

- a completed SELECT can never legitimately return **0 records**
- **Nth** means the count must not exceed the requested quantity; **All Available** skips
  the quantity comparison entirely
- the **include set** is judged in one direction only — a standing universe is routinely
  *wider* than any one order, and only *narrower* (higher floor, shorter window) is wrong,
  because those donors were never in the pool
- **flags/states/zips are asymmetric**: an omit the ticket requires and the SELECT skipped
  is `WRONG`; an extra omit in the SELECT is a `NOTE`
- flags deferred to prose (`FLAG OMITS: FLAGS LISTED BELOW IN SPECIAL INST.`) are
  `BLOCKING-BLANK`, not a pass — they cannot be verified at all
- ADSTRA's published per-seed-database flag defaults still load from
  `config/adstra_omit_database.yaml` (`_adstra_flag_context`) as a third source

**The live incident it left open is now handled as design.** Measured over the 30
most-recent tickets with a SELECT PDF, the only surviving non-hard failures were two
`List Name` rows — DSLF-1066 (SELECT customer `AREIVIM`, ticket list `3-HOC HEAL OUR
CHILDREN`, db `A12D`) and DSLF-1083 (SELECT customer `NEWPORT CREATIVE SWEEPS MASTER`,
ticket list `3-SDCA CHARITABLE APPEALS MF`, db `N15R`). Both are **hosted lists**: the
SELECT prints the host/master account while the ticket names the rented list, and the two
legitimately differ. `_SYSTEM_SELECT` states this — the database **code** must still match,
and a name-only difference is a `NOTE`. DSLF-1083 also carried a real `File Format` defect
(ASCII Delimited where the destination forces ASCII Fixed) which the check still reports.

### Auto-fix (`--fix`) — order-check findings only

`apply_fixes()` collects every finding whose `fix_field` is in the `_FIXABLE` whitelist and
writes them in **one PUT**. What it refuses, and why the refusals are the design:

- **`client_db` / `seed_db` / `billable_account` are never writable.** They come from
  `client_lookup`, not off the order; they are select fields resolved against a live
  createmeta lookup; and a wrong write here is the worst outcome in this system. Reported,
  never written.
- **`description` / `omission` are never writable** — ADF prose owned by the parsers and
  `tools_polish`, and a field-level overwrite flattens the bullet structure
  `_build_adf_description` builds.
- A **select option** Jira cannot resolve is dropped server-side *without failing the
  request*, so `_validate_fix` checks every option against `AVAILABILITY_RULE_OPTIONS` /
  `FILE_FORMAT_OPTIONS` / `SHIPPING_METHOD_OPTIONS` and sends `{"id": ...}`.
- `list_manager` must be one of the 14 (read from `client_lookup._MANAGER_TO_FILE`), dates
  must be `YYYY-MM-DD`, quantities must be plausible positive integers, and `seed_tracking`
  must equal the Manager Order # or it is refused.
- **`NOTE`-severity findings are never applied**, an empty `fix_value` is refused (blanking
  a field is not a fix), and a second fix for the same field is ignored.
- Every refusal is printed in the QC comment under `NOT APPLIED` with its reason, so a
  skipped fix is visible rather than silent.

**`knowledge.md` has been absorbed, not referenced.** It specced a separate hosted agent for
whether a ticket was *created* correctly against the **order** PDF, reporting by email. That
job is now the ORDER check: its field map, severities, broker PO table, requestor defaults,
known-missing-Jira-options list and both wrong-client incidents live in `_SYSTEM_ORDER`. Its
line-161 claim that KAP titles are `P.O. {DL#} {LIST NAME}` "by design" was **not** carried
over — that was a bug fixed in `39d94bc` which 64 tickets carried, and
`test_qc_llm_verdict.py` asserts the exemption never comes back.

## AI-Assisted Offline Tools

Auxiliary, **not part of the live pipeline**. All require `ANTHROPIC_API_KEY` and are pinned to `claude-opus-4-8` (`ai_extract.py:34`, `compare_extraction.py:275`, `hybrid_create.py:39/56/106`). That pin predates the Claude 5 family and has not been re-evaluated — it is inertia, not a measured choice, unlike the live-pipeline `claude-haiku-4-5` pin which was benchmarked. `compare_extraction` and `hybrid_create` both take `--model`, so a newer model can be tried without editing anything.

| Tool | Purpose |
|------|---------|
| `ai_extract.py` | `extract_fields_from_pdf()` — base64 PDF → Claude structured output (`DSLF_SCHEMA`); prose fields returned as per-line arrays. Module import only (no CLI). Rejects PDFs > 32 MB. |
| `compare_extraction.py` | **Read-only**: pull a ticket's current fields + its PDF, run Claude, print a field-by-field diff (terminal/`--md`/`--json`). Never writes to Jira. |
| `hybrid_create.py` | `process_pdf(dry_run=True)` for complete rule-based kwargs, then merges Claude's Description prose on top → **live create**. Omission stays 100% rule-based. |

⚠ **`hybrid_create.py` (non-dry-run) BYPASSES the duplicate check** and writes a live production ticket — always run `--dry-run` first. Claude-only extraction misses db_code enrichment, profile-injected STANDARD/FLAG omits, and the correct broker requestor — which is why the hybrid keeps rule-based for all structured fields and uses Claude only for Description prose.

## Field Rules

- **Title**: `{LIST NAME} - {MAILER NAME} - {MANAGER ORDER NUMBER}` (never Mailer PO). e.g. `JUDICIAL WATCH DONORS - HERITAGE FOUNDATION - W74926JW`
- **Description**: an **ADF document** of `segment_criteria` (selection/select portion of the PDF) plus the client profile's `Select By`, `Dollar Cap`, `Standard Suppressions`, and `Special Instructions`. It is **not** the raw PDF text — the raw order text is passed separately as `create_jira_ticket(order_text=…)` and used only for the Saturn ship-to rule; the PDF itself is attached.
  - **`Dollar Cap:` is written on every ticket that has one on file**, immediately after `Select By:`, verbatim from the profile's `dollar_cap` (`$99.99`, `NO CAP`, `VARIES PER ORDER`, … — never normalised, each means something different). Without it neither a human nor `qc_llm` can tell a correct capped pull (`10.00 THRU 99.99` against a `$10+` order) from one that quietly lost every donor above the cap. Tickets created before this ran are corrected by `backfill_dollar_cap.py`; a client with no cap recorded gets no line.
  - **Indentation in `segment_criteria` is structural, not cosmetic.** In `_build_adf_description` a run of indented lines becomes an ADF `bulletList` under the paragraph above it — the same shape the profile blocks use. This is the contract `tools_polish` writes to when it labels a `Selects:` group. Jira's renderer collapses leading whitespace, so an indent that stays a plain string is invisible in the UI; it has to become real ADF structure.
- **Omission Description** (`cf[12270]`, ADF): what is omitted/suppressed — flags, states, zips/SCFs, "OMIT PREVIOUS ORDER", "1 PER HOUSEHOLD", plus profile `FLAG OMITS:`. Accepts a pre-built ADF dict **or** a plain string; a plain string is split into **one paragraph per line** so criteria don't render as a run-on blob.
- **List Manager** = one of these exact values: ADSTRA, AALC, AMLC, CELCO, CONRAD, DATA-AXLE, KAP, MARY E GRANGER, NEGEV, NAMES IN THE NEWS, RKD, RMI, WASHINGTON LISTS, WE ARE MOORE
- **Mailer Name** = organization sending the mail. **List Name** = donor list being rented. Never swap. On the order forms themselves the "Mailer" and "Broker" labels are used interchangeably (per Lee Ann Hazelwood) — read the value, not the label.
- **List Name** is stored as the abbreviation, e.g. FAIR = Federation for American Immigration Reform.
- **Availability Rule**: "Full Run" = "All Available", "NTH NAME" = "Nth"
- **Other Fees**: "STATE OMITS" when the omission has 6+ states/zips/SCFs (state count + 3-5-digit-number count summed ≥ 6 — automatic, expected)
- **File Format**: `create_jira_ticket` defaults to **ASCII Delimited** when unspecified (after the ASCII-Fixed forcing rules), so new tickets are never blank on this field.
- **Special Seed Instructions**: only "Insert:" lines. Never FTP/email info. Blank for most orders.
- **Status on creation**: Always "Needs Assignment". Never transition on creation.

## Ship-To House Rules (tools_jira.py)

Run at the top of `create_jira_ticket` and **override** whatever the parser produced:

1. **Saturn** — `"saturn"` in `ship_to_email` OR `order_text` → force File Format = ASCII Fixed, Method = FTP, rewrite ship-to to `FTP NOTIFY: … (SATURN CORP)`. (`order_text` is passed in specifically so a body-only Saturn mention still fires.)
2. **data-axle.com** ship-to → ASCII Fixed + FTP + `FTP NOTIFY:` prefix (never emailed).
3. `_FIXED_FORMAT_EMAILS` (data@trylondm.com, data@talonmm.com, data@rkdgroup.com, tisdata@trinitydirect.net, tapelibrarian@directmail.com) → ASCII Fixed but delivery stays **Email**.

## Mailer PO and Manager Order # by Broker

| Broker | Mailer PO source | Manager Order # source |
|--------|-----------------|----------------------|
| ADSTRA | 6-digit or BRK-prefixed | J-prefix or I-prefix |
| RMI | Broker PO# field | MGT# |
| WE ARE MOORE | Ship Label number | Order# |
| Data Axle | Ship Label PO: with suffix (58364-RN) | Order# (2316747) |
| WASHINGTON LISTS | Client Reference with suffix | Order Number |
| KAP | Broker order # value | KAP ORDER DL-prefix |
| CONRAD DIRECT | BROK/MAIL PO: field | PURCHASE ORDER NO |
| Names in News | 6-7 digit number | LR # |
| CELCO | ORDER # | ORDER # |
| SimioCloud | Ship Label `PO#` **including a letter prefix** (`PO# E23063` -> `E23063`), else first 4+ digit run in the label, else falls back to Order# | Order# (inherits `DataAxleParser.parse`) |
| RKD / AMLC | `Client P.O.:` — in AMLC's columnar layout the value can sit up to 25 lines *below* its label | first 5-6 digit number in the first 10 lines (Service Bureau No. / Purchase Order No.) |

**A failed match in the Ship Label becomes a plausible wrong answer, not a blank.**
Until 2026-08-27 the `PO#` capture was digits-only, so a letter-prefixed value did not
match at all and control fell through to the "first 4+ digit run anywhere in the label"
fallback — which then found the digits of the value the first branch had just rejected and
stored them without the prefix. DSLF-1132's label reads `WWP f/PBC/PO# E23063/Job #54793`
and the ticket held `23063`. `qc_llm`'s ORDER check found it on its first live run; the
parser is fixed and DSLF-1132 was corrected to `E23063`. **Other SimioCloud/Data Axle
tickets created before the fix may carry the same truncation** — it can only be confirmed
against each order PDF, and `python qc_llm.py --status "<queue>" --order-only` is the way
to sweep for it. `test_data_axle_ship_label.py` pins both branches.

## Requestor by Broker

| Broker | Requestor | Email |
|--------|-----------|-------|
| ADSTRA | BOBBI DURRETT | BOBBI.DURRETT@ADSTRADATA.COM |
| RMI | ALICIA GALLAGHER | AGALLAGHER@RMIDIRECT.COM |
| WE ARE MOORE | MICHELLE NAY | MNAY@WEAREMOORE.COM |
| KAP | JENNY GOMEZ | jgomez@keyacquisition.com |
| CONRAD DIRECT | Brenda Gundlah | bgundlah@conraddirect.com |

## DSLF Custom Field IDs

| Field | ID | Type | Notes |
|-------|-----|------|-------|
| Work Order | customfield_12089 | text | Set by IBM i step |
| Client Database | customfield_12155 | select | = full db_code; derived in create |
| Seed Database | customfield_12156 | select | = db_code[:-1] + "S"; derived in create |
| Billable Account | customfield_12191 | select | Passed in (suffix stripped) |
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

From db_code (e.g., F41D): Billable Account = db_code without suffix (F41); Client Database = full db_code (F41D); Seed Database = db_code with S suffix (F41S). Note: Billable Account (`billing_cust`) can legitimately differ from the Client-DB prefix by config design — treat billing_cust as authoritative.

## Key Code Patterns

| Broker | Source |
|--------|--------|
| Conrad Direct | Text after "And"/"&" on MATERIAL line (not always present). e.g. "...PO# L50278HF & HF Thirteen Star Flag #2215A" → Key Code = "HF Thirteen Star Flag #2215A" |
| Data Axle | "Key Code:" field or Order# suffix |
| Others | Extracted from order if present |

## Supported Brokers (12) & Detection

12 brokers in 10 files; two files host two parsers each via inheritance:
- `data_axle.py`: `DataAxleParser` + `SimioCloudParser` — ⚠ SimioCloud's **List Manager is WE ARE MOORE** (SimioCloud is We Are Moore's ordering platform), not DATA-AXLE.
- `amlc.py`: `RkdGroupParser` (→ List Manager RKD) + `AmlcParser` (→ AMLC; sets billable_account=T11 only for rentals).

`PARSER_REGISTRY` keys: adstra, data_axle, simiocloud, rmi_direct, celco, rkd_group, amlc, kap, washington_lists, conrad_direct, names_in_news, we_are_moore.

`_RULES` order is **load-bearing**: `amlc` precedes `rkd_group` (RKD-serviced AMLC orders contain "RKD GROUP"); `simiocloud`/`data_axle` share the "Exchange/Rental Order" anchor and are disambiguated only by the second pattern.

## Parser Internals

- **BaseBrokerParser** (`parsers/base.py`): shared helpers include `_find()`, `_find_date()`, `_normalize_date()`, `_find_quantity()`, `_map_shipping_method()`, `_detect_file_format()`, `_detect_state_omits()`, `_state_codes_from_omit()`, `_collect_continuation_block()`, `_is_saturn_order()`, `_extract_special_seed_instructions()`, `_find_email()`.
- **CONFIDENCE_RULE_BASED = 0.92** is defined in **both** `base.py:7` and `parsers/__init__.py:28` (detect_broker uses the `__init__` copy). Keep them in sync — editing one alone does not change the other.
- **Broker detection** (`parsers/__init__.py`): pre-compiled regex in `_RULES`, scanned over `text[:3000]`; all patterns must match; first fully-matching broker wins; `detect_broker()` returns a frozen `BrokerMatch(broker_key, confidence, matched_patterns)` or `None`.
- **Client lookup** (`client_lookup.py`): reads generated `config/*.yaml` (see Config System) — **not** the Excel at runtime.

## Adding a New Broker Parser

1. Create `parsers/my_broker.py` inheriting from `BaseBrokerParser`; implement `parse(text) -> ParseResult`.
2. Register in `PARSER_REGISTRY` in `parsers/__init__.py`.
3. Add detection regex to `_RULES` in `parsers/__init__.py` — **positioned** so its patterns neither shadow nor are shadowed by an existing broker (more-specific/ambiguous formats first).

## Project Subagents (`.claude/agents/`)

Two repo-local agents, both `model: opus` and both **write-capable**
(`Read, Write, Edit, Bash, Glob, Grep, Agent, WebSearch, WebFetch`):

| Agent | Scope |
|-------|-------|
| `jira_Auto` | General DSLF pipeline work — parse broker PDFs, fix parsers, manage tickets. |
| `bff_agent` | BrightFocus Foundation orders only (ADSTRA-brokered, ADR/MDR/NGR programs) and their BFF-specific parsing quirks. |

They can create and edit live Jira tickets through `tools_jira`, same as the pipeline. Prefer
`bff_agent` when the order is BFF — it carries the program-level knowledge `jira_Auto` does not.

## Github Rules

- When a change is made in any file always push it to github with proper message.
- When i say commit to github - it means you need to commit and push the changes.
- Make sure all push happens in main branch.
- **"Always push" means the code you changed, not everything in the working tree.** Stage
  named paths; never `git add -A` / `git add .`. Anything already modified when you started
  is someone else's change — surface it, don't sweep it into your commit.
- **Never commit, even if it appears staged or someone asks:** `.env` / `*.env` (Jira token,
  MS service password, IBM i credentials), `email_scanner/thread_map.json` and
  `processed_ids.json` and `token_cache.bin` (runtime state — see "Email scanner specifics"
  for why losing them spawns duplicate tickets), the source Excel workbooks and
  `Client Profiles/` (client data), and `.claude/settings.local.json` (per-machine
  permissions). `.gitignore` covers these today; that is a safety net, not a reason to skip
  checking `git status` before committing.
