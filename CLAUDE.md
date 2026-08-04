# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DSLF List Rental Pipeline — processes purchase order PDFs from brokers, extracts structured fields via **rule-based** parsing (one parser per broker), enriches from generated YAML lookups, and creates DSLF tickets in Jira (rkdgroup.atlassian.net, project DSLF, issue type 11806).

**Every structured field is rule-based.** The one LLM step in the live pipeline is `tools_polish.py`, which structurally cleans the two prose fields (Description, Omission Description) after parsing — see "Prose Polish". A PDF matching none of the 12 broker fingerprints is still flagged for review (no ticket, no Claude fallback parser). Claude is otherwise used only by the offline auxiliary tools (see "AI-Assisted Offline Tools").

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

`--dry-run` and `--verbose` are the **only** two CLI flags. `broker_hint` is a function argument (used by the email scanner), not a flag.

**Testing**: there is no linter and no CI test stage. The single automated test is
`WO#/test_work_order_allocation.py` (7 branch-coverage tests for the WO collision loop,
fake cursor, **no DB access**) — run it after touching `WO#/work_order.py`:

```bash
python "WO#/test_work_order_allocation.py"      # standalone runner, prints PASS/ALL PASSED
pytest "WO#/test_work_order_allocation.py"      # also works under pytest
```

Everything else is tested manually via `--dry-run --verbose` against real broker PDFs.
The `broker_pdf/`, `Test_pdf/`, and `AMLC/` sample folders are **gitignored and not present
in a fresh clone** — ask for sample PDFs or point at a downloaded order instead of assuming
those paths exist.

**Windows console**: `--verbose` prints ligature-normalized PDF text that cp1252 cannot
encode (`UnicodeEncodeError`). Prefix runs with `PYTHONIOENCODING=utf-8`.

```bash
# Scheduled automation (see "Scheduled Automation")
python email_scanner/email_scanner.py                 # one poll of the shared mailbox
python qc_checker.py [DSLF-123] [--dry-run] [--watch [MIN]]
python qty_approval_scanner.py [--no-email-scan] [--combined] [--output f] [--email a] [--cc b] [--subject s]
python ticket_scanner/ticket_scanner.py [--loop N] [--reset] [--learn] [--reporter NAME]

# Config tooling (see "Config System")
python config_guard.py        # fast syntax gate over config/*.yaml (exit 1 on parse error)
python verify_configs.py      # deep audit of YAMLs vs source Excel/docs → config_audit_report.md
python build_profile_yaml.py  # regenerate config/client_profiles.yaml from Client Profiles/

# Offline AI tools (see "AI-Assisted Offline Tools")
python compare_extraction.py DSLF-916 [--pdf f] [--md f] [--json f]   # read-only diff
python hybrid_create.py order.pdf [--dry-run] [--no-claude]           # --dry-run first!
```

## Dependencies & Credentials

```bash
pip install anthropic requests pymupdf pdfminer.six pymupdf4llm python-dotenv msal pyyaml \
            openpyxl xlrd jaydebeapi JPype1 python-docx
```

- `requirements.txt` is the base list but is still **missing `python-docx`** (needed by `client_profiles.py`, `build_profile_yaml.py`, `verify_configs.py`). `openpyxl` and `xlrd` **are** in it — the zip-omit splitter runs on the Jenkins email path, which installs from that file only.
- `anthropic` is imported by the offline AI tools (`ai_extract.py`) **and by `tools_polish.py`, which runs in the live pipeline** — so `ANTHROPIC_API_KEY` is now load-bearing for scheduled runs (a missing key degrades prose quality, it does not break ticket creation).
- `jaydebeapi` + `JPype1` (+ `jt400.jar`) power the IBM i work-order step.

`.env` credentials by consumer:

| Vars | Used by |
|------|---------|
| `JIRA_BASE_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN` | Everything (Jira REST) |
| `MS_CLIENT_ID`, `MS_CLIENT_SECRET`, `MS_SERVICE_ACCOUNT`, `MS_SERVICE_PASSWORD`, `MS_TENANT_ID`, `IMAP_EMAIL` | email + qty scanners (MSAL ROPC auth) |
| `IBMI_HOST`, `IBMI_USER`, `IBMI_PASSWORD` | work-order creation |
| `ANTHROPIC_API_KEY` | `tools_polish` (live pipeline) + offline AI tools |

The `JIRA_API_TOKEN` in `.env` **can create and edit tickets** — `tools_jira` uses it to create issues (POST), update fields (`update_ticket_fields`, PUT → 204), comment, and attach. (Verified 2026-07-27: created DSLF-919, updated DSLF-936.) The Atlassian MCP connector is an optional alternative for interactive edits under the user's own account, not a requirement.

## Architecture

`process_pdf()` in `parse_pipeline.py` is the single orchestrator:

```
PDF → [tools_pdf] extract text (PyMuPDF primary; pdfminer fallback only if PyMuPDF <50 chars)
    → [parsers/__init__] detect_broker() — ALL of a broker's regexes must match within
      first 3000 chars; rules tried in _RULES order, first fully-matching broker wins.
      No match → flagged for review (no ticket).
    → [parsers/<broker>.py] rule-based parse → ParseResult (confidence 0.92)
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

Four independent entry points share the pipeline and `.env`. **Only `email_scanner` + `qc_checker` are Jenkins-scheduled** (Jenkinsfile, cron `H/5 * * * *`, 4-min timeout). `qty_approval_scanner` is run manually / emailed; `ticket_scanner` uses a Windows Task Scheduler `.bat`.

| Tool | Trigger / scope | Behavior |
|------|-----------------|----------|
| `email_scanner/email_scanner.py` | Shared-mailbox `List Rental` folder | MSAL ROPC auth → per message: if `conversationId` in `thread_map.json`, add a comment to the existing ticket; else download PDFs (or synthesize one from the body) → `process_pdf(broker_hint=SENDER_BROKER_MAP[domain])` → move mail to `List Rental/Processed` or `/Failed`. `broker_hint` short-circuits fingerprint detection. |
| `qc_checker.py` | `Needs QC` tickets | Downloads the most-recent SELECT PDF, posts a PASS/FAIL comment. **Never transitions.** Pass = `PASS count ≥ 4` AND no hard-required fail (hard = Client Database + Manager Order #); WARN rows are dropped. |
| `qty_approval_scanner.py` | `Waiting on Qty Approval` tickets | Reads `QTY APPROVAL/<order#>` emails → sets Requested Quantity (`cf[12271]`); SELECT-PDF `TOTAL RECORDS SELECTED` fallback. **Never transitions.** Emails a per-mailer qty digest; single-card subjects prefix the list short code via `resolve_list_code` (from `dslf_list_and_mailer_names.txt`). |
| `ticket_scanner/ticket_scanner.py` | New DSLF tickets (issue# > saved state) | **Read-only** audit → report under `ticket_scanner/reports/`. `--learn` mines List Name→db_code patterns into `learned_patterns.json` (enrich tier 5). |

Notes: `email_scanner.main()` has **no argparse** — `run_email_scanner.bat --loop` is a silent no-op (single scan). SKIP_DB_CODES emails are deliberately **left in `List Rental`** for manual handling (not moved). `email_scanner.py` and `qc_checker.py` call `config_guard.validate_configs_or_exit()` before doing work.

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

## Prose Polish (`tools_polish.py`) — the live LLM step

Runs on every ticket inside `process_pdf`, between kwargs assembly and the FLAG OMITS append.
Cleans the two PDF-derived prose values structurally, because parsers copy PDF text verbatim
and inherit its line wrapping (DSLF-967: one sentence wrapped across two lines, with an omit
criterion stranded in the Description and duplicated into Omission).

- **Four permitted operations only**: join a wrapped line, move/split an omit criterion into
  the omission field, drop a redundant line, drop an empty one. **No rewording, ever.**
- **Only PDF-derived prose is sent.** `Select By`, `Standard Suppressions`, `Special
  Instructions`, and `FLAG OMITS:` are config-sourced and never leave the process — the
  existing code re-attaches them around the cleaned text.
- **The gate, not the model, is the guarantee** (`_validate`): the fact-token set across
  *both* fields must be identical before and after — moving a criterion between fields is
  allowed, inventing or dropping one is not. Output lines may exceed input lines by at most
  one per line containing an omit keyword (a mixed line legitimately splits in two).
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

## AI-Assisted Offline Tools

Auxiliary, **not part of the live pipeline**. All use Claude `claude-opus-4-8` and require `ANTHROPIC_API_KEY`.

| Tool | Purpose |
|------|---------|
| `ai_extract.py` | `extract_fields_from_pdf()` — base64 PDF → Claude structured output (`DSLF_SCHEMA`); prose fields returned as per-line arrays. Module import only (no CLI). Rejects PDFs > 32 MB. |
| `compare_extraction.py` | **Read-only**: pull a ticket's current fields + its PDF, run Claude, print a field-by-field diff (terminal/`--md`/`--json`). Never writes to Jira. |
| `hybrid_create.py` | `process_pdf(dry_run=True)` for complete rule-based kwargs, then merges Claude's Description prose on top → **live create**. Omission stays 100% rule-based. |

⚠ **`hybrid_create.py` (non-dry-run) BYPASSES the duplicate check** and writes a live production ticket — always run `--dry-run` first. Claude-only extraction misses db_code enrichment, profile-injected STANDARD/FLAG omits, and the correct broker requestor — which is why the hybrid keeps rule-based for all structured fields and uses Claude only for Description prose.

## Field Rules

- **Title**: `{LIST NAME} - {MAILER NAME} - {MANAGER ORDER NUMBER}` (never Mailer PO). e.g. `JUDICIAL WATCH DONORS - HERITAGE FOUNDATION - W74926JW`
- **Description**: an **ADF document** of `segment_criteria` (selection/select portion of the PDF) plus the client profile's `Select By`, `Standard Suppressions`, and `Special Instructions`. It is **not** the raw PDF text — the raw order text is passed separately as `create_jira_ticket(order_text=…)` and used only for the Saturn ship-to rule; the PDF itself is attached.
- **Omission Description** (`cf[12270]`, ADF): what is omitted/suppressed — flags, states, zips/SCFs, "OMIT PREVIOUS ORDER", "1 PER HOUSEHOLD", plus profile `FLAG OMITS:`. Accepts a pre-built ADF dict **or** a plain string; a plain string is split into **one paragraph per line** so criteria don't render as a run-on blob.
- **List Manager** = one of these exact values: ADSTRA, AALC, AMLC, CELCO, CONRAD, DATA-AXLE, KAP, MARY E GRANGER, NEGEV, NAMES IN THE NEWS, RKD, RMI, WASHINGTON LISTS, WE ARE MOORE
- **Mailer Name** = organization sending the mail. **List Name** = donor list being rented. Never swap.
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

## Additional Field Rules (from Lee Ann Hazelwood)

- **Mailer / Broker fields**: interchangeable on list-rental order forms.
- **Availability Rule**: "Full Run" = "All Available". Confirmed.
- **List Name abbreviations**: FAIR = Federation for American Immigration Reform.

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

## Github Rules

- When a change is made in any file always push it to github with proper message.
- When i say commit to github - it means you need to commit and push the changes.
- Make sure all push happens in main branch.
