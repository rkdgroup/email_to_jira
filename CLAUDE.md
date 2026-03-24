# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DSLF List Rental Pipeline — processes purchase order PDFs from brokers, extracts structured fields via hybrid rule-based + Claude AI parsing, and creates DSLF tickets in Jira (rkdgroup.atlassian.net, project DSLF, issue type 11806).

## Commands

```bash
# Single PDF (extract → validate → create Jira ticket)
python parse_pipeline.py /path/to/order.pdf

# Batch folder processing
python parse_pipeline.py /path/to/folder/

# Dry-run (extract + validate only, no ticket created)
python parse_pipeline.py /path/to/order.pdf --dry-run --verbose
```

No test framework, linter, or CI/CD configured. Testing is manual against PDFs in `broker_pdf/` and `Test_pdf/`.

## Dependencies

```bash
pip install anthropic requests pymupdf pdfminer.six pymupdf4llm openpyxl python-dotenv
```

Credentials in `.env`: `JIRA_BASE_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN`, `ANTHROPIC_API_KEY`.

## Architecture

```
PDF → [tools_pdf.py] extract text (PyMuPDF primary, pdfminer fallback)
    → [parsers/__init__.py] detect_broker() from regex fingerprints (first 3000 chars)
    → Match: [parsers/<broker>.py] rule-based parse (confidence 0.92)
      No match: [claude_fallback.py] Claude AI parse (confidence 0.75)
    → [parse_result.py] ParseResult frozen dataclass
    → [parse_result.py] validate_result() validates fields
    → [client_lookup.py] enrich from Excel (NEW LR CLIENT LIST 2026.xlsx)
    → [tools_jira.py] duplicate check → create ticket + attach PDF
```

## Field Rules

- **Title**: `{LIST NAME} - {MAILER NAME} - {MANAGER ORDER NUMBER}` — List Name first, Mailer Name second, Manager Order Number last (never Mailer PO). e.g. `JUDICIAL WATCH DONORS - HERITAGE FOUNDATION - W74926JW`
- **Description**: Full PDF text content, never as a comment
- **List Manager** = must be one of these exact values: ADSTRA, AALC, AMLC, CELCO, CONRAD, DATA-AXLE, KAP, MARY E GRANGER, NEGEV, NAMES IN THE NEWS, RKD, RMI, WASHINGTON LISTS, WE ARE MOORE
- **Mailer Name** = organization sending the mail. **List Name** = donor list being rented. Never swap.
- **Availability Rule**: "Full Run" = "All Available", "NTH NAME" = "Nth"
- **Other Fees**: "STATE OMITS" when omission has 6+ states/zips/SCFs
- **Special Seed Instructions**: Only "Insert:" lines. Never FTP/email info. Blank for most orders.
- **Status on creation**: Always "Needs Assignment". Never transition on creation.

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
| Work Order | customfield_12089 | text | |
| Client Database | customfield_12155 | select | 94 options |
| Seed Database | customfield_12156 | select | 85 options |
| Billable Account | customfield_12191 | select | 95 options |
| Manager Order Number | customfield_12192 | text | Used in title |
| Mailer PO | customfield_12193 | text | Duplicate check field |
| Mailer Name | customfield_12194 | text | |
| Key Code | customfield_12195 | text | |
| Mail Date | customfield_12196 | date | YYYY-MM-DD |
| List Manager | customfield_12231 | text | Broker company |
| Requestor Name | customfield_12232 | text | |
| Requestor Email | customfield_12233 | text | |
| List Name | customfield_12234 | text | Abbreviation |
| Omission Description | customfield_12270 | ADF | |
| Requested Quantity | customfield_12271 | number | Integer |
| Seed Tracking Number | customfield_12272 | text | = Manager Order # |
| Availability Rule | customfield_12273 | select | Nth=13235, All Available=13236 |
| File Format | customfield_12274 | select | ASCII Delimited=13237, ASCII Fixed=13238, Excel=13239, Other=13240 |
| Ship To Email | customfield_12275 | text | |
| Shipping Method | customfield_12276 | select | Email=13241, FTP=13242, Other=13243 |
| Shipping Instructions | customfield_12277 | text | CC: email@domain.com |
| Other Fees | customfield_12278 | text | |
| Special Seed Instructions | customfield_12311 | text | Only "Insert:" lines |
| Due Date | duedate | date | Ship By date |

## Billable Account / Client DB / Seed DB

From Excel lookup via db_code (e.g., F41D):
- Billable Account = db_code without suffix (F41)
- Client Database = full db_code (F41D)
- Seed Database = db_code with S suffix (F41S)

### Always fill all the feild in the JIRA tickets.

## Key Code Patterns

| Broker | Source |
|--------|--------|
| Conrad Direct | Text after "And"/"&" on MATERIAL line. Not always present. e.g. "...PO# L50278HF & HF Thirteen Star Flag #2215A" → Key Code = "HF Thirteen Star Flag #2215A" |
| Data Axle | "Key Code:" field or Order# suffix |
| Others | Extracted from order if present |

## Additional Field Rules (from Lee Ann Hazelwood)

- **Mailer / Broker fields**: Treated identically for list rental orders — interchangeable on order forms.
- **Special Seed Instructions**: Blank for most orders. FTP details and email addresses never go here.
- **Shipping Instructions**: `CC: {requestor_email}` using the broker's requestor email.
- **Availability Rule**: "Full Run" = "All Available". Confirmed.
- **List Name abbreviations**: FAIR = Federation for American Immigration Reform.

## Supported Brokers (10)

data_axle, simiocloud, rmi_direct, celco, rkd_group, amlc, kap, washington_lists, conrad_direct, names_in_news

Note: ADSTRA and WE ARE MOORE have no dedicated parsers — they fall through to `claude_fallback.py`.

## Adding a New Broker Parser

1. Create `parsers/my_broker.py` inheriting from `BaseBrokerParser`
2. Implement `parse(text: str) -> ParseResult`
3. Register in `PARSER_REGISTRY` in `parsers/__init__.py`
4. Add detection regex to `_RULES` in `parsers/__init__.py`

## Parser Internals

- **BaseBrokerParser** (`parsers/base.py`): Shared helpers — `_find()`, `_find_date()`, `_find_quantity()`, `_map_shipping_method()`, `_detect_file_format()`, `_detect_state_omits()`, `_extract_special_seed_instructions()`
- **CONFIDENCE_RULE_BASED** = 0.92 — never hardcode, import from base
- **Broker detection** (`parsers/__init__.py`): Pre-compiled regex patterns in `_RULES`, called via `detect_broker()`
- **Client lookup** (`client_lookup.py`): Reads `NEW LR CLIENT LIST 2026.xlsx`. Exact db_code match first, then fuzzy name match (≥50% word overlap).
