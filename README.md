# DSLF List Rental Automation

Automated pipeline that processes list rental purchase order PDFs and creates fully populated DSLF tickets in Jira.

## How It Works

```
PDF → Extract Text → Detect Broker → Parse Fields → Validate → Enrich → Create Jira Ticket
```

1. **Extract** text from PDF (PyMuPDF primary, pdfminer fallback)
2. **Detect** which broker format the order uses (12 known brokers via regex fingerprints on the first 3000 chars)
3. **Parse** structured fields using the broker-specific rule-based parser (confidence 0.92). A PDF matching no broker is flagged for review — no ticket is created (there is no Claude AI fallback parser).
4. **Validate** required fields, dates, enums
5. **Enrich** billable account, client database, and list manager from the `config/*.yaml` lookups
6. **Create** DSLF Jira ticket with 25+ custom fields, PDF attached, order content in description

## Quick Start

```bash
# Install dependencies
pip install requests python-dotenv pyyaml pymupdf pdfminer.six pymupdf4llm python-docx openpyxl msal

# Configure credentials
cp .env.example .env
# Edit .env with your Jira credentials

# Process a single PDF
python parse_pipeline.py /path/to/order.pdf

# Dry run (extract + validate, no ticket created)
python parse_pipeline.py /path/to/order.pdf --dry-run --verbose

# Process all PDFs in a folder
python parse_pipeline.py /path/to/folder/
```

> **Every structured field is rule-based.** There is no Claude fallback *parser* — unrecognized PDFs are flagged for review, not parsed by an LLM. The one LLM step in the live pipeline is `tools_polish.py`, which structurally cleans the Description and Omission Description after parsing (joining lines the PDF wrapped, moving omit criteria to the right field); it never rewords, and falls back to the parser's own text on any failure. `anthropic` is also required by the offline AI tools (`ai_extract.py`, `compare_extraction.py`, `hybrid_create.py`).

## Configuration

Create a `.env` file in the project root:

```
JIRA_BASE_URL=https://your-instance.atlassian.net
JIRA_EMAIL=your@email.com
JIRA_API_TOKEN=your_api_token
```

The email/qty scanners additionally require Microsoft Graph (MSAL) credentials —
`MS_*` variables — for reading the shared inbox. See `email_scanner/`.

## Supported Brokers (12)

Detection is defined in `_RULES` in `parsers/__init__.py`; **all** patterns for a
broker must match for it to be selected (rules are evaluated in the order below).

| Broker | Parser | Detection Pattern |
|--------|--------|------------------|
| ADSTRA | `adstra.py` | `adstradata.com` + `Adstra order#` |
| AMLC | `amlc.py` | "American Mailing Lists Corporation Management" + "Service Bureau/Purchase Order No" |
| RKD Group | `amlc.py` (`RkdGroupParser`) | "RKD GROUP" + "Service Bureau No" |
| CELCO | `celco.py` | "LIST EXCHANGE/RENTAL ORDER" + "CELCO" |
| SimioCloud | `data_axle.py` (`SimioCloudParser`) | "Exchange/Rental Order" + "SimioCloud" |
| Data Axle | `data_axle.py` | "Exchange/Rental Order" + "Data Axle" |
| RMI Direct | `rmi_direct.py` | "RMI Direct Marketing" + "Exchange/Rental Instruction" |
| KAP | `kap.py` | "KAP Order" / "keyacquisition(partners).com" / "LIST MANAGEMENT DIVISION" |
| Washington Lists | `washington_lists.py` | "Washington Lists, Inc." |
| We Are Moore | `we_are_moore.py` | "We Are Moore" / "wearemoore.com" |
| Conrad Direct | `conrad_direct.py` | "PURCHASE ORDER NO:" + "Conrad Direct" |
| Names in News | `names_in_news.py` | "List Order" + "Fulfillment Copy" |

## Project Structure

```
├── parse_pipeline.py       # Main entry point (single PDF or folder → Jira ticket)
├── tools_pdf.py            # PDF text extraction (PyMuPDF / pdfminer)
├── tools_jira.py           # Jira REST API integration
├── parse_result.py         # ParseResult dataclass + validate_result()
├── client_lookup.py        # Client enrichment from config/*.yaml
├── client_profiles.py      # Locate/read client profile .doc(x) sheets
├── qc_checker.py           # LLM QC: order-vs-ticket + ticket-vs-SELECT
├── qty_approval_scanner.py # Sets Requested Qty from Qty Approval emails
├── config_guard.py         # Fail-fast startup validation of config/*.yaml
├── verify_configs.py       # Deeper config audit vs source Excel/docs
├── build_profile_yaml.py   # Regenerate config/client_profiles.yaml from profiles
├── parsers/
│   ├── __init__.py         # detect_broker() + _RULES + PARSER_REGISTRY
│   ├── base.py             # BaseBrokerParser (shared helpers)
│   ├── adstra.py           # ADSTRA
│   ├── data_axle.py        # Data Axle (+ SimioCloud)
│   ├── rmi_direct.py       # RMI Direct Marketing
│   ├── celco.py            # CELCO
│   ├── amlc.py             # AMLC (+ RKD Group)
│   ├── kap.py              # Key Acquisition Partners
│   ├── washington_lists.py # Washington Lists
│   ├── conrad_direct.py    # Conrad Direct
│   ├── names_in_news.py    # Names in the News
│   └── we_are_moore.py     # We Are Moore
├── config/                 # 20 runtime YAML lookups (client/broker/profile data)
├── email_scanner/          # Inbound-email → pipeline (MSAL auth, Jenkins-scheduled)
├── ticket_scanner/         # Scheduled DSLF ticket scanner + reports
├── Client Profiles/        # Source .doc/.docx client profile sheets
├── .env                    # Credentials (not in git)
├── CLAUDE.md               # Claude Code context
└── NEW LR CLIENT LIST 2026.xlsx  # Source Excel (config/*.yaml generated from it)
```

## Other Tools

| Tool | Purpose |
|------|---------|
| `qc_checker.py` | The QC checker. Two LLM checks per ticket: was it created correctly from the broker's order, and did the SELECT deliver it. Posts a comment; `--fix` writes the order-check corrections back. Never transitions the ticket. |
| `qty_approval_scanner.py` | Reads Qty Approval emails / SELECT PDFs and sets the ticket's Requested Qty. |
| `email_scanner/` | Watches the shared inbox and routes broker PDFs into `parse_pipeline` by sender domain. |
| `ticket_scanner/` | Scheduled scan of DSLF tickets, writing reports under `ticket_scanner/reports/`. |
| `config_guard.py` | Run at the top of the Jenkins entry points; aborts loudly (exit 1) on any malformed `config/*.yaml`. |
| `verify_configs.py` | Audits the config YAMLs against the source Excel/profile docs and writes `config_audit_report.md`. |
| `build_profile_yaml.py` | Regenerates `config/client_profiles.yaml` from the `Client Profiles/` `.doc(x)` sheets. |

## Adding a New Broker

1. Create `parsers/my_broker.py` inheriting from `BaseBrokerParser`
2. Implement `parse(text: str) -> ParseResult`
3. Register the parser in `PARSER_REGISTRY` in `parsers/__init__.py`
4. Add detection patterns to `_RULES` in `parsers/__init__.py`

## Jira Integration

- **Project**: DSLF
- **Issue Type**: List Fulfillment (ID: 11806)
- **Duplicate Detection**: JQL query on Mailer PO field before creation
- **Status on Creation**: Needs Assignment (no auto-transition)
```
