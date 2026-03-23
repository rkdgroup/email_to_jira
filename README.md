# DSLF List Rental Automation

Automated pipeline that processes list rental purchase order PDFs and creates fully populated DSLF tickets in Jira.

## How It Works

```
PDF → Extract Text → Detect Broker → Parse Fields → Validate → Enrich → Create Jira Ticket
```

1. **Extract** text from PDF (PyMuPDF primary, pdfminer fallback)
2. **Detect** which broker format the order uses (10 known brokers via regex fingerprints)
3. **Parse** structured fields using broker-specific parser (or Claude AI fallback for unknown formats)
4. **Validate** required fields, dates, enums
5. **Enrich** billable account and client database from Excel lookup
6. **Create** DSLF Jira ticket with 25+ custom fields, PDF attached, order content in description

## Quick Start

```bash
# Install dependencies
pip install anthropic requests pymupdf pdfminer.six pymupdf4llm openpyxl python-dotenv

# Configure credentials
cp .env.example .env
# Edit .env with your Jira and Anthropic API credentials

# Process a single PDF
python parse_pipeline.py /path/to/order.pdf

# Dry run (extract + validate, no ticket created)
python parse_pipeline.py /path/to/order.pdf --dry-run --verbose

# Process all PDFs in a folder
python parse_pipeline.py /path/to/folder/
```

## Configuration

Create a `.env` file in the project root:

```
JIRA_BASE_URL=https://your-instance.atlassian.net
JIRA_EMAIL=your@email.com
JIRA_API_TOKEN=your_api_token
ANTHROPIC_API_KEY=your_key  # Optional, for unknown broker fallback
```

## Supported Brokers

| Broker | Parser | Detection Pattern |
|--------|--------|------------------|
| Data Axle | `data_axle.py` | "Exchange/Rental Order" + "Data Axle" |
| SimioCloud | `simiocloud.py` | "Exchange/Rental Order" + "SimioCloud" |
| RMI Direct | `rmi_direct.py` | "RMI Direct Marketing" + "Exchange Instruction" |
| CELCO | `celco.py` | "LIST EXCHANGE/RENTAL ORDER" + "CELCO" |
| RKD Group | `rkd_group.py` | "RKD GROUP" + "Service Bureau No" |
| AMLC | `amlc.py` | "American Mailing Lists Corporation" |
| KAP | `kap.py` | "LIST MANAGEMENT DIVISION" + "KAP ORDER" |
| Washington Lists | `washington_lists.py` | "Washington Lists, Inc." |
| Conrad Direct | `conrad_direct.py` | "PURCHASE ORDER NO:" + "Conrad Direct" |
| Names in News | `names_in_news.py` | "List Order" + "Fulfillment Copy" |

## Project Structure

```
├── parse_pipeline.py       # Main entry point
├── tools_pdf.py            # PDF text extraction
├── broker_detector.py      # Broker format detection
├── parse_result.py         # ParseResult dataclass
├── result_validator.py     # Field validation
├── client_lookup.py        # Excel client enrichment
├── claude_fallback.py      # Claude AI fallback parser
├── tools_jira.py           # Jira REST API integration
├── parsers/
│   ├── base.py             # BaseBrokerParser (shared helpers)
│   ├── data_axle.py        # Data Axle / SimioCloud
│   ├── rmi_direct.py       # RMI Direct Marketing
│   ├── celco.py            # CELCO
│   ├── rkd_group.py        # RKD Group
│   ├── amlc.py             # AMLC
│   ├── kap.py              # Key Acquisition Partners
│   ├── washington_lists.py # Washington Lists
│   ├── conrad_direct.py    # Conrad Direct
│   ├── names_in_news.py    # Names in the News
│   └── simiocloud.py       # SimioCloud (reuses Data Axle)
├── .env                    # Credentials (not in git)
├── CLAUDE.md               # Claude Code context
└── NEW LR CLIENT LIST 2026.xlsx  # Client lookup data
```

## Adding a New Broker

1. Create `parsers/my_broker.py` inheriting from `BaseBrokerParser`
2. Implement `parse(text: str) -> ParseResult`
3. Register in `PARSER_REGISTRY` in `parsers/__init__.py`
4. Add detection patterns to `_RULES` in `broker_detector.py`

## Jira Integration

- **Project**: DSLF (Data Services List Fulfillment)
- **Issue Type**: List Fulfillment (ID: 11806)
- **Duplicate Detection**: JQL query on Mailer PO field before creation
- **Status on Creation**: Needs Assignment (no auto-transition)
