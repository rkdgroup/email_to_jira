# DSLF List Rental Automation

Processes list rental purchase order PDFs from brokers and creates fully populated DSLF tickets in Jira.

```
PDF → Extract Text → Detect Broker → Parse Fields → Validate → Enrich → Create Jira Ticket
```

Every structured field is rule-based (one parser per broker, 12 brokers, detected by regex
fingerprints on the first 3000 characters). A PDF that matches no broker is flagged for review and
creates no ticket. The only LLM step in the live pipeline is `tools_polish.py`, which cleans the
Description and Omission Description prose after parsing; it never rewords and falls back to the
parser's text on any failure. `qc_checker.py` (LLM) checks tickets after creation.

## Quick Start

```bash
pip install -r requirements.txt     # covers every runtime import

# Create .env in the repo root (Jira creds at minimum):
#   JIRA_BASE_URL=https://your-instance.atlassian.net
#   JIRA_EMAIL=your@email.com
#   JIRA_API_TOKEN=your_api_token
# CLAUDE.md's "Dependencies & Credentials" table lists every var and what needs it.
# The IBM i work-order step also needs jt400.jar, which is not in the repo.

python parse_pipeline.py /path/to/order.pdf              # single PDF
python parse_pipeline.py /path/to/folder/                # every PDF in a folder
python parse_pipeline.py /path/to/order.pdf --dry-run    # no ticket created
```

## Adding a Broker

1. Create `parsers/my_broker.py` inheriting `BaseBrokerParser`; implement `parse(text) -> ParseResult`.
2. Register it in `PARSER_REGISTRY` in `parsers/__init__.py`.
3. Add its detection patterns to `_RULES` in `parsers/__init__.py`, ordered so it neither shadows nor is shadowed by another broker.

## More

`CLAUDE.md` is the authoritative guide (architecture, field rules, scheduled jobs, QC, IBM i work
orders, tests). `docs/DSLF_REFERENCE.md` holds the incident history behind those rules.
