# Jira Automation — DSLF List Rental Pipeline

## What This Is

An end-to-end automation system that monitors a shared Microsoft Outlook inbox for incoming emails containing list rental purchase order PDFs, extracts structured fields from those PDFs using a hybrid rule-based + Claude AI pipeline, and automatically creates DSLF project tickets in Jira (rkdgroup.atlassian.net). After each successful ticket creation, an IBM i processing card is attached as a comment and a Slack/Teams notification is sent.

**Target project:** DSLF (Data Management Incorporated — List Fulfillment)
**Jira instance:** `https://rkdgroup.atlassian.net` — Issue Type ID `11806`

## Core Value

A purchase order PDF arriving in the shared inbox becomes a fully populated Jira DSLF ticket with zero manual data entry — every time, for every known broker format.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Monitor shared Microsoft Outlook / Exchange inbox for new emails with PDF attachments
- [ ] Extract full text from PDF attachments (pdfminer / PyMuPDF)
- [ ] Detect broker format from PDF text using regex fingerprints (10+ known brokers)
- [ ] Parse structured fields with rule-based broker parsers (confidence 0.92)
- [ ] Fall back to Claude Opus 4.6 AI parsing for unrecognized broker formats (confidence 0.75)
- [ ] Validate parsed result (required fields, date formats, enum values)
- [ ] Check for duplicate tickets via JQL on Mailer PO field before creating
- [ ] Enrich billable account and list manager from Excel client lookup (NEW LR CLIENT LIST 2026.xlsx)
- [ ] Create DSLF Jira ticket with all custom fields populated (25+ fields)
- [ ] Attach IBM i processing card as a comment on the created ticket
- [ ] Send Slack/Teams notification with ticket link after successful creation
- [ ] Flag orders for human review when validation fails, duplicate detected, or extraction fails
- [ ] Support dry-run mode (extract + validate, no ticket created)
- [ ] Support batch processing of a folder of PDFs
- [ ] Support both pipeline entry point (rule-based first) and orchestrator entry point (Claude-driven)

### Out of Scope

- Mobile app or web UI for managing tickets — Jira itself is the UI
- Reply-to-sender email confirmation — Slack/Teams notification is sufficient
- OAuth / user login — service account credentials via .env
- Real-time streaming — polling interval is acceptable

## Context

The parsing architecture uses two entry points:

1. **`parse_pipeline.py`** — Rule-based parsers first, Claude fallback only when no broker pattern matches. Predictable, fast, low cost.
2. **`orchestrator.py`** — Claude drives the entire workflow as a tool-use loop (agentic). More flexible, higher cost.

**Known broker parsers (10):** data_axle, simiocloud, rmi_direct, celco, rkd_group, amlc, kap, washington_lists, conrad_direct, names_in_news.

**Key extraction rules (critical):**
- List Manager = the data/list company (e.g. ADSTRA), NOT the broker
- Requestor Name/Email = contact at the data company (Ship-To section), NOT the broker contact
- Shipping Instructions format: `CC: email@domain.com`
- Availability Rule: `"All Available"` if "X OR ALL AVAILABLE", otherwise `"Nth"`
- Ticket summary format: `[MAILER NAME] - [LIST NAME] - PO [PO NUMBER]`
- Ticket status on creation: always "Needs Assignment" (never auto-transition)

**Email ingestion:** Microsoft Outlook shared inbox monitored via Microsoft Graph API (OAuth 2.0 client credentials flow for service account access).

## Constraints

- **Tech stack**: Python — consistent with existing parsers and tooling
- **Jira API**: REST API v3 — 25+ custom fields with static option IDs documented in knowledge base
- **AI model**: Claude Opus 4.6 for fallback parsing and orchestrator mode
- **Email**: Microsoft Graph API for Outlook inbox monitoring
- **Config**: Credentials via `.env` (ANTHROPIC_API_KEY, JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN, plus MS Graph credentials)
- **Client data**: `NEW LR CLIENT LIST 2026.xlsx` must be present for account enrichment

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Hybrid parsing (rule-based + Claude fallback) | Rule-based is fast/cheap for known brokers; Claude handles novel formats | — Pending |
| Microsoft Graph API for Outlook | Service account access without user interaction; supports shared mailbox | — Pending |
| Flag-for-review instead of fail-hard | Preserves human oversight for edge cases; no lost orders | — Pending |
| Immutable ParseResult dataclass | Forces explicit validation before Jira submission; prevents partial writes | — Pending |

---
*Last updated: 2026-03-17 after initialization*
