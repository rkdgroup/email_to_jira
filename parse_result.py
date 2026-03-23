"""
Immutable ParseResult dataclass — output from all parsers (rule-based and Claude fallback).
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ParseResult:
    # Required identifiers
    source: str                     # e.g. "rule:data_axle", "claude_fallback"
    confidence: float = 0.0         # 0.0–1.0

    # Core required fields
    mailer_name: str = ""
    mailer_po: str = ""
    list_name: str = ""
    list_manager: str = ""
    requested_quantity: int = 0

    # Auto-built summary
    summary: str = ""

    # Optional fields
    manager_order_number: str = ""
    mail_date: str = ""             # YYYY-MM-DD
    ship_by_date: str = ""          # YYYY-MM-DD → duedate
    requestor_name: str = ""
    requestor_email: str = ""
    ship_to_email: str = ""
    key_code: str = ""
    billable_account: str = ""
    availability_rule: str = ""     # "Nth" or "All Available"
    file_format: str = ""           # "ASCII Delimited", "ASCII Fixed", "Excel", "Other"
    shipping_method: str = ""       # "Email", "FTP", "Other"
    shipping_instructions: str = "" # "CC: email@domain.com"
    omission_description: str = ""
    other_fees: str = ""
    special_seed_instructions: str = ""

    # Non-fatal issues
    warnings: tuple = field(default_factory=tuple)

    def __post_init__(self):
        # Auto-build summary if not provided
        # Title uses manager_order_number (NOT mailer_po) per Lee Ann's feedback
        if not self.summary and self.mailer_name and self.list_name:
            order_id = self.manager_order_number or self.mailer_po
            object.__setattr__(
                self, "summary",
                f"{self.list_name.upper()} - {self.mailer_name.upper()} - {order_id}"
            )

    def to_jira_kwargs(self) -> dict:
        """Convert to keyword arguments for create_jira_ticket()."""
        kwargs = {}
        for fld in (
            "summary", "mailer_name", "mailer_po", "list_name", "list_manager",
            "requested_quantity", "manager_order_number", "mail_date", "ship_by_date",
            "requestor_name", "requestor_email", "ship_to_email", "key_code",
            "billable_account", "availability_rule", "file_format", "shipping_method",
            "shipping_instructions", "omission_description", "other_fees",
            "special_seed_instructions",
        ):
            val = getattr(self, fld)
            if val:
                kwargs[fld] = val
        return kwargs
