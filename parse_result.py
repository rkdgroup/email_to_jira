"""
ParseResult dataclass and validation.

ParseResult is the immutable output from all parsers.
validate_result() checks fields before Jira ticket creation.
"""

import re
import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

VALID_AVAILABILITY = {"Nth", "All Available"}
VALID_FILE_FORMAT = {"ASCII Delimited", "ASCII Fixed", "Excel", "Other"}
VALID_SHIPPING_METHOD = {"Email", "FTP", "Other"}
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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
    segment_criteria: str = ""          # e.g. "12MOS $10+ DONORS" (ADSTRA Selects: field)

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


@dataclass
class ValidationResult:
    valid: bool = True
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


def validate_result(result: ParseResult) -> ValidationResult:
    """Validate a ParseResult. Returns ValidationResult with valid=False if hard errors found."""
    v = ValidationResult()

    # --- Hard errors (block ticket creation) ---
    for fld, label in [
        ("mailer_name", "Mailer Name"),
        ("mailer_po", "Mailer PO"),
        ("list_name", "List Name"),
        ("list_manager", "List Manager"),
    ]:
        val = getattr(result, fld, "")
        if not val or not val.strip():
            v.errors.append(f"Missing required field: {label}")
            v.valid = False

    if not result.requested_quantity or result.requested_quantity <= 0:
        v.errors.append("Missing or zero requested_quantity")
        v.valid = False

    for fld, label in [("mail_date", "Mail Date"), ("ship_by_date", "Ship By Date")]:
        val = getattr(result, fld, "")
        if val and not DATE_PATTERN.match(val):
            v.errors.append(f"Invalid date format for {label}: {val!r} (expected YYYY-MM-DD)")
            v.valid = False

    if result.availability_rule and result.availability_rule not in VALID_AVAILABILITY:
        v.errors.append(f"Invalid availability_rule: {result.availability_rule!r}")
        v.valid = False

    if result.file_format and result.file_format not in VALID_FILE_FORMAT:
        v.errors.append(f"Invalid file_format: {result.file_format!r}")
        v.valid = False

    if result.shipping_method and result.shipping_method not in VALID_SHIPPING_METHOD:
        v.errors.append(f"Invalid shipping_method: {result.shipping_method!r}")
        v.valid = False

    # --- Warnings (logged, do not block) ---
    for fld in ("requestor_email", "ship_to_email"):
        val = getattr(result, fld, "")
        if val and not re.match(r"^[\w.+-]+@[\w.-]+\.\w+$", val):
            v.warnings.append(f"Suspicious email format in {fld}: {val!r}")

    if not result.mail_date:
        v.warnings.append("Missing mail_date")
    if not result.ship_by_date:
        v.warnings.append("Missing ship_by_date")
    if not result.requestor_email and not result.ship_to_email:
        v.warnings.append("No email addresses found")

    return v
