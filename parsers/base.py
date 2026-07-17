"""Base class for broker-specific parsers."""

import re
from abc import ABC, abstractmethod
from parse_result import ParseResult

CONFIDENCE_RULE_BASED = 0.92


class BaseBrokerParser(ABC):
    """All broker parsers inherit from this and implement parse()."""

    broker_key: str = ""

    @abstractmethod
    def parse(self, text: str) -> ParseResult:
        """Parse PDF text and return a ParseResult."""
        ...

    # --- Shared helper methods ---

    def _find(self, text: str, pattern: str, group: int = 1, default: str = "") -> str:
        """Find a regex pattern and return the specified group, or default."""
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        return m.group(group).strip() if m else default

    def _find_date(self, text: str, pattern: str) -> str:
        """Find a date and normalize to YYYY-MM-DD."""
        raw = self._find(text, pattern)
        if not raw:
            return ""
        return self._normalize_date(raw)

    def _normalize_date(self, raw: str) -> str:
        """Convert common date formats to YYYY-MM-DD."""
        raw = raw.strip()
        if not raw:
            return ""

        # Already YYYY-MM-DD
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
        if m:
            return raw

        # MM/DD/YYYY or MM/DD/YY
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", raw)
        if m:
            month, day, year = m.groups()
            if len(year) == 2:
                year = f"20{year}"
            return f"{year}-{int(month):02d}-{int(day):02d}"

        # MM-DD-YYYY
        m = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{2,4})$", raw)
        if m:
            month, day, year = m.groups()
            if len(year) == 2:
                year = f"20{year}"
            return f"{year}-{int(month):02d}-{int(day):02d}"

        return ""

    def _find_quantity(self, text: str, pattern: str) -> tuple[int, str]:
        """
        Find quantity and availability rule.
        Returns (quantity_int, availability_rule).
        If Nth: returns the exact number stated. If All Available: returns the
        approximate number stated near "ALL AVAILABLE" (or any number found).
        """
        raw = self._find(text, pattern)
        if not raw:
            return 0, ""

        # Check for "ALL AVAILABLE"
        if re.search(r"ALL\s+AVAILABLE", raw, re.IGNORECASE):
            # Try "X OR ALL AVAILABLE" format first
            m = re.search(r"([\d,]+)\s+(?:OR\s+)?ALL\s+AVAILABLE", raw, re.IGNORECASE)
            if not m:
                # ALL AVAILABLE without a preceding number — use any number in raw as approximation
                m = re.search(r"([\d,]+)", raw)
            qty = int(m.group(1).replace(",", "")) if m else 0
            return qty, "All Available"

        # Fixed (Nth) quantity — must be exact
        m = re.search(r"([\d,]+)", raw)
        qty = int(m.group(1).replace(",", "")) if m else 0
        return qty, "Nth"

    def _collect_continuation_block(
        self,
        text: str,
        anchor_pattern: str,
        cont_pattern: str = r"^\s*OR\s*=",
        max_lines: int = 30,
    ) -> str:
        """
        Capture a multi-line block: the first line matching anchor_pattern,
        then any immediately-following lines that match cont_pattern (e.g. "OR = ...").
        Returns all matched lines stripped and joined with newlines.
        """
        found_lines: list[str] = []
        capturing = False
        for line in text.split("\n"):
            stripped = line.strip()
            if not capturing:
                if re.search(anchor_pattern, stripped, re.IGNORECASE):
                    capturing = True
                    found_lines.append(stripped)
            else:
                if stripped and re.match(cont_pattern, stripped, re.IGNORECASE):
                    found_lines.append(stripped)
                    if len(found_lines) >= max_lines:
                        break
                else:
                    break
        return "\n".join(found_lines)

    def _map_shipping_method(self, raw: str) -> str:
        """Map raw shipping method text to standard value."""
        if not raw:
            return ""
        raw_lower = raw.lower().strip()
        if "email" in raw_lower or "e-mail" in raw_lower:
            return "Email"
        if "ftp" in raw_lower:
            return "FTP"
        return "Other"

    def _is_saturn_order(self, text: str) -> bool:
        """True if the order routes to Saturn Corp (FileShare upload / Convert@saturncorp.com).

        Saturn orders are ALWAYS ASCII Fixed delivered via FTP, even when the ship-to on the
        order form is a notify email rather than the Saturn address itself.
        """
        return bool(text) and "saturn" in text.lower()

    def _detect_file_format(self, text: str) -> str:
        """Detect file format from PDF text."""
        lower = text.lower()
        if "fixed field" in lower or "ascii fixed" in lower or "fixed field text format" in lower:
            return "ASCII Fixed"
        if "ascii delimited" in lower or "csv" in lower or "comma" in lower:
            return "ASCII Delimited"
        if "excel" in lower or ".xls" in lower:
            return "Excel"
        if "e-mail transmission" in lower:
            return "Other"
        return ""

    # --- US state abbreviations for omit counting ---
    _US_STATES = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC",
    }

    # --- Full state name → abbreviation (for canonical STATE OMITS lines) ---
    _STATE_NAME_TO_ABBR = {
        "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
        "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT",
        "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC", "FLORIDA": "FL",
        "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL",
        "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS", "KENTUCKY": "KY",
        "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
        "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN",
        "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT",
        "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH",
        "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
        "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH",
        "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA",
        "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD",
        "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT",
        "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
        "WISCONSIN": "WI", "WYOMING": "WY",
    }

    def _state_codes_from_omit(self, omit_line: str) -> list[str]:
        """
        If an OMIT line is purely a list of US states (abbreviations and/or
        full names, e.g. "OMIT NJ AND DC" or "OMIT NEW JERSEY, DC"), return the
        de-duplicated list of state abbreviations. If the line contains any
        non-state content (cities, ZIPs, countries, "USA NAMES ONLY", etc.),
        return an empty list so the caller keeps the line verbatim.
        """
        if not omit_line:
            return []
        body = re.sub(r"^\s*OMIT\s+", "", omit_line, flags=re.IGNORECASE).strip()
        if not body:
            return []
        upper = body.upper()
        # Replace full state names with abbreviations (longest first so
        # "WEST VIRGINIA" wins over "VIRGINIA").
        for name in sorted(self._STATE_NAME_TO_ABBR, key=len, reverse=True):
            upper = re.sub(rf"\b{re.escape(name)}\b", self._STATE_NAME_TO_ABBR[name], upper)
        codes: list[str] = []
        for tok in re.split(r"[,\s&]+", upper):
            tok = tok.strip(".").strip()
            if tok in ("", "AND"):
                continue
            if tok in self._US_STATES:
                if tok not in codes:
                    codes.append(tok)
            else:
                return []  # non-state token → not a pure state-omit line
        return codes

    def _detect_state_omits(self, omission_description: str) -> str:
        """
        If omission_description contains 6+ US states, zip codes, or SCFs,
        return "State Omits" for the other_fees field.
        """
        if not omission_description:
            return ""
        upper = omission_description.upper()
        # Count state abbreviations
        state_count = sum(1 for s in self._US_STATES if re.search(rf"\b{s}\b", upper))
        # Count zip codes (5-digit or 3-digit SCF prefixes)
        zip_matches = re.findall(r"\b\d{3,5}\b", omission_description)
        total = state_count + len(zip_matches)
        if total >= 6:
            return "State Omits"
        return ""

    def _extract_special_seed_instructions(self, text: str) -> str:
        """Extract special seed instructions from PDF text."""
        # Look for "Insert:" or "Special Seed" patterns
        m = re.search(r"(?:Insert|SEED\s*INSTRUCTIONS?)[:\s]+(.+?)(?:\n|$)", text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return ""

    def _find_email(self, text: str, pattern: str = None) -> str:
        """Find an email address, optionally near a pattern."""
        if pattern:
            section = self._find(text, pattern)
            if section:
                m = re.search(r"[\w.+-]+@[\w.-]+\.\w+", section)
                if m:
                    return m.group()
        # Generic email search
        m = re.search(r"[\w.+-]+@[\w.-]+\.\w+", text)
        return m.group() if m else ""
