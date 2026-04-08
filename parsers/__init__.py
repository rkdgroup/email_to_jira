"""
Broker detection and parser registry.

detect_broker() identifies the broker format from PDF text.
PARSER_REGISTRY maps broker keys to parser instances.
"""

import re
import logging
from dataclasses import dataclass

from parsers.adstra import AdstraParser
from parsers.data_axle import DataAxleParser, SimioCloudParser
from parsers.rmi_direct import RmiDirectParser
from parsers.celco import CelcoParser
from parsers.rkd_group import RkdGroupParser, AmlcParser
from parsers.kap import KapParser
from parsers.washington_lists import WashingtonListsParser
from parsers.conrad_direct import ConradDirectParser
from parsers.names_in_news import NamesInNewsParser

log = logging.getLogger(__name__)

# --- Broker Detection ---

SCAN_LENGTH = 3000
CONFIDENCE_RULE_BASED = 0.92


@dataclass(frozen=True)
class BrokerMatch:
    broker_key: str
    confidence: float
    matched_patterns: tuple


_RULES = [
    ("adstra", [
        re.compile(r"adstradata\.com", re.IGNORECASE),
        re.compile(r"Adstra\s+order#", re.IGNORECASE),
    ]),
    ("rkd_group", [
        re.compile(r"RKD\s+GROUP", re.IGNORECASE),
        re.compile(r"Service\s+Bureau\s+No", re.IGNORECASE),
    ]),
    ("amlc", [
        re.compile(r"American\s+Mailing\s+Lists\s+Corporation\s+Management", re.IGNORECASE),
        re.compile(r"(?:Service\s+Bureau|Purchase\s+Order)\s+No", re.IGNORECASE),
    ]),
    ("celco", [
        re.compile(r"LIST\s+(?:EXCHANGE|RENTAL)\s+ORDER", re.IGNORECASE),
        re.compile(r"CELCO", re.IGNORECASE),
    ]),
    ("simiocloud", [
        re.compile(r"(?:Exchange|Rental)\s+Order", re.IGNORECASE),
        re.compile(r"SimioCloud", re.IGNORECASE),
    ]),
    ("data_axle", [
        re.compile(r"(?:Exchange|Rental)\s+Order", re.IGNORECASE),
        re.compile(r"Data\s+Axle", re.IGNORECASE),
    ]),
    ("rmi_direct", [
        re.compile(r"RMI\s+Direct\s+Marketing", re.IGNORECASE),
        re.compile(r"(?:Exchange|Rental)\s*Instruction", re.IGNORECASE),
    ]),
    ("kap", [
        re.compile(r"KAP\s+Order|keyacquisitionpartners\.com|keyacquisition\.com|LIST\s+MANAGEMENT\s+DIVISION", re.IGNORECASE),
    ]),
    ("washington_lists", [
        re.compile(r"Washington\s+Lists,?\s+Inc", re.IGNORECASE),
    ]),
    ("conrad_direct", [
        re.compile(r"PURCHASE\s+ORDER\s+NO:", re.IGNORECASE),
        re.compile(r"Conrad\s+Direct", re.IGNORECASE),
    ]),
    ("names_in_news", [
        re.compile(r"List\s+Order", re.IGNORECASE),
        re.compile(r"Fulfillment\s+Copy", re.IGNORECASE),
    ]),
]


def detect_broker(text: str) -> BrokerMatch | None:
    """Detect broker format from PDF text. Returns BrokerMatch or None."""
    scan_text = text[:SCAN_LENGTH]

    for broker_key, patterns in _RULES:
        matched = []
        all_match = True
        for pattern in patterns:
            m = pattern.search(scan_text)
            if m:
                matched.append(m.group())
            else:
                all_match = False
                break

        if all_match:
            log.info("Broker detected: %s (patterns: %s)", broker_key, matched)
            return BrokerMatch(
                broker_key=broker_key,
                confidence=CONFIDENCE_RULE_BASED,
                matched_patterns=tuple(matched),
            )

    log.info("No broker pattern matched in first %d chars", SCAN_LENGTH)
    return None


# --- Parser Registry ---

PARSER_REGISTRY = {
    "adstra":           AdstraParser(),
    "data_axle":        DataAxleParser(),
    "simiocloud":       SimioCloudParser(),
    "rmi_direct":       RmiDirectParser(),
    "celco":            CelcoParser(),
    "rkd_group":        RkdGroupParser(),
    "amlc":             AmlcParser(),
    "kap":              KapParser(),
    "washington_lists":  WashingtonListsParser(),
    "conrad_direct":    ConradDirectParser(),
    "names_in_news":    NamesInNewsParser(),
}
