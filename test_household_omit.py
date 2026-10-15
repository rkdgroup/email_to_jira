"""A household dedupe on the order must reach the ticket, whichever broker printed it.

DSLF-1429's KAP order said "**PLEASE PROVIDE ONE NAME PER HOUSEHOLD." with no OMIT word,
so the KAP parser dropped it and the ticket check failed. DSLF-1402 was the same drop on
Data Axle. Hermetic — no Jira, no PDFs, no network.
"""
import inspect

import parse_pipeline as pp
from parsers import base
from parsers.data_axle import DataAxleParser

_KAP_1429 = "\n".join([
    "Attn: see instructions below",
    "**PLEASE PROVIDE ONE NAME PER HOUSEHOLD. Must supply individual names. Records without",
    "first and last name will be dropped.",
    "executed.  Omit all APO, FPO, Foreign addresses DMA Panders.",
])


def test_kap_wording_adds_the_canonical_line():
    out = pp._with_household("Omit all APO, FPO, Foreign addresses DMA Panders", _KAP_1429)
    assert out == "Omit all APO, FPO, Foreign addresses DMA Panders\n1 PER HOUSEHOLD", out
    print("PASS DSLF-1429's ONE NAME PER HOUSEHOLD adds 1 PER HOUSEHOLD")


def test_every_wording_seen_on_tickets_is_read():
    for said in ("SUPPLY 1 NAME PER HOUSEHOLD", "1 PER HOUSEHOLD", "ONE PER HOUSEHOLD",
                 "Please supply one per household.", "1 PER HH", "1/PER HH",
                 "ONE NAME PER\nHOUSEHOLD", "1 PER HOUSE HOLD"):
        assert pp._with_household("OMIT: NJ", said) == "OMIT: NJ\n1 PER HOUSEHOLD", said
    print("PASS every wording on past tickets, and a PDF wrap, is read")


def test_an_omission_that_already_says_it_is_untouched():
    for omission in ("OMIT MN,NJ,APO/FPO - SUPPLY 1 NAME PER HOUSEHOLD", "1 PER HOUSEHOLD"):
        assert pp._with_household(omission, "SUPPLY 1 NAME PER HOUSEHOLD") == omission
    print("PASS adstra's verbatim line and data_axle's canonical line are not doubled")


def test_a_spaced_house_hold_is_respelled_not_doubled():
    # DSLF-1385: LRF's criteria_parser reads only "HOUSEHOLD", so the spaced form is rewritten.
    out = pp._with_household("OMIT: NJ\n1 PER HOUSE HOLD", "1 PER HOUSE HOLD")
    assert out == "OMIT: NJ\n1 PER HOUSEHOLD", out
    print("PASS a kept 1 PER HOUSE HOLD becomes 1 PER HOUSEHOLD, once")


def test_other_counts_and_negations_add_nothing():
    for said in ("2 PER HOUSEHOLD", "TWO NAMES PER HOUSEHOLD", "DO NOT SUPPLY ONE PER HOUSEHOLD",
                 "Don't limit to 1 per household", "OMIT: NJ"):
        assert pp._with_household("OMIT: NJ", said) == "OMIT: NJ", said
    print("PASS 2 per household, DO NOT and no mention add nothing")


def test_an_empty_omission_gets_just_the_line():
    assert pp._with_household("", "1 PER HH") == "1 PER HOUSEHOLD"
    print("PASS an empty omission becomes the one line")


def test_the_line_written_is_the_line_the_pattern_reads():
    # LRF's criteria_parser reads the same pattern; the canonical line must satisfy it.
    assert base.HOUSEHOLD_RE.search(base.HOUSEHOLD)
    assert DataAxleParser._HOUSEHOLD_RE is base.HOUSEHOLD_RE
    print("PASS one pattern, shared by the pipeline and data_axle")


def test_finalize_runs_it_for_every_broker():
    # finalize_and_create is the one path every parser and LLM_writes shares.
    assert "_with_household(" in inspect.getsource(pp.finalize_and_create)
    print("PASS finalize_and_create applies it")


if __name__ == "__main__":
    test_kap_wording_adds_the_canonical_line()
    test_every_wording_seen_on_tickets_is_read()
    test_an_omission_that_already_says_it_is_untouched()
    test_a_spaced_house_hold_is_respelled_not_doubled()
    test_other_counts_and_negations_add_nothing()
    test_an_empty_omission_gets_just_the_line()
    test_the_line_written_is_the_line_the_pattern_reads()
    test_finalize_runs_it_for_every_broker()
    print("ALL PASSED")
