"""
Dollar-cap placement tests. No network, no Jira, no PDFs.

    python test_dollar_cap_backfill.py      # standalone, prints PASS / ALL PASSED
    pytest test_dollar_cap_backfill.py      # also works

insert_cap() edits the Description of live tickets, so its two dangerous behaviours are
pinned here: it must never add a second cap line, and it must never disturb the rest of the
document — the bullet lists carrying the priced selects and the profile's suppressions are
real ADF structure, and flattening them makes whole sections render blank in Jira.

Placement has to agree with what parse_pipeline._build_adf_description emits on a fresh
ticket (cap immediately after "Select By:"), or a backfilled ticket and a new one read
differently for no reason. The last test asserts that agreement directly rather than
trusting the two to stay in step.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from backfill_dollar_cap import insert_cap, CAP_LABEL

_failures = []


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


def _p(text):
    return {"type": "paragraph", "content": [{"type": "text", "text": text}]}


def _doc(*nodes):
    return {"type": "doc", "version": 1, "content": list(nodes)}


_BULLETS = {"type": "bulletList", "content": [
    {"type": "listItem", "content": [_p("SUPPRESS DECEASED")]}]}


def _lines(adf):
    """Paragraph text in order; non-paragraph nodes as <type> so structure is visible."""
    out = []
    for n in adf.get("content", []):
        if n.get("type") == "paragraph":
            out.append("".join(c.get("text", "") for c in n.get("content") or []))
        else:
            out.append(f"<{n['type']}>")
    return out


def test_cap_lands_after_select_by():
    r = insert_cap(_doc(_p("12 MONTH $10+"), _p("Select By: DONORS"),
                        _p("Standard Suppressions:"), _BULLETS), "$99.99")
    check("cap sits directly after Select By",
          _lines(r), ["12 MONTH $10+", "Select By: DONORS", "Dollar Cap: $99.99",
                      "Standard Suppressions:", "<bulletList>"])


def test_cap_lands_before_the_profile_block_when_there_is_no_select_by():
    r = insert_cap(_doc(_p("12 MONTH $10+"), _p("Standard Suppressions:"), _BULLETS), "$49.99")
    check("cap precedes Standard Suppressions",
          _lines(r), ["12 MONTH $10+", "Dollar Cap: $49.99",
                      "Standard Suppressions:", "<bulletList>"])


def test_cap_appends_when_there_is_nothing_to_anchor_to():
    r = insert_cap(_doc(_p("12 MONTH $10+")), "NO CAP")
    check("cap appended at the end", _lines(r), ["12 MONTH $10+", "Dollar Cap: NO CAP"])


def test_special_instructions_also_anchors():
    r = insert_cap(_doc(_p("$5+"), _p("Special Instructions:"), _BULLETS), "$249.99")
    check("cap precedes Special Instructions",
          _lines(r)[1], "Dollar Cap: $249.99")


def test_an_existing_cap_is_never_duplicated():
    """Re-running the backfill must be harmless — it is a one-off, run by hand."""
    d = _doc(_p("Select By: X"), _p("Dollar Cap: $49.99"))
    check("second run makes no change", insert_cap(d, "$99.99"), None)
    check("case and spacing do not fool the check",
          insert_cap(_doc(_p("  dollar cap:  $49.99  ")), "$99.99"), None)


def test_non_document_input_is_left_alone():
    for bad in ("plain string", None, {}, {"type": "paragraph"}, []):
        check(f"refuses {type(bad).__name__} {bad!r}", insert_cap(bad, "$99.99"), None)


def test_bullet_lists_survive_untouched():
    """A flattened bulletList renders as blank in Jira — see jira_adf_field_inspection."""
    src = _doc(_p("SELECTS:"), _BULLETS, _p("Select By: DONORS"))
    r = insert_cap(src, "$99.99")
    check("bullet node passed through by identity",
          r["content"][1] is _BULLETS, True)
    check("original document not mutated", len(src["content"]), 3)


def test_uncapped_values_are_written_verbatim():
    """"NO CAP" and "VARIES PER ORDER" mean different things — neither is normalised."""
    for cap in ("NO CAP", "VARIES PER ORDER", "No transaction $ available",
                "CAP AT 99.99 UNLESS ORDER STATES OTHERWISE"):
        r = insert_cap(_doc(_p("$10+")), cap)
        check(f"{cap!r} written as recorded", _lines(r)[-1], f"{CAP_LABEL} {cap}")


def test_placement_matches_a_freshly_created_ticket():
    """Backfilled and new tickets must read the same, or the difference is noise."""
    from parse_pipeline import _build_adf_description
    from parse_result import ParseResult

    result = ParseResult(source="t", confidence=0.92, segment_criteria="12 MONTH $10+")
    profile = {"select_by": "DONORS", "dollar_cap": "$99.99",
               "standard_suppressions": ["SUPPRESS DECEASED"]}

    fresh = _lines(_build_adf_description(result, profile_data=profile))
    # The same ticket as the pipeline would have built it before the cap line existed.
    without = {k: v for k, v in profile.items() if k != "dollar_cap"}
    old     = _build_adf_description(result, profile_data=without)
    patched = _lines(insert_cap(old, "$99.99"))

    check("backfill reproduces the fresh layout exactly", patched, fresh)


def main():
    for fn in sorted(
        (v for k, v in globals().items() if k.startswith("test_") and callable(v)),
        key=lambda f: f.__code__.co_firstlineno,
    ):
        fn()
    print()
    if _failures:
        print("FAILURES:")
        for f in _failures:
            print("  - " + f)
        return 1
    print("ALL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
