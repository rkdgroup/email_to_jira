"""Offline tests for the one sanctioned Description -> Omission edit. No Jira."""
import adf_move as m


def _bullets(*items):
    return {"type": "bulletList",
            "content": [{"type": "listItem",
                         "content": [{"type": "paragraph",
                                      "content": [{"type": "text", "text": t}]}]}
                        for t in items]}


def _para(text):
    return {"type": "paragraph", "content": [{"type": "text", "text": text}]}


def _doc(*nodes):
    return {"type": "doc", "version": 1, "content": list(nodes)}


# DSLF-1274 as it actually reads on the ticket.
_DESC = _doc(_para("3M $5+ W/GEO"), _para("Selects:"),
             _bullets("3 MONTH HOTLINE", "STATE OMIT"))
_OMIT = _doc(_para("STANDARD OMITS (F6)"))


def test_the_misfiled_bullet_moves_and_nothing_else_changes():
    plan = m.plan_move(_DESC, _OMIT, "STATE OMIT")

    assert plan.ok
    assert m.lines(plan.description) == ["3M $5+ W/GEO", "Selects:", "3 MONTH HOTLINE"]
    assert m.lines(plan.omission) == ["STANDARD OMITS (F6)", "STATE OMIT"]


def test_the_surviving_bullet_keeps_its_list_structure():
    """A whole-field rewrite is what the exclusion comment warns against."""
    plan = m.plan_move(_DESC, _OMIT, "STATE OMIT")
    kinds = [n["type"] for n in plan.description["content"]]

    assert kinds == ["paragraph", "paragraph", "bulletList"]
    assert len(plan.description["content"][2]["content"]) == 1


def test_a_list_that_loses_its_last_item_is_dropped_with_it():
    """An empty bulletList renders as a stray bullet."""
    doc = _doc(_para("head"), _bullets("only one"))

    out, dropped = m.remove_line(doc, "only one")

    assert dropped == 1 and [n["type"] for n in out["content"]] == ["paragraph"]


def test_a_plain_paragraph_moves_too_not_just_a_bullet():
    plan = m.plan_move(_doc(_para("keep me"), _para("STATE OMIT")), _OMIT, "STATE OMIT")

    assert plan.ok and m.lines(plan.description) == ["keep me"]


def test_matching_ignores_case_and_spacing_but_keeps_the_ticket_wording():
    plan = m.plan_move(_DESC, _OMIT, "  state   omit ")

    assert plan.ok
    assert m.lines(plan.omission)[-1] == "STATE OMIT"  # the Description's casing


def test_a_line_that_is_not_in_the_description_is_refused():
    plan = m.plan_move(_DESC, _OMIT, "NCOA OMIT")

    assert not plan.ok and "not a line in the Description" in plan.reason
    assert plan.description is None and plan.omission is None


def test_an_ambiguous_line_is_refused_rather_than_guessed():
    doc = _doc(_para("STATE OMIT"), _bullets("STATE OMIT"))

    plan = m.plan_move(doc, _OMIT, "STATE OMIT")

    assert not plan.ok and "appears 2x" in plan.reason


def test_a_line_already_filed_is_refused_so_it_is_not_duplicated():
    plan = m.plan_move(_DESC, _doc(_para("STATE OMIT")), "STATE OMIT")

    assert not plan.ok and "already in the Omission" in plan.reason


def test_an_empty_quote_is_refused():
    assert not m.plan_move(_DESC, _OMIT, "   ").ok
    assert not m.plan_move(_DESC, _OMIT, None).ok


def test_an_empty_omission_field_still_receives_the_line():
    for empty in (None, {}, _doc()):
        plan = m.plan_move(_DESC, empty, "STATE OMIT")
        assert plan.ok and m.lines(plan.omission) == ["STATE OMIT"]
        assert plan.omission["type"] == "doc" and plan.omission["version"] == 1


def test_the_written_docs_are_shaped_the_way_jira_expects():
    """update_ticket_fields PUTs whatever it is handed; malformed ADF looks like
    it worked and silently changes nothing."""
    plan = m.plan_move(_DESC, _OMIT, "STATE OMIT")

    for doc in (plan.description, plan.omission):
        assert doc["type"] == "doc" and doc["version"] == 1
        assert isinstance(doc["content"], list)
    assert plan.omission["content"][-1] == _para("STATE OMIT")


def test_a_quoted_bullet_glyph_does_not_defeat_the_match():
    """The model quotes what it SEES; a bulletList item renders with a marker the
    ADF text does not contain. DSLF-1274 was skipped over exactly this."""
    for quoted in ("• STATE OMIT", "- STATE OMIT", "* STATE OMIT",
                   "– STATE OMIT", "·  STATE OMIT"):
        plan = m.plan_move(_DESC, _OMIT, quoted)
        assert plan.ok, quoted
        assert m.lines(plan.omission)[-1] == "STATE OMIT"  # stored without the marker


def test_a_marker_in_the_stored_line_is_normalized_too():
    """Symmetric: the marker never decides a match from either side."""
    doc = _doc(_para("keep"), _para("- STATE OMIT"))

    assert m.plan_move(doc, _OMIT, "STATE OMIT").ok


def test_lines_differing_only_by_a_marker_are_ambiguous_not_a_coin_flip():
    doc = _doc(_para("STATE OMIT"), _bullets("STATE OMIT"))

    assert "appears 2x" in m.plan_move(doc, _OMIT, "• STATE OMIT").reason
