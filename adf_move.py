"""Move one misfiled line from the Description into the Omission Description.

The ONE sanctioned edit to either ADF field. Everything else about them stays
reported-never-written: they are prose the parsers build, and a field-level
overwrite flattens the bullet structure `_build_adf_description` creates. This
module never rewrites a field wholesale -- it drops exactly one matching node and
appends exactly one paragraph, leaving every other node untouched.

The case it exists for is DSLF-1274: the order's `STATE OMIT` was left as a bullet
under `Selects:` in the Description instead of being filed under Omission, the
pre-check called it WRONG, and nothing could correct it, so the ticket stranded.

Pure and network-free: the caller already holds both ADF values (`ticket_fields`
carries `description_adf` and `omission_adf`).
"""
from __future__ import annotations

from typing import NamedTuple


class MovePlan(NamedTuple):
    """`ok` False means do not write; `reason` says why, for the QC comment."""
    ok: bool
    reason: str
    description: dict | None
    omission: dict | None


# Rendered list markers. A bulletList item's ADF text is "STATE OMIT"; what the
# reader (and the model quoting it) sees is "* STATE OMIT". Stripped from BOTH
# sides of every comparison, so the glyph never decides a match either way.
_MARKERS = "•·◦‣⁃−–—-*  	"


def _key(text: str) -> str:
    """Normalized for matching -- case, whitespace and list markers never decide."""
    return " ".join(str(text or "").lstrip(_MARKERS).split()).casefold()


def node_text(node) -> str:
    """All text under an ADF node, concatenated in document order."""
    if isinstance(node, dict):
        if node.get("type") == "text":
            return str(node.get("text") or "")
        return "".join(node_text(c) for c in node.get("content") or ())
    if isinstance(node, list):
        return "".join(node_text(c) for c in node)
    return ""


def _paragraph(text: str) -> dict:
    return {"type": "paragraph", "content": [{"type": "text", "text": text}]}


def _empty_doc() -> dict:
    return {"type": "doc", "version": 1, "content": []}


def lines(adf) -> list[str]:
    """Every line an ADF doc renders, paragraphs and list items alike."""
    out: list[str] = []
    for node in (adf or {}).get("content") or ():
        kind = node.get("type")
        if kind in ("bulletList", "orderedList"):
            out.extend(node_text(item) for item in node.get("content") or ())
        else:
            out.append(node_text(node))
    return [ln for ln in (s.strip() for s in out) if ln]


def remove_line(adf, line: str) -> tuple[dict, int]:
    """(doc without `line`, how many nodes were dropped). Never rewrites the rest.

    A list that loses its last item is dropped with it -- an empty bulletList
    renders as a stray bullet.
    """
    want, content = _key(line), []
    dropped = 0
    for node in (adf or {}).get("content") or ():
        if node.get("type") in ("bulletList", "orderedList"):
            kept = [i for i in node.get("content") or () if _key(node_text(i)) != want]
            dropped += len(node.get("content") or ()) - len(kept)
            if kept:
                content.append({**node, "content": kept})
            continue
        if _key(node_text(node)) == want:
            dropped += 1
            continue
        content.append(node)
    return {**(adf or _empty_doc()), "type": "doc", "version": 1,
            "content": content}, dropped


def append_line(adf, line: str) -> dict:
    """`line` as one more paragraph at the end. Matches tools_jira's omission shape."""
    doc = adf if isinstance(adf, dict) and adf.get("content") is not None else _empty_doc()
    return {**doc, "type": "doc", "version": 1,
            "content": list(doc.get("content") or ()) + [_paragraph(line.strip())]}


def plan_move(description, omission, line: str) -> MovePlan:
    """Plan the move, or refuse it. Pure -- nothing is written here.

    Refuses rather than guessing, because a half-applied move is worse than the
    refusal it replaces: drop the line from the Description without filing it and
    the order silently loses an omit, which is the DSLF-1101 shape.
    """
    text = " ".join(str(line or "").split())
    if not text:
        return MovePlan(False, "no line to move was quoted", None, None)

    hits = [ln for ln in lines(description) if _key(ln) == _key(text)]
    if not hits:
        return MovePlan(False, f"{text!r} is not a line in the Description", None, None)
    if len(hits) > 1:
        return MovePlan(False, f"{text!r} appears {len(hits)}x in the Description — "
                               f"ambiguous, moved by hand", None, None)
    if any(_key(ln) == _key(text) for ln in lines(omission)):
        return MovePlan(False, f"{text!r} is already in the Omission Description",
                        None, None)

    new_desc, dropped = remove_line(description, text)
    if dropped != 1:
        return MovePlan(False, f"removing {text!r} touched {dropped} nodes, expected 1",
                        None, None)
    # Keep the original casing from the Description, not the model's paraphrase.
    return MovePlan(True, f"moved {hits[0]!r} to the Omission Description",
                    new_desc, append_line(omission, hits[0]))
