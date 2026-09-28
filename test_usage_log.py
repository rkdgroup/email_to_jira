"""The API-usage ledger, and the one cache breakpoint QC now sends.

Offline: no key, no network. `usage_log.py` is a deliberate copy of the one in
LRF_Processing/order-processor -- keep the row shape identical so one reader
can total both repos.
"""
import json
import sys
from types import SimpleNamespace

import pytest

import qc_checker
import usage_log


class _Resp:
    def __init__(self, text='{"verdict":"PASS","findings":[]}', stop_reason="end_turn"):
        self.content = [SimpleNamespace(type="text", text=text)]
        self.model = "claude-opus-5"
        self.stop_reason = stop_reason
        self.usage = SimpleNamespace(input_tokens=3000, output_tokens=200,
                                     cache_creation_input_tokens=0,
                                     cache_read_input_tokens=2800)


@pytest.fixture(autouse=True)
def _tmp_log(tmp_path, monkeypatch):
    monkeypatch.setattr(usage_log, "LOG_PATH", tmp_path / "api_usage.jsonl")
    # setenv, not delenv: teardown then also undoes check_ticket's os.environ write.
    monkeypatch.setenv("DSLF_TICKET", "")


def test_row_carries_all_four_token_meters(monkeypatch):
    monkeypatch.setenv("DSLF_TICKET", "DSLF-9")
    row = usage_log.row_for("qc_checker.order", _Resp(), "form.pdf")
    assert row["input"] == 3000 and row["output"] == 200
    assert row["cache_read"] == 2800 and row["cache_write"] == 0
    assert row["module"] == "qc_checker.order"
    assert row["ticket"] == "DSLF-9" and row["ref"] == "form.pdf"


def test_one_hour_cache_writes_are_split_out_and_priced_at_twice_base():
    resp = _Resp()
    resp.usage.cache_creation_input_tokens = 1_000_000
    resp.usage.cache_creation = SimpleNamespace(ephemeral_1h_input_tokens=1_000_000,
                                                ephemeral_5m_input_tokens=0)
    row = usage_log.row_for("m", resp)
    assert row["cache_write_1h"] == 1_000_000
    assert usage_log.cost_of({**row, "input": 0, "cache_read": 0, "output": 0})         == pytest.approx(10.0)


def test_check_ticket_tags_its_usage_rows_with_the_ticket(monkeypatch):
    import tools_jira
    monkeypatch.setattr(tools_jira, "get_ticket_qc_fields", lambda k: {"error": "offline"})
    qc_checker.check_ticket("DSLF-77")
    assert usage_log.row_for("m", _Resp())["ticket"] == "DSLF-77"


def test_truncation_is_countable():
    assert usage_log.row_for("m", _Resp(stop_reason="max_tokens"))["stop_reason"] == "max_tokens"


def test_cost_of_prices_each_token_kind():
    base = {"model": "claude-opus-5", "input": 0, "cache_write": 0,
            "cache_read": 0, "output": 0}
    assert usage_log.cost_of({**base, "input": 1_000_000}) == pytest.approx(5.0)
    assert usage_log.cost_of({**base, "output": 1_000_000}) == pytest.approx(25.0)
    assert usage_log.cost_of({**base, "cache_read": 1_000_000}) == pytest.approx(0.5)
    assert usage_log.cost_of({**base, "model": "unknown", "input": 1_000_000}) == 0.0


def test_record_never_raises_on_a_bad_path(monkeypatch, tmp_path):
    monkeypatch.setattr(usage_log, "LOG_PATH", tmp_path / "no\x00pe" / "x.jsonl")
    usage_log.record("m", _Resp())


def test_summarise_reads_back_what_record_wrote():
    usage_log.record("a", _Resp())
    usage_log.record("b", _Resp())
    s = usage_log.summarise(usage_log.LOG_PATH)
    assert s["calls"] == 2 and set(s["by_module"]) == {"a", "b"}


def _fake_anthropic(calls):
    class _Client:
        def __init__(self, timeout=None, **kw):
            pass

        class messages:      # noqa: N801 - mirrors the SDK attribute name
            @staticmethod
            def create(**kw):
                calls.append(kw)
                return _Resp()

    return SimpleNamespace(Anthropic=_Client)


def test_qc_sends_its_system_prompt_as_one_cached_block(monkeypatch, tmp_path):
    """The prompt text is unchanged; only its cacheability is."""
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    monkeypatch.setitem(sys.modules, "anthropic", _fake_anthropic(calls := []))
    monkeypatch.setattr(qc_checker, "_cache", {})

    qc_checker._review(str(pdf), "SYSTEM TEXT", {"type": "object"}, "ask", "order")

    sent = calls[0]["system"]
    assert sent == [{"type": "text", "text": "SYSTEM TEXT",
                     "cache_control": {"type": "ephemeral"}}]


def test_the_pdf_is_the_cached_prefix_and_the_ticket_text_the_suffix(monkeypatch, tmp_path):
    """_run_precheck sends one PDF twice with different ticket text, so the
    breakpoint belongs on the document -- never after the varying text."""
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    monkeypatch.setitem(sys.modules, "anthropic", _fake_anthropic(calls := []))
    monkeypatch.setattr(qc_checker, "_cache", {})

    qc_checker._review(str(pdf), "SYSTEM TEXT", {"type": "object"}, "ask", "order")

    content = calls[0]["messages"][0]["content"]
    assert content[0]["type"] == "document"
    assert content[0]["cache_control"] == {"type": "ephemeral"}
    assert content[-1]["type"] == "text" and "cache_control" not in content[-1]


def test_qc_writes_a_usage_row(monkeypatch, tmp_path):
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    monkeypatch.setitem(sys.modules, "anthropic", _fake_anthropic([]))
    monkeypatch.setattr(qc_checker, "_cache", {})

    qc_checker._review(str(pdf), "SYSTEM TEXT", {"type": "object"}, "ask", "select")

    rows = [json.loads(ln) for ln in
            usage_log.LOG_PATH.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["module"] == "qc_checker.select"
    assert rows[0]["cache_read"] == 2800
