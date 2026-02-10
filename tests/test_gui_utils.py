from pathlib import Path

from agentlab_gui.utils import (
    load_jsonl,
    parse_key_value_lines,
    parse_run_identity,
)


def test_parse_key_value_lines() -> None:
    out = parse_key_value_lines("run_id: run_1\nrun_dir: /tmp/run\nnoise\n")
    assert out["run_id"] == "run_1"
    assert out["run_dir"] == "/tmp/run"


def test_parse_run_identity() -> None:
    run_id, run_dir = parse_run_identity("run_id: run_abc\nrun_dir: /x/y\n")
    assert run_id == "run_abc"
    assert run_dir == "/x/y"


def test_load_jsonl_ignores_bad_lines(tmp_path: Path) -> None:
    p = tmp_path / "x.jsonl"
    p.write_text('{"a":1}\nnot_json\n{"b":2}\n', encoding="utf-8")
    rows = load_jsonl(p)
    assert rows == [{"a": 1}, {"b": 2}]

