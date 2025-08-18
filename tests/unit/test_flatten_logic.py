from __future__ import annotations
import json
import logging
from pathlib import Path
import polars as pl
from polars.testing import assert_frame_equal
from src.application.flatten import flatten_events, validate_flat_columns


class DummySpec:
    def __init__(self, name: str, kind: str, expected):
        self.name = name
        self.kind = kind
        self.raw_path = "ignored"
        self.raw_schema: dict[str, pl.DataType] = {}
        self.flat_expected_cols = expected


def test_flatten_non_events_passthrough():
    spec = DummySpec("pays", "pays", ["pay_date", "total"])
    df_raw = pl.DataFrame({"pay_date": ["2020-11-01"], "total": [7.04]})
    df_flat = flatten_events(spec, df_raw)
    assert_frame_equal(df_flat, df_raw, check_dtypes=False)


def test_validate_flat_columns_non_events_returns_true_and_empty_report():
    spec = DummySpec("pays", "pays", ["pay_date", "total"])
    df_flat = pl.DataFrame({"pay_date": ["2020-11-01"], "total": [7.04]})
    ok, report_path = validate_flat_columns(spec, df_flat, strict=True)
    assert ok is True
    assert report_path == ""


def test_flatten_events_keep_empty_returns_original_event_cols():
    spec = DummySpec("weird_events", "events", ["not_there"])
    df_raw = pl.DataFrame({"event_data": [{"x": 1}], "user_id": [1]})
    df_flat = flatten_events(spec, df_raw)
    assert set(df_flat.columns) == {"x", "user_id"}


def test_validate_flat_columns_missing_strict_true(
    tmp_path, caplog, monkeypatch
):
    caplog.set_level(logging.ERROR)
    spec = DummySpec("prints", "events", ["day", "position"])
    monkeypatch.chdir(tmp_path)
    df_flat = pl.DataFrame({"day": ["2020-11-01"]})
    ok, report_path = validate_flat_columns(spec, df_flat, strict=True)
    assert ok is False
    p = Path(report_path)
    assert p.exists()
    report = json.loads(p.read_text())
    assert report["dataset"] == "prints"
    assert report["ok"] is False
    assert report["missing_columns"] == ["position"]
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors and errors[-1].msg.get("event") == "flat_schema_missing"


def test_validate_flat_columns_missing_strict_false(
    tmp_path, caplog, monkeypatch
):
    caplog.set_level(logging.INFO)
    spec = DummySpec("taps", "events", ["day", "position"])
    monkeypatch.chdir(tmp_path)
    df_flat = pl.DataFrame({"day": ["2020-11-01"]})
    ok, report_path = validate_flat_columns(spec, df_flat, strict=False)
    assert ok is True
    p = Path(report_path)
    assert p.exists()
    report = json.loads(p.read_text())
    assert report["ok"] is False
    assert report["missing_columns"] == ["position"]
    infos = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any(r.msg.get("event") == "flat_schema_ok" for r in infos)
