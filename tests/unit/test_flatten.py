from __future__ import annotations
import json
import logging
from pathlib import Path
import polars as pl
from polars.testing import assert_frame_equal
from src.application.flatten import (
    flatten_events,
    validate_flat_columns,
    _join_report_path,
)
from src.domain.schema_registry import (
    DatasetSpec,
    EVENTS_FLAT_COLS,
    EVENTS_RAW_SCHEMA,
)


def test_join_report_path_uses_joinpath(monkeypatch, tmp_path):
    import src.application.flatten as fl

    monkeypatch.setattr(fl, "EXPECTATIONS_REPORTS_DIR", tmp_path)
    got = _join_report_path("ds", "file.json")
    assert got == str(tmp_path.joinpath("ds", "file.json"))


def test_validate_flat_columns_both_missing_and_new_strict_false(
    monkeypatch, tmp_path, caplog
):
    caplog.set_level(logging.ERROR)

    import src.application.flatten as fl

    monkeypatch.setattr(fl, "EXPECTATIONS_REPORTS_DIR", tmp_path)

    spec = DatasetSpec(
        name="mix",
        kind="events",
        raw_path=Path("mix.json"),
        raw_schema={"a": pl.Int64, "b": pl.Int64},
        flat_expected_cols=["a", "b"],
        allow_new_columns=True,
    )
    df_flat = pl.DataFrame({"a": [1], "c": [9]})
    (tmp_path / spec.name).mkdir(parents=True, exist_ok=True)

    ok, report_path = validate_flat_columns(spec, df_flat, strict=False)
    assert ok is True

    p = Path(report_path)
    assert p.exists()
    rp = json.loads(p.read_text())
    assert rp["missing_columns"] == ["b"]
    assert "c" in rp["new_columns"]

    rec = [r for r in caplog.records if r.levelno == logging.ERROR][-1]
    assert rec.msg.get("event") == "flat_schema_error"


def test_flatten_noop_when_not_events():
    spec = DatasetSpec(
        name="pays_like",
        kind="pays",
        raw_path=Path("ignored.csv"),
        raw_schema={"a": pl.Int64},
        flat_expected_cols=[],
        allow_new_columns=True,
    )
    df_raw = pl.DataFrame({"a": [1, 2, 3]})
    df_out = flatten_events(spec, df_raw)
    assert_frame_equal(df_out, df_raw, check_dtypes=False)


def test_validate_flat_columns_missing_strict_true(
    tmp_path, monkeypatch, caplog
):
    caplog.set_level(logging.ERROR)

    spec = DatasetSpec(
        name="prints",
        kind="events",
        raw_path=Path("prints.json"),
        raw_schema=EVENTS_RAW_SCHEMA,
        flat_expected_cols=["day", "position", "value_prop", "user_id"],
        allow_new_columns=True,
    )

    df_flat = pl.DataFrame(
        {"day": ["2020-11-01"], "value_prop": ["x"], "user_id": [1]}
    )

    monkeypatch.chdir(tmp_path)
    ok, report_path = validate_flat_columns(spec, df_flat, strict=True)
    assert ok is False

    p = Path(report_path)
    assert p.exists()
    rp = json.loads(p.read_text())
    assert rp["dataset"] == "prints"
    assert rp["ok"] is False
    assert rp["missing_columns"] == ["position"]

    rec = [r for r in caplog.records if r.levelno == logging.ERROR][-1]
    assert rec.msg.get("event") == "flat_schema_missing"


def test_validate_flat_columns_newcols_strict_false(
    tmp_path, monkeypatch, caplog
):
    caplog.set_level(logging.ERROR)

    spec = DatasetSpec(
        name="taps",
        kind="events",
        raw_path=Path("taps.json"),
        raw_schema=EVENTS_RAW_SCHEMA,
        flat_expected_cols=EVENTS_FLAT_COLS,
        allow_new_columns=True,
    )

    df_flat = pl.DataFrame(
        {
            "day": ["2020-11-01"],
            "position": [0],
            "value_prop": ["x"],
            "user_id": [1],
            "extra_col": [123],
        }
    )

    monkeypatch.chdir(tmp_path)
    ok, report_path = validate_flat_columns(spec, df_flat, strict=False)
    assert ok is True
    p = Path(report_path)
    assert p.exists()
    rp = json.loads(p.read_text())
    assert "extra_col" in rp["new_columns"]

    rec = [r for r in caplog.records if r.levelno == logging.ERROR][-1]
    assert rec.msg.get("event") == "flat_schema_new_cols"
