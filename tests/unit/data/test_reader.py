import json
import logging
import polars as pl

from src.adapters.reader import read_raw
from src.domain.schema_registry import DatasetSpec


def test_read_raw_pays_ok(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="src.adapters.reader")

    p = tmp_path / "pays.csv"
    p.write_text(
        "pay_date,total,user_id,value_prop\n"
        "2020-11-01,7.04,35994,link_cobro\n"
        "2020-11-01,37.36,79066,cellphone_recharge\n"
    )

    spec = DatasetSpec(
        name="pays",
        kind="pays",
        raw_path=str(p),
        raw_schema={
            "pay_date": pl.Utf8,
            "total": pl.Float64,
            "user_id": pl.Int64,
            "value_prop": pl.Utf8,
        },
        flat_expected_cols=["pay_date", "total", "user_id", "value_prop"],
    )

    df = read_raw(spec)
    assert df.shape == (2, 4)
    assert df.schema == {
        "pay_date": pl.Utf8,
        "total": pl.Float64,
        "user_id": pl.Int64,
        "value_prop": pl.Utf8,
    }

    assert caplog.records, "No se capturaron logs"
    rec = caplog.records[-1]
    assert rec.name == "src.adapters.reader"
    assert rec.levelno == logging.INFO
    assert isinstance(rec.msg, dict)
    assert rec.msg.get("event") == "read_raw_ok"
    assert rec.msg.get("dataset") == "pays"
    assert rec.msg.get("rows") == 2
    assert rec.msg.get("cols") == [
        "pay_date",
        "total",
        "user_id",
        "value_prop",
    ]


def test_read_raw_events_ok(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="src.adapters.reader")

    p = tmp_path / "events.ndjson"
    rows = [
        {
            "day": "2020-11-01",
            "position": 0,
            "value_prop": "point",
            "user_id": 123,
        },
        {
            "day": "2020-11-01",
            "position": 1,
            "value_prop": "prepaid",
            "user_id": 123,
        },
    ]
    p.write_text("\n".join(json.dumps(r) for r in rows))

    spec = DatasetSpec(
        name="prints",
        kind="events",
        raw_path=str(p),
        raw_schema={
            "day": pl.Utf8,
            "position": pl.Int64,
            "value_prop": pl.Utf8,
            "user_id": pl.Int64,
        },
        flat_expected_cols=["day", "position", "value_prop", "user_id"],
    )

    df = read_raw(spec)
    assert df.shape == (2, 4)
    assert df.schema == {
        "day": pl.Utf8,
        "position": pl.Int64,
        "value_prop": pl.Utf8,
        "user_id": pl.Int64,
    }

    assert caplog.records, "No se capturaron logs"
    rec = caplog.records[-1]
    assert rec.name == "src.adapters.reader"
    assert rec.levelno == logging.INFO
    assert isinstance(rec.msg, dict)
    assert rec.msg.get("event") == "read_raw_ok"
    assert rec.msg.get("dataset") == "prints"
    assert rec.msg.get("rows") == 2
    assert rec.msg.get("cols") == ["day", "position", "value_prop", "user_id"]


def test_read_raw_invalid_kind():
    spec = DatasetSpec(
        name="x",
        kind="other",
        raw_path="ignored",
        raw_schema={"a": pl.Int64},
        flat_expected_cols=["a"],
    )
    try:
        read_raw(spec)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert str(e) == "other"
