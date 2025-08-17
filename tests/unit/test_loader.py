import logging
import polars as pl
import pytest

import src.application.dq_and_load as loader


class DummySpec:
    def __init__(self, name: str):
        self.name = name
        self.kind = "events"
        self.raw_path = "dummy"
        self.raw_schema = {"a": pl.Int64}
        self.flat_expected_cols = ["a"]


def make_df(rows=1):
    return pl.DataFrame({"a": list(range(rows))})


def test_load_and_prepare_all_ok(monkeypatch, caplog):
    spec = DummySpec("dummy")
    monkeypatch.setattr(loader, "DATASETS", {"dummy": spec}, raising=True)

    monkeypatch.setattr(
        loader, "validate_raw_schema", lambda s, df, strict: (True, "ok")
    )
    monkeypatch.setattr(loader, "read_raw", lambda s: make_df(2))
    monkeypatch.setattr(loader, "flatten_events", lambda s, df: df)
    monkeypatch.setattr(
        loader, "validate_flat_columns", lambda s, df, strict: (True, "ok")
    )

    caplog.set_level(logging.INFO, logger="src.application.dq_and_load")

    result = loader.load_and_prepare_all()
    assert "dummy" in result
    df = result["dummy"]
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, 1)

    assert caplog.records, "No se capturaron logs"
    rec = caplog.records[-1]
    assert rec.name == "src.application.dq_and_load"
    assert rec.levelno == logging.INFO
    assert isinstance(rec.msg, dict)
    assert rec.msg.get("event") == "dataset_ready"
    assert rec.msg.get("dataset") == "dummy"
    assert rec.msg.get("rows") == 2
    assert rec.msg.get("cols") == ["a"]


def test_load_and_prepare_all_raw_schema_fail(monkeypatch):
    spec = DummySpec("bad")
    monkeypatch.setattr(loader, "DATASETS", {"bad": spec}, raising=True)

    monkeypatch.setattr(
        loader, "validate_raw_schema", lambda *a, **k: (False, "bad schema")
    )

    with pytest.raises(AssertionError) as e:
        loader.load_and_prepare_all()
    assert "RAW schema failed for bad" in str(e.value)
    assert "bad schema" in str(e.value)


def test_load_and_prepare_all_flat_schema_fail(monkeypatch):
    spec = DummySpec("badflat")
    monkeypatch.setattr(loader, "DATASETS", {"badflat": spec}, raising=True)

    monkeypatch.setattr(
        loader, "validate_raw_schema", lambda *a, **k: (True, "ok")
    )
    monkeypatch.setattr(loader, "read_raw", lambda s: make_df(1))
    monkeypatch.setattr(loader, "flatten_events", lambda s, df: df)
    monkeypatch.setattr(
        loader, "validate_flat_columns", lambda *a, **k: (False, "flat fail")
    )

    with pytest.raises(AssertionError) as e:
        loader.load_and_prepare_all()
    assert "FLAT schema failed for badflat" in str(e.value)
    assert "flat fail" in str(e.value)
