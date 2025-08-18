import json
from pathlib import Path
from datetime import datetime
import polars as pl
import src.application.validation as val
from src.domain.schema_registry import DatasetSpec


def _cleanup_report(rp: str):
    p = Path(rp)
    if p.exists():
        p.unlink()
    for parent in p.parents:
        try:
            if parent.name == "expectations":
                break
            parent.rmdir()
        except OSError:
            break


def test_is_int_like_cases():
    assert val._is_int_like(None)
    assert val._is_int_like(5)
    assert val._is_int_like("10")
    assert not val._is_int_like("abc")
    assert val._is_int_like("  +42 ")
    assert val._is_int_like("-7")


def test_is_float_like_cases():
    assert val._is_float_like(None)
    assert val._is_float_like(3.14)
    assert val._is_float_like("2.5")
    assert val._is_float_like("1e5")
    assert not val._is_float_like("xx")
    assert val._is_float_like(".5")
    assert val._is_float_like("5.")
    assert val._is_float_like("1e-3")


def test_is_bool_like_cases():
    assert val._is_bool_like(None)
    assert val._is_bool_like(True)
    assert val._is_bool_like("true")
    assert val._is_bool_like("No")
    assert val._is_bool_like(1)
    assert not val._is_bool_like("maybe")
    for s in ["T", "F", "YES", "no"]:
        assert val._is_bool_like(s)


def test_is_date_like_cases():
    assert val._is_date_like(None)
    assert val._is_date_like(datetime.now())
    assert val._is_date_like("2020-01-01")
    assert val._is_date_like("2020-01-01T12:00:00Z")
    assert val._is_date_like("2020-01-01T00:00:00+00:00")
    assert not val._is_date_like("notadate")


def test_invalid_token_counts_csv_and_ndjson(tmp_path):
    csv = tmp_path / "f.csv"
    csv.write_text("a,b\nx,1\n2,y\n")
    out = val._invalid_token_counts_csv(csv, {"a": pl.Int64, "b": pl.Float64})
    assert {"a", "b"} <= set(out)

    nd = tmp_path / "f.json"
    nd.write_text('{"a":"x"}\n{"a":1}\n')
    out2 = val._invalid_token_counts_ndjson(nd, {"a": pl.Int64})
    assert "a" in out2


def test_invalid_token_counts_csv_boolean_and_unknown_dtype(tmp_path):
    p = tmp_path / "b.csv"
    p.write_text("b,s\nmaybe,foo\ntrue,bar\n")
    out = val._invalid_token_counts_csv(
        p, {"b": pl.Boolean, "s": pl.List(pl.Int64)}
    )
    assert out["b"] >= 1 and "s" in out


def test_invalid_token_counts_ndjson_skips_struct(tmp_path):
    p = tmp_path / "e.json"
    p.write_text(
        '{"a":"x","event_data":{"k":1}}\n{"a":2,"event_data":{"k":2}}\n'
    )
    out = val._invalid_token_counts_ndjson(
        p, {"a": pl.Int64, "event_data": pl.Struct}
    )
    assert "a" in out and "event_data" not in out


def test_ndjson_keys_and_rowcount(tmp_path):
    p = tmp_path / "a.json"
    p.write_text('{"a":1}\n{}\nnotjson\n')
    keys, n = val._ndjson_keys_and_rowcount(p)
    assert "a" in keys and n == 3


def test_validate_raw_schema_csv_newcols_and_missing(tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text("colX\nabc\n")
    spec = type(
        "S",
        (),
        {
            "name": "bad",
            "kind": "pays",
            "raw_path": str(csv),
            "raw_schema": {"colY": pl.Int64},
            "allow_new_columns": False,
        },
    )()
    ok, rp = val.validate_raw_schema(spec, strict=True)
    assert not ok
    data = json.loads(Path(rp).read_text())
    assert "new_columns" in data or "missing_columns" in data
    _cleanup_report(rp)


def test_validate_raw_schema_ndjson_with_wrong_types(tmp_path):
    p = tmp_path / "b.json"
    p.write_text('{"x":"abc"}\n')
    spec = type(
        "S",
        (),
        {
            "name": "bbb",
            "kind": "events",
            "raw_path": str(p),
            "raw_schema": {"x": pl.Int64},
            "allow_new_columns": True,
        },
    )()
    ok, rp = val.validate_raw_schema(spec, strict=True)
    assert not ok
    d = json.loads(Path(rp).read_text())
    assert d["stage"] == "raw" and d["dataset"] == "bbb"
    _cleanup_report(rp)


def test_validate_raw_schema_handles_read_error(tmp_path):
    spec = type(
        "S",
        (),
        {
            "name": "err",
            "kind": "pays",
            "raw_path": str(tmp_path / "nofile.csv"),
            "raw_schema": {"a": pl.Int64},
            "allow_new_columns": False,
        },
    )()
    ok, rp = val.validate_raw_schema(spec, strict=True)
    assert not ok
    data = json.loads(Path(rp).read_text())
    assert data["ok"] is False and "read_error" in data
    _cleanup_report(rp)


def test_validate_raw_schema_new_columns_allowed(tmp_path):
    csv = tmp_path / "ok.csv"
    csv.write_text("colA\n1\n")
    spec = type(
        "S",
        (),
        {
            "name": "okcols",
            "kind": "pays",
            "raw_path": str(csv),
            "raw_schema": {"colA": pl.Int64},
            "allow_new_columns": True,
        },
    )()
    ok, rp = val.validate_raw_schema(spec, strict=True)
    assert ok
    _cleanup_report(rp)


def test_validate_raw_schema_strict_false_with_missing(tmp_path):
    p = tmp_path / "m.csv"
    p.write_text("colA\n1\n")
    spec = type(
        "S",
        (),
        {
            "name": "miss",
            "kind": "pays",
            "raw_path": str(p),
            "raw_schema": {"colA": pl.Int64, "colB": pl.Int64},
            "allow_new_columns": True,
        },
    )()
    ok, rp = val.validate_raw_schema(spec, strict=False)
    data = json.loads(Path(rp).read_text())
    assert ok and data["ok"] is False and "colB" in data["missing_columns"]
    _cleanup_report(rp)


def test_validate_raw_schema_strict_false_with_newcols_not_allowed(tmp_path):
    p = tmp_path / "n.csv"
    p.write_text("colA,colX\n1,9\n")
    spec = type(
        "S",
        (),
        {
            "name": "newdisallowed",
            "kind": "pays",
            "raw_path": str(p),
            "raw_schema": {"colA": pl.Int64},
            "allow_new_columns": False,
        },
    )()
    ok, rp = val.validate_raw_schema(spec, strict=False)
    data = json.loads(Path(rp).read_text())
    assert ok and "colX" in data["new_columns"] and data["ok"] is False
    _cleanup_report(rp)


def test_validate_raw_schema_wrong_types_and_newcols_combo(tmp_path):
    p = tmp_path / "w.csv"
    p.write_text("x,y\nabc,1\n")
    spec = type(
        "S",
        (),
        {
            "name": "combo",
            "kind": "pays",
            "raw_path": str(p),
            "raw_schema": {"x": pl.Int64},
            "allow_new_columns": False,
        },
    )()
    ok, rp = val.validate_raw_schema(spec, strict=False)
    data = json.loads(Path(rp).read_text())
    assert ok and "y" in data["new_columns"]
    assert any(w["column"] == "x" for w in data.get("wrong_types", []))
    _cleanup_report(rp)


def test_is_int_like_non_numeric_types():
    assert not val._is_int_like([])
    assert not val._is_int_like({})


def test_is_float_like_non_numeric_types():
    class X:
        pass

    assert not val._is_float_like(X())


def test_resolve_temporal_dtype():
    assert val._resolve_temporal_dtype(pl.Date) is pl.Date
    assert val._resolve_temporal_dtype(pl.Datetime) is pl.Datetime


def test_invalid_token_counts_csv_dates(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text("d,dt\n2020-01-01,2020-01-01T00:00:00\nbad,also-bad\n")
    out = val._invalid_token_counts_csv(p, {"d": pl.Date, "dt": pl.Datetime})
    assert out["d"] >= 1 and out["dt"] >= 1


def test_invalid_token_counts_csv_no_overlap_returns_empty(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("a\n1\n")
    out = val._invalid_token_counts_csv(p, {"z": pl.Int64})
    assert out == {}


def test_invalid_token_counts_ndjson_mixed_types(tmp_path):
    p = tmp_path / "m.json"
    p.write_text(
        "\n".join(
            [
                '{"b":"true","f":"3.14","d":"2020-01-01"}',
                '{"b":"maybe","f":"xx","d":"notadate"}',
            ]
        )
    )
    out = val._invalid_token_counts_ndjson(
        p, {"b": pl.Boolean, "f": pl.Float64, "d": pl.Date}
    )
    assert out["b"] == 1 and out["f"] == 1 and out["d"] == 1


def test_validate_flat_columns_ok(tmp_path, monkeypatch):
    spec = DatasetSpec(
        name="ev_ok",
        kind="events",
        raw_path=Path("ignored"),
        raw_schema={},
        flat_expected_cols=["a", "b"],
        allow_new_columns=True,
    )
    df = pl.DataFrame({"a": [1], "b": [2]})
    monkeypatch.chdir(tmp_path)
    ok, rp = val.validate_flat_columns(spec, df, strict=True)
    assert ok is True
    assert Path(rp).exists()
    _cleanup_report(rp)


def test_validate_flat_columns_missing_strict_true(tmp_path, monkeypatch):
    spec = DatasetSpec(
        name="ev_miss",
        kind="events",
        raw_path=Path("ignored"),
        raw_schema={},
        flat_expected_cols=["a", "b"],
        allow_new_columns=True,
    )
    df = pl.DataFrame({"a": [1]})
    monkeypatch.chdir(tmp_path)
    ok, rp = val.validate_flat_columns(spec, df, strict=True)
    assert ok is False
    data = json.loads(Path(rp).read_text())
    assert data["ok"] is False and "b" in data["missing_columns"]
    _cleanup_report(rp)


def test_validate_flat_columns_newcols_strict_false(tmp_path, monkeypatch):
    spec = DatasetSpec(
        name="ev_new",
        kind="events",
        raw_path=Path("ignored"),
        raw_schema={},
        flat_expected_cols=["a", "b"],
        allow_new_columns=True,
    )
    df = pl.DataFrame({"a": [1], "b": [2], "extra": [9]})
    monkeypatch.chdir(tmp_path)
    ok, rp = val.validate_flat_columns(spec, df, strict=False)
    assert ok is True
    data = json.loads(Path(rp).read_text())
    assert "extra" in data["new_columns"]
    _cleanup_report(rp)


def test_validate_raw_schema_events_ok(tmp_path):
    nd = tmp_path / "ok.json"
    nd.write_text('{"a":1,"when":"2020-01-01"}\n')
    spec = DatasetSpec(
        name="evt_ok",
        kind="events",
        raw_path=str(nd),
        raw_schema={"a": pl.Int64, "when": pl.Date},
        flat_expected_cols=[],
        allow_new_columns=False,
    )
    ok, rp = val.validate_raw_schema(spec, strict=True)
    assert ok is True
    _cleanup_report(rp)


def test_ndjson_keys_and_rowcount_errors_counted(tmp_path):
    p = tmp_path / "keys.json"
    p.write_text('{"k":1}\nnotjson\n{"k":2}\n')
    keys, n = val._ndjson_keys_and_rowcount(p)
    assert "k" in keys and n == 3
