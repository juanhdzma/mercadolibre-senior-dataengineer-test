from __future__ import annotations
import json
import re
import os
from datetime import datetime
from typing import Any, Mapping
import polars as pl
from polars.datatypes import DataType, DataTypeClass
import fsspec  # type: ignore[import-untyped]
from src.adapters.logging import get_logger
from src.domain.schema_registry import DatasetSpec

log = get_logger()

REPORT_BASE = os.getenv(
    "EXPECTATIONS_REPORTS_DIR", "expectations/reports"
).rstrip("/")

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?((\d+(\.\d*)?)|(\.\d+))([eE][+-]?\d+)?$")

PolarsDType = DataType | DataTypeClass


def _is_int_like(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, int):
        return True
    if isinstance(x, str):
        return bool(_INT_RE.match(x.strip()))
    return False


def _is_float_like(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, (int, float)):
        return True
    if isinstance(x, str):
        return bool(_FLOAT_RE.match(x.strip()))
    return False


def _is_bool_like(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, bool):
        return True
    if isinstance(x, (int, float)) and x in {0, 1}:
        return True
    if isinstance(x, str) and x.strip().lower() in {
        "true",
        "false",
        "1",
        "0",
        "t",
        "f",
        "yes",
        "no",
    }:
        return True
    return False


def _is_date_like(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, datetime):
        return True
    if isinstance(x, str):
        s = x.strip()
        try:
            (
                datetime.fromisoformat(s.replace("Z", "+00:00"))
                if "T" in s
                else datetime.fromisoformat(s)
            )
            return True
        except Exception:
            return False
    return False


def _write_text(path: str, text: str) -> str:
    with fsspec.open(path, "w") as f:
        f.write(text)
    return path


def _write_report(dataset: str, stage: str, payload: dict) -> str:
    dir_base = f"{REPORT_BASE}/{dataset}"
    filename = f"schema_{stage}.json"
    full = f"{dir_base}/{filename}"
    return _write_text(full, json.dumps(payload, ensure_ascii=False, indent=2))


def _csv_columns(uri: str) -> list[str]:
    return pl.read_csv(uri, n_rows=0).columns


def _csv_rowcount(uri: str) -> int:
    return int(
        pl.scan_csv(uri, infer_schema_length=0)
        .select(pl.len())
        .collect()
        .item()
    )


def _resolve_temporal_dtype(
    t: PolarsDType,
) -> type[pl.Date] | type[pl.Datetime]:
    return pl.Date if t == pl.Date else pl.Datetime


def _invalid_token_counts_csv(
    uri: str, expected: Mapping[str, PolarsDType]
) -> dict[str, int]:
    present = set(_csv_columns(uri))
    cols = [c for c in expected.keys() if c in present]
    if not cols:
        return {}
    lf = pl.scan_csv(
        uri,
        schema_overrides={c: pl.Utf8 for c in cols},
        infer_schema_length=0,
        ignore_errors=True,
    )
    exprs = []
    for col in cols:
        t = expected[col]
        s = pl.col(col)
        if t in (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ):
            invalid = (
                ~s.str.contains(_INT_RE.pattern, literal=False)
            ) & s.is_not_null()
        elif t in (pl.Float32, pl.Float64):
            invalid = (
                ~s.str.contains(_FLOAT_RE.pattern, literal=False)
            ) & s.is_not_null()
        elif t == pl.Boolean:
            invalid = (
                ~s.str.to_lowercase().is_in(
                    ["true", "false", "1", "0", "t", "f", "yes", "no"]
                )
            ) & s.is_not_null()
        elif t in (pl.Date, pl.Datetime):
            invalid = (
                s.str.strptime(
                    _resolve_temporal_dtype(t), strict=False
                ).is_null()
                & s.is_not_null()
            )
        else:
            invalid = pl.lit(False)
        exprs.append(invalid.cast(pl.Int64).sum().alias(col))
    out = lf.select(exprs).collect().to_dicts()[0]
    return {k: int(out.get(k) or 0) for k in cols}


def _ndjson_keys_and_rowcount(
    uri: str, limit_keys: int = 20000
) -> tuple[set[str], int]:
    keys: set[str] = set()
    n = 0
    with fsspec.open(uri, "r") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            n += 1
            if len(keys) < limit_keys:
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        keys.update(obj.keys())
                except Exception as e:
                    log.debug("ndjson_parse_error", error=str(e))
    return keys, n


def _invalid_token_counts_ndjson(
    uri: str, expected: Mapping[str, PolarsDType]
) -> dict[str, int]:
    checks = {
        k: v for k, v in expected.items() if not isinstance(v, pl.Struct)
    }
    if not checks:
        return {}
    counts = {k: 0 for k in checks.keys()}
    with fsspec.open(uri, "r") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception as e:
                log.debug("ndjson_line_error", error=str(e))
                continue
            if not isinstance(obj, dict):
                continue
            for col, t in checks.items():
                if col not in obj or obj[col] is None:
                    continue
                v = obj[col]
                ok = True
                if t in (
                    pl.Int8,
                    pl.Int16,
                    pl.Int32,
                    pl.Int64,
                    pl.UInt8,
                    pl.UInt16,
                    pl.UInt32,
                    pl.UInt64,
                ):
                    ok = _is_int_like(v)
                elif t in (pl.Float32, pl.Float64):
                    ok = _is_float_like(v)
                elif t == pl.Boolean:
                    ok = _is_bool_like(v)
                elif t in (pl.Date, pl.Datetime):
                    ok = _is_date_like(v)
                if not ok:
                    counts[col] += 1
    return {k: v for k, v in counts.items() if v > 0}


def validate_raw_schema_pays(
    spec: DatasetSpec, df: pl.DataFrame | None = None, strict: bool = True
) -> tuple[bool, str]:
    try:
        present = set(_csv_columns(spec.raw_path))
        rowcount = (
            df.height if df is not None else _csv_rowcount(spec.raw_path)
        )
        invalid_tokens = _invalid_token_counts_csv(
            spec.raw_path, spec.raw_schema
        )
    except Exception as e:
        rp = _write_report(
            spec.name,
            "raw",
            {
                "dataset": spec.name,
                "stage": "raw",
                "rows": 0,
                "missing_columns": [],
                "new_columns": [],
                "wrong_types": [],
                "expected_schema": {
                    k: str(v) for k, v in spec.raw_schema.items()
                },
                "source_columns": [],
                "read_error": str(e),
                "ok": False,
            },
        )
        log.error("raw_read_error", dataset=spec.name, report=rp, error=str(e))
        return False, rp
    exp_cols = set(spec.raw_schema.keys())
    missing = sorted(list(exp_cols - present))
    new_cols = sorted(list(present - exp_cols))
    wrong_types = [
        {"column": c, "expected": str(spec.raw_schema[c])}
        for c, n in sorted(invalid_tokens.items())
        if n > 0
    ]
    ok = (
        (not missing)
        and (not wrong_types)
        and (spec.allow_new_columns or not new_cols)
    )
    rp = _write_report(
        spec.name,
        "raw",
        {
            "dataset": spec.name,
            "stage": "raw",
            "rows": int(rowcount),
            "missing_columns": missing,
            "new_columns": new_cols,
            "wrong_types": wrong_types,
            "expected_schema": {k: str(v) for k, v in spec.raw_schema.items()},
            "source_columns": sorted(list(present)),
            "ok": ok,
        },
    )
    if missing or wrong_types:
        log.error(
            "raw_schema_error",
            dataset=spec.name,
            missing=missing,
            wrong_types=wrong_types,
            new_columns=new_cols,
            report=rp,
        )
        if strict:
            return False, rp
    if new_cols and not spec.allow_new_columns and strict:
        log.error(
            "raw_schema_new_cols_error",
            dataset=spec.name,
            new=new_cols,
            report=rp,
        )
        return False, rp
    log.info("raw_schema_ok", dataset=spec.name, report=rp)
    return True, rp


def validate_raw_schema_events(
    spec: DatasetSpec, df: pl.DataFrame | None = None, strict: bool = True
) -> tuple[bool, str]:
    try:
        keys, rowcount = _ndjson_keys_and_rowcount(spec.raw_path)
        present = set(keys)
        invalid_tokens = _invalid_token_counts_ndjson(
            spec.raw_path, spec.raw_schema
        )
    except Exception as e:
        rp = _write_report(
            spec.name,
            "raw",
            {
                "dataset": spec.name,
                "stage": "raw",
                "rows": 0,
                "missing_columns": [],
                "new_columns": [],
                "wrong_types": [],
                "expected_schema": {
                    k: str(v) for k, v in spec.raw_schema.items()
                },
                "source_columns": [],
                "read_error": str(e),
                "ok": False,
            },
        )
        log.error("raw_read_error", dataset=spec.name, report=rp, error=str(e))
        return False, rp
    exp_cols = set(spec.raw_schema.keys())
    missing = sorted(list(exp_cols - present))
    new_cols = sorted(list(present - exp_cols))
    wrong_types = [
        {"column": c, "expected": str(spec.raw_schema[c])}
        for c, n in sorted(invalid_tokens.items())
        if n > 0
    ]
    ok = (
        (not missing)
        and (not wrong_types)
        and (spec.allow_new_columns or not new_cols)
    )
    rp = _write_report(
        spec.name,
        "raw",
        {
            "dataset": spec.name,
            "stage": "raw",
            "rows": int(rowcount),
            "missing_columns": missing,
            "new_columns": new_cols,
            "wrong_types": wrong_types,
            "expected_schema": {k: str(v) for k, v in spec.raw_schema.items()},
            "source_columns": sorted(list(present)),
            "ok": ok,
        },
    )
    if missing or wrong_types:
        log.error(
            "raw_schema_error",
            dataset=spec.name,
            missing=missing,
            wrong_types=wrong_types,
            new_columns=new_cols,
            report=rp,
        )
        if strict:
            return False, rp
    if new_cols and not spec.allow_new_columns and strict:
        log.error(
            "raw_schema_new_cols_error",
            dataset=spec.name,
            new=new_cols,
            report=rp,
        )
        return False, rp
    log.info("raw_schema_ok", dataset=spec.name, report=rp)
    return True, rp


def validate_flat_columns(
    spec: DatasetSpec, df_flat: pl.DataFrame, strict: bool = True
) -> tuple[bool, str]:
    expected_cols = spec.flat_expected_cols
    present_cols = list(df_flat.columns)
    miss = [c for c in expected_cols if c not in present_cols]
    extras = [c for c in present_cols if c not in expected_cols]
    ok = (not miss) and (not extras)
    rp = _write_report(
        spec.name,
        "flat",
        {
            "dataset": spec.name,
            "stage": "flat",
            "expected_columns": expected_cols,
            "present_columns": present_cols,
            "missing_columns": miss,
            "new_columns": extras,
            "ok": ok,
        },
    )
    if miss or extras:
        log.error(
            "flat_schema_error",
            dataset=spec.name,
            missing=miss,
            new_columns=extras,
            report=rp,
        )
        if strict:
            return False, rp
    log.info("flat_schema_ok", dataset=spec.name, report=rp)
    return True, rp


def validate_raw_schema(
    spec: DatasetSpec, df: pl.DataFrame | None = None, strict: bool = True
) -> tuple[bool, str]:
    if spec.kind == "pays":
        return validate_raw_schema_pays(spec, df, strict)
    return validate_raw_schema_events(spec, df, strict)
