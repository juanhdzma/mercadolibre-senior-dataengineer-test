from __future__ import annotations
from pathlib import Path
import json, re
from datetime import datetime
import polars as pl
from src.adapters.logging import get_logger
from src.domain.schema_registry import DatasetSpec

log = get_logger()

REPORT_BASE = Path(__file__).resolve().parents[2] / "expectations" / "reports"

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?((\d+(\.\d*)?)|(\.\d+))([eE][+-]?\d+)?$")


def _is_int_like(x) -> bool:
    if x is None:
        return True
    if isinstance(x, int):
        return True
    if isinstance(x, str):
        return bool(_INT_RE.match(x.strip()))
    return False


def _is_float_like(x) -> bool:
    if x is None:
        return True
    if isinstance(x, (int, float)):
        return True
    if isinstance(x, str):
        return bool(_FLOAT_RE.match(x.strip()))
    return False


def _is_bool_like(x) -> bool:
    if x is None:
        return True
    if isinstance(x, bool):
        return True
    if isinstance(x, str):
        return x.strip().lower() in {"true", "false", "1", "0", "t", "f", "yes", "no"}
    if isinstance(x, (int, float)):
        return x in {0, 1}
    return False


def _is_date_like(x) -> bool:
    if x is None:
        return True
    if isinstance(x, datetime):
        return True
    if isinstance(x, str):
        s = x.strip()
        try:
            if "T" in s:
                datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                datetime.fromisoformat(s)
            return True
        except Exception:
            return False
    return False


def _csv_columns(path: Path) -> list[str]:
    return pl.read_csv(path, n_rows=0).columns


def _csv_rowcount(path: Path) -> int:
    return int(pl.scan_csv(path, infer_schema_length=0).select(pl.len()).collect().item())


def _invalid_token_counts_csv(path: Path, expected: dict[str, pl.DataType]) -> dict[str, int]:
    present = set(_csv_columns(path))
    cols = [c for c in expected.keys() if c in present]
    if not cols:
        return {}
    lf = pl.scan_csv(path, dtypes={c: pl.Utf8 for c in cols}, infer_schema_length=0, ignore_errors=True)
    exprs = []
    for col in cols:
        t = expected[col]
        s = pl.col(col)
        if t in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            invalid = (~s.str.contains(_INT_RE.pattern, literal=False)) & s.is_not_null()
        elif t in (pl.Float32, pl.Float64):
            invalid = (~s.str.contains(_FLOAT_RE.pattern, literal=False)) & s.is_not_null()
        elif t == pl.Boolean:
            invalid = (~s.str.to_lowercase().is_in(["true", "false", "1", "0", "t", "f", "yes", "no"])) & s.is_not_null()
        elif t in (pl.Date, pl.Datetime):
            invalid = s.str.strptime(t, strict=False).is_null() & s.is_not_null()
        else:
            invalid = pl.lit(False)
        exprs.append(invalid.cast(pl.Int64).sum().alias(col))
    out = lf.select(exprs).collect().to_dicts()[0]
    return {k: int(out.get(k) or 0) for k in cols}


def _ndjson_keys_and_rowcount(path: Path, limit_keys: int = 20000) -> tuple[set[str], int]:
    keys: set[str] = set()
    n = 0
    with open(path, "r", encoding="utf-8") as f:
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
                except Exception:
                    pass
    return keys, n


def _invalid_token_counts_ndjson(path: Path, expected: dict[str, pl.DataType]) -> dict[str, int]:
    checks = {k: v for k, v in expected.items() if not isinstance(v, pl.Struct)}
    if not checks:
        return {}
    counts = {k: 0 for k in checks.keys()}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            for col, t in checks.items():
                if col not in obj or obj[col] is None:
                    continue
                v = obj[col]
                ok = True
                if t in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
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


def _write_report(path: Path, payload: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return str(path)


def validate_raw_schema(spec: DatasetSpec, ds: str, df: pl.DataFrame | None = None, strict: bool = True) -> tuple[bool, str]:
    try:
        if spec.kind == "pays":
            present_raw = set(_csv_columns(spec.raw_path))
            rowcount = df.height if df is not None else _csv_rowcount(spec.raw_path)
            invalid_tokens = _invalid_token_counts_csv(spec.raw_path, spec.raw_schema)
        else:
            keys, rowcount = _ndjson_keys_and_rowcount(spec.raw_path)
            present_raw = set(keys)
            invalid_tokens = _invalid_token_counts_ndjson(spec.raw_path, spec.raw_schema)
    except Exception as e:
        out_dir = REPORT_BASE / spec.name
        out_path = out_dir / f"schema_raw_{ds}.json"
        report = {
            "dataset": spec.name,
            "stage": "raw",
            "rows": 0,
            "missing_columns": [],
            "new_columns": [],
            "wrong_types": [],
            "expected_schema": {k: str(v) for k, v in spec.raw_schema.items()},
            "source_columns": [],
            "read_error": str(e),
            "ok": False,
        }
        rp = _write_report(out_path, report)
        log.error("raw_read_error", dataset=spec.name, report=rp, error=str(e))
        return False, rp

    exp_cols = set(spec.raw_schema.keys())
    missing = sorted(list(exp_cols - present_raw))
    new_cols = sorted(list(present_raw - exp_cols))

    wrong_types = [{"column": c, "expected": str(spec.raw_schema[c])} for c, n in sorted(invalid_tokens.items()) if n > 0]

    out_dir = REPORT_BASE / spec.name
    out_path = out_dir / f"schema_raw_{ds}.json"
    ok = (not missing) and (not wrong_types) and (spec.allow_new_columns or not new_cols)

    report = {
        "dataset": spec.name,
        "stage": "raw",
        "rows": int(rowcount),
        "missing_columns": missing,
        "new_columns": new_cols,
        "expected_schema": {k: str(v) for k, v in spec.raw_schema.items()},
        "source_columns": sorted(list(present_raw)),
        "ok": ok,
    }
    if wrong_types:
        report["wrong_types"] = wrong_types

    rp = _write_report(out_path, report)

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
        log.error("raw_schema_new_cols_error", dataset=spec.name, new=new_cols, report=rp)
        return False, rp

    log.info("raw_schema_ok", dataset=spec.name, report=rp)
    return True, rp
