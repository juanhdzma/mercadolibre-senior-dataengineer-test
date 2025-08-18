from __future__ import annotations
import json
import polars as pl
import fsspec  # type: ignore[import-untyped]
from src.adapters.logging import get_logger
from src.domain.schema_registry import DatasetSpec
from src.config.paths import EXPECTATIONS_REPORTS_DIR

log = get_logger()


def _join_report_path(dataset: str, filename: str) -> str:
    base = EXPECTATIONS_REPORTS_DIR
    if hasattr(base, "joinpath"):
        return str(base.joinpath(dataset, filename))
    return f"{str(base).rstrip('/')}/{dataset}/{filename}"


def _write_report(dataset: str, filename: str, payload: dict) -> str:
    uri = _join_report_path(dataset, filename)
    with fsspec.open(uri, "w") as f:
        f.write(json.dumps(payload, ensure_ascii=False, indent=2))
    return uri


def flatten_events(spec: DatasetSpec, df_raw: pl.DataFrame) -> pl.DataFrame:
    if spec.kind != "events":
        return df_raw
    df = df_raw
    if "event_data" in df.columns:
        df = df.unnest("event_data")
    keep = [c for c in spec.flat_expected_cols if c in df.columns]
    return df.select(keep) if keep else df


def validate_flat_columns(
    spec: DatasetSpec, df_flat: pl.DataFrame, strict: bool = True
) -> tuple[bool, str]:
    if spec.kind != "events":
        return True, ""

    expected_cols = list(spec.flat_expected_cols)
    present_cols = list(df_flat.columns)
    missing = [c for c in expected_cols if c not in present_cols]
    new_cols = [c for c in present_cols if c not in expected_cols]
    ok = (not missing) and (not new_cols)

    report = {
        "dataset": spec.name,
        "stage": "flat",
        "rows": int(df_flat.height),
        "expected_columns": expected_cols,
        "present_columns": present_cols,
        "missing_columns": missing,
        "new_columns": new_cols,
        "ok": ok,
    }
    rp = _write_report(spec.name, "schema_flat.json", report)

    if not ok:
        if missing and not new_cols:
            ev = "flat_schema_missing"
        elif new_cols and not missing:
            ev = "flat_schema_new_cols"
        else:
            ev = "flat_schema_error"
        log.error(
            ev,
            dataset=spec.name,
            missing=missing,
            new_columns=new_cols,
            report=rp,
        )
        if strict:
            return False, rp

    log.info("flat_schema_ok", dataset=spec.name, report=rp)
    return True, rp
