from __future__ import annotations
from pathlib import Path
import json, polars as pl
from src.adapters.logging import get_logger
from src.domain.schema_registry import DatasetSpec

log = get_logger()


def flatten_events(spec: DatasetSpec, df_raw: pl.DataFrame) -> pl.DataFrame:
    if spec.kind != "events":
        return df_raw
    df = df_raw.unnest("event_data")
    keep = [c for c in spec.flat_expected_cols if c in df.columns]
    return df.select(keep) if keep else df


def validate_flat_columns(spec: DatasetSpec, ds: str, df_flat: pl.DataFrame, strict: bool = True) -> tuple[bool, str]:
    if spec.kind != "events":
        return True, ""
    expected = set(spec.flat_expected_cols)
    present = set(df_flat.columns)
    missing = sorted(list(expected - present))
    ok = not missing
    report = {
        "dataset": spec.name,
        "stage": "flat",
        "rows": df_flat.height,
        "missing_columns": missing,
        "ok": ok,
    }
    out_dir = Path(f"expectations/reports/{spec.name}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"schema_flat_{ds}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    if missing:
        log.error("flat_schema_missing", dataset=spec.name, missing=missing)
        if strict:
            return False, str(out_path)
    log.info("flat_schema_ok", dataset=spec.name)
    return True, str(out_path)
