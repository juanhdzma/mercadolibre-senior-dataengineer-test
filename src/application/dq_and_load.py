from __future__ import annotations
import polars as pl
from src.adapters.logging import get_logger
from src.domain.schema_registry import DATASETS
from src.adapters.reader import read_raw
from src.application.validation import validate_raw_schema
from src.application.flatten import flatten_events, validate_flat_columns

log = get_logger()


def load_and_prepare_all(ds: str) -> dict[str, pl.DataFrame]:
    ready: dict[str, pl.DataFrame] = {}
    for name, spec in DATASETS.items():
        ok_raw, rep_raw = validate_raw_schema(spec, ds, df=None, strict=True)
        if not ok_raw:
            raise AssertionError(f"RAW schema failed for {name}. See report: {rep_raw}")
        raw_df = read_raw(spec)
        flat_df = flatten_events(spec, raw_df)
        ok_flat, rep_flat = validate_flat_columns(spec, ds, flat_df, strict=True)
        if not ok_flat:
            raise AssertionError(f"FLAT schema failed for {name}. See report: {rep_flat}")
        ready[name] = flat_df
        log.info("dataset_ready", dataset=name, rows=flat_df.height, cols=list(flat_df.columns))
    return ready
