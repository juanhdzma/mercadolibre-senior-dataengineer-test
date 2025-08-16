from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import polars as pl
import os


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    kind: str
    raw_path: Path
    raw_schema: dict[str, pl.DataType]
    flat_expected_cols: list[str]
    allow_new_columns: bool = True


EVENT_STRUCT = pl.Struct([pl.Field("position", pl.Int64), pl.Field("value_prop", pl.Utf8)])

PAYS_RAW_SCHEMA = {
    "pay_date": pl.Date,
    "total": pl.Float64,
    "user_id": pl.Int64,
    "value_prop": pl.Utf8,
}
EVENTS_RAW_SCHEMA = {
    "day": pl.Date,
    "event_data": EVENT_STRUCT,
    "user_id": pl.Int64,
}
EVENTS_FLAT_COLS = ["day", "position", "value_prop", "user_id"]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_BASE = Path(os.getenv("RAW_BASE_DIR", str(PROJECT_ROOT / "data" / "raw")))

DATASETS: dict[str, DatasetSpec] = {
    "pays": DatasetSpec(
        name="pays",
        kind="pays",
        raw_path=RAW_BASE / "pays.csv",
        raw_schema=PAYS_RAW_SCHEMA,
        flat_expected_cols=[],
        allow_new_columns=True,
    ),
    "taps": DatasetSpec(
        name="taps",
        kind="events",
        raw_path=RAW_BASE / "taps.json",
        raw_schema=EVENTS_RAW_SCHEMA,
        flat_expected_cols=EVENTS_FLAT_COLS,
        allow_new_columns=True,
    ),
    "prints": DatasetSpec(
        name="prints",
        kind="events",
        raw_path=RAW_BASE / "prints.json",
        raw_schema=EVENTS_RAW_SCHEMA,
        flat_expected_cols=EVENTS_FLAT_COLS,
        allow_new_columns=True,
    ),
}
