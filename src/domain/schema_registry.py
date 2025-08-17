from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias, Union, Type
import polars as pl
from src.config.paths import RAW_DATA_DIR

_PolarsBase = pl.DataType
PolarsDType: TypeAlias = Union[Type[_PolarsBase], _PolarsBase]


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    kind: str
    raw_path: Path
    raw_schema: dict[str, PolarsDType]
    flat_expected_cols: list[str]
    allow_new_columns: bool = True


EVENT_STRUCT: pl.Struct = pl.Struct(
    [pl.Field("position", pl.Int64), pl.Field("value_prop", pl.Utf8)]
)

PAYS_RAW_SCHEMA: dict[str, PolarsDType] = {
    "pay_date": pl.Date,
    "total": pl.Float64,
    "user_id": pl.Int64,
    "value_prop": pl.Utf8,
}

EVENTS_RAW_SCHEMA: dict[str, PolarsDType] = {
    "day": pl.Date,
    "event_data": EVENT_STRUCT,
    "user_id": pl.Int64,
}

EVENTS_FLAT_COLS = ["day", "position", "value_prop", "user_id"]

DATASETS: dict[str, DatasetSpec] = {
    "pays": DatasetSpec(
        name="pays",
        kind="pays",
        raw_path=RAW_DATA_DIR / "pays.csv",
        raw_schema=PAYS_RAW_SCHEMA,
        flat_expected_cols=[],
        allow_new_columns=True,
    ),
    "taps": DatasetSpec(
        name="taps",
        kind="events",
        raw_path=RAW_DATA_DIR / "taps.json",
        raw_schema=EVENTS_RAW_SCHEMA,
        flat_expected_cols=EVENTS_FLAT_COLS,
        allow_new_columns=True,
    ),
    "prints": DatasetSpec(
        name="prints",
        kind="events",
        raw_path=RAW_DATA_DIR / "prints.json",
        raw_schema=EVENTS_RAW_SCHEMA,
        flat_expected_cols=EVENTS_FLAT_COLS,
        allow_new_columns=True,
    ),
}
