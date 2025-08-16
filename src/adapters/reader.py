from __future__ import annotations
import polars as pl
from src.adapters.logging import get_logger
from src.domain.schema_registry import DatasetSpec

log = get_logger()


def read_raw(spec: DatasetSpec) -> pl.DataFrame:
    if spec.kind == "pays":
        df = pl.read_csv(spec.raw_path, schema=spec.raw_schema)
    elif spec.kind == "events":
        df = pl.read_ndjson(spec.raw_path, schema=spec.raw_schema)
    else:
        raise ValueError(spec.kind)
    log.info("read_raw_ok", dataset=spec.name, rows=df.height, cols=list(df.columns))
    return df
