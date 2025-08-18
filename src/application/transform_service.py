from __future__ import annotations
import fsspec  # type: ignore[import-untyped]
import polars as pl
from src.adapters.logging import get_logger
from src.config.paths import OUT_DATA_DIR

log = get_logger()


def get_last_week(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.col(column_name).dt.truncate("1w").alias("week_start")
        )
        .filter(pl.col("week_start") == pl.col("week_start").max())
        .drop("week_start")
    )


def get_last_weeks(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.col(column_name).dt.truncate("1w").alias("week_start")
        )
        .filter(
            pl.col("week_start").is_in(
                pl.col("week_start").unique().sort().tail(4).head(3).implode()
            )
        )
        .drop("week_start")
    )


def build_output_and_export(dfs: dict) -> tuple[str, str]:
    pays, taps, prints = dfs["pays"], dfs["taps"], dfs["prints"]
    out = get_last_week(prints, "day")
    counts = (
        get_last_weeks(prints, "day")
        .group_by(["user_id", "value_prop"])
        .agg(pl.len().cast(pl.Int64).alias("cantidad_vistas"))
    )
    out = out.join(
        counts, on=["user_id", "value_prop"], how="left"
    ).with_columns(pl.col("cantidad_vistas").fill_null(0).cast(pl.Int64))
    counts = (
        get_last_weeks(taps, "day")
        .group_by(["user_id", "value_prop"])
        .agg(pl.len().cast(pl.Int64).alias("cantidad_taps"))
    )
    out = (
        out.join(counts, on=["user_id", "value_prop"], how="left")
        .with_columns(pl.col("cantidad_taps").fill_null(0).cast(pl.Int64))
        .with_columns((pl.col("cantidad_taps") > 0).alias("hizo_click"))
    )
    counts = (
        get_last_weeks(pays, "pay_date")
        .group_by(["user_id", "value_prop"])
        .agg(pl.len().cast(pl.Int64).alias("cantidad_pagos"))
    )
    out = out.join(
        counts, on=["user_id", "value_prop"], how="left"
    ).with_columns(pl.col("cantidad_pagos").fill_null(0).cast(pl.Int64))
    counts = (
        get_last_weeks(pays, "pay_date")
        .group_by(["user_id", "value_prop"])
        .agg(pl.sum("total").cast(pl.Int64).alias("total_pagos"))
    )
    out = out.join(
        counts, on=["user_id", "value_prop"], how="left"
    ).with_columns(pl.col("total_pagos").fill_null(0).cast(pl.Int64))
    csv_path = f"{OUT_DATA_DIR}/final.csv"
    pq_path = f"{OUT_DATA_DIR}/final.parquet"
    with fsspec.open(csv_path, "wb") as f:
        out.write_csv(f)
    with fsspec.open(pq_path, "wb") as f:
        out.write_parquet(f)
    log.info("export_done", rows=out.height, csv=csv_path, parquet=pq_path)
    return csv_path, pq_path
