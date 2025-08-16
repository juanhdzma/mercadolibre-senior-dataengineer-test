import polars as pl
from src.domain.schema_registry import DATASETS
from src.application.validation import validate_raw_schema
from tests.units.data.pays import df_pays_raw
from tests.units.data.taps import df_taps_raw
from tests.units.data.prints import df_prints_raw


def test_pays_raw_schema_ok():
    spec = DATASETS["pays"]
    df = df_pays_raw()
    ok, _ = validate_raw_schema(spec, ds="2025-08-15", df=df, strict=True)
    assert ok


def test_taps_raw_schema_ok():
    spec = DATASETS["taps"]
    df = df_taps_raw()
    ok, _ = validate_raw_schema(spec, ds="2025-08-15", df=df, strict=True)
    assert ok


def test_prints_raw_schema_ok():
    spec = DATASETS["prints"]
    df = df_prints_raw()
    ok, _ = validate_raw_schema(spec, ds="2025-08-15", df=df, strict=True)
    assert ok
