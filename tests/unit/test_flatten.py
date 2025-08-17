from polars.testing import assert_frame_equal
from src.domain.schema_registry import DATASETS
from src.application.flatten import flatten_events, validate_flat_columns
from tests.unit.data.taps import df_taps_raw, df_taps_flat_expected
from tests.unit.data.prints import df_prints_raw, df_prints_flat_expected


def test_flatten_taps_ok():
    spec = DATASETS["taps"]
    df_raw = df_taps_raw
    df_flat = flatten_events(spec, df_raw)
    ok, _ = validate_flat_columns(spec, df_flat=df_flat, strict=True)
    assert ok
    assert_frame_equal(
        df_flat.sort(["user_id", "day", "position"]),
        df_taps_flat_expected.sort(["user_id", "day", "position"]),
        check_dtypes=False,
    )


def test_flatten_prints_ok():
    spec = DATASETS["prints"]
    df_raw = df_prints_raw
    df_flat = flatten_events(spec, df_raw)
    ok, _ = validate_flat_columns(spec, df_flat=df_flat, strict=True)
    assert ok
    assert_frame_equal(
        df_flat.sort(["user_id", "day", "position"]),
        df_prints_flat_expected.sort(["user_id", "day", "position"]),
        check_dtypes=False,
    )
