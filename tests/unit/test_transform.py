from pathlib import Path
import polars as pl
from polars.testing import assert_frame_equal
from src.application import transform_service as ts
from src.application.transform_service import build_output_and_export
from tests.unit.data.final import df_final_expected
from tests.unit.data.pays import df_pays_raw
from tests.unit.data.prints import df_prints_flat_expected
from tests.unit.data.taps import df_taps_flat_expected


def test_build_output_ok(tmp_path: Path, monkeypatch) -> None:
    dfs = {
        "pays": df_pays_raw,
        "taps": df_taps_flat_expected,
        "prints": df_prints_flat_expected,
    }
    monkeypatch.setattr(ts, "OUT_DATA_DIR", tmp_path, raising=True)

    csv_path, pq_path = build_output_and_export(dfs)

    assert Path(csv_path).exists()
    assert Path(pq_path).exists()

    got = pl.read_csv(csv_path)
    exp = df_final_expected
    assert_frame_equal(
        got.sort(["user_id", "value_prop"]),
        exp.sort(["user_id", "value_prop"]),
        check_dtypes=False,
    )
