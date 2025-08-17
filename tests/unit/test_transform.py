from polars.testing import assert_frame_equal
import polars as pl
from src.application.transform_service import build_output_and_export
from src.application import transform_service as ts
from tests.unit.data.taps import df_taps_flat_expected
from tests.unit.data.prints import df_prints_flat_expected
from tests.unit.data.final import df_final_expected
from tests.unit.data.pays import df_pays_raw


def test_build_output_ok(tmp_path, monkeypatch):
    dfs = {
        "pays": df_pays_raw,
        "taps": df_taps_flat_expected,
        "prints": df_prints_flat_expected,
    }
    real_Path = ts.Path

    def _Path(p):
        if p == "../data/out":
            return tmp_path
        return real_Path(p)

    monkeypatch.setattr(ts, "Path", _Path)
    csv_path, pq_path = build_output_and_export(
        dfs,
    )
    assert csv_path.exists()
    assert pq_path.exists()
    got = pl.read_csv(csv_path)
    print(got)
    exp = df_final_expected
    assert_frame_equal(
        got.sort(["user_id", "value_prop"]),
        exp.sort(["user_id", "value_prop"]),
        check_dtypes=False,
    )
