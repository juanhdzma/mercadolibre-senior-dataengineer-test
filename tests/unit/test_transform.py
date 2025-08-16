from polars.testing import assert_frame_equal
import polars as pl
from src.application.transform_service import build_output_and_export
from tests.units.data.pays import df_pays_flat
from tests.units.data.taps import df_taps_flat_expected
from tests.units.data.prints import df_prints_flat_expected
from tests.units.data.final import df_final_expected


def test_build_output_ok(tmp_path, monkeypatch):
    dfs = {
        "pays": df_pays_flat(),
        "taps": df_taps_flat_expected(),
        "prints": df_prints_flat_expected(),
    }
    from src.application import transform_service as ts

    real_Path = ts.Path

    def _Path(p):
        if p == "../data/out":
            return tmp_path
        return real_Path(p)

    monkeypatch.setattr(ts, "Path", _Path)
    out_dir = build_output_and_export(dfs, ds="2025-08-15")
    out_csv = tmp_path / "2025-08-15.csv"
    assert out_csv.exists()
    got = pl.read_csv(out_csv)
    exp = df_final_expected()
    assert_frame_equal(got.sort(["user_id", "value_prop"]), exp.sort(["user_id", "value_prop"]), check_dtypes=False)
