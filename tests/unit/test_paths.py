from __future__ import annotations
import os
import importlib
import pathlib
import types


def _reload_paths(env: dict[str, str] | None = None) -> types.ModuleType:
    for k in ("RAW_DATA_DIR", "OUT_DATA_DIR", "EXPECTATIONS_REPORTS_DIR"):
        os.environ.pop(k, None)
    if env:
        os.environ.update(env)
    import src.config.paths as paths

    return importlib.reload(paths)


def test_defaults_are_local_strings():
    mod = _reload_paths()
    assert isinstance(mod.RAW_DATA_DIR, str)
    assert isinstance(mod.OUT_DATA_DIR, str)
    assert isinstance(mod.EXPECTATIONS_REPORTS_DIR, str)

    assert not mod.RAW_DATA_DIR.startswith("s3://")
    assert not mod.OUT_DATA_DIR.startswith("s3://")
    assert not mod.EXPECTATIONS_REPORTS_DIR.startswith("s3://")

    assert pathlib.Path(mod.RAW_DATA_DIR).exists()
    assert pathlib.Path(mod.OUT_DATA_DIR).exists()
    assert pathlib.Path(mod.EXPECTATIONS_REPORTS_DIR).exists()


def test_s3_overrides_trailing_slash_stripped():
    env = {
        "RAW_DATA_DIR": "s3://raw/",
        "OUT_DATA_DIR": "s3://out///",
        "EXPECTATIONS_REPORTS_DIR": "s3://expectations/reports/",
    }
    mod = _reload_paths(env)
    assert mod.RAW_DATA_DIR == "s3://raw"
    assert mod.OUT_DATA_DIR == "s3://out"
    assert mod.EXPECTATIONS_REPORTS_DIR == "s3://expectations/reports"


def test_resolve_path_helper_direct(tmp_path):
    default = tmp_path / "data" / "raw"

    os.environ["RAW_DATA_DIR"] = "s3://bucket/raw///"
    import src.config.paths as paths

    assert paths._resolve_path("RAW_DATA_DIR", default) == "s3://bucket/raw"

    os.environ["RAW_DATA_DIR"] = "/alguna/ruta/local"
    assert paths._resolve_path("RAW_DATA_DIR", default) == str(default)

    os.environ.pop("RAW_DATA_DIR", None)
    assert paths._resolve_path("RAW_DATA_DIR", default) == str(default)
