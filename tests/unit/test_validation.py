from src.domain.schema_registry import DATASETS
from src.application.validation import validate_raw_schema
from tests.unit.data.pays import df_pays_raw
from tests.unit.data.taps import df_taps_raw
from tests.unit.data.prints import df_prints_raw


def test_pays_raw_schema_ok():
    spec = DATASETS["pays"]
    ok, _ = validate_raw_schema(spec, df=df_pays_raw, strict=True)
    assert ok


def test_taps_raw_schema_ok():
    spec = DATASETS["taps"]
    ok, _ = validate_raw_schema(spec, df=df_taps_raw, strict=True)
    assert ok


def test_prints_raw_schema_ok():
    spec = DATASETS["prints"]
    ok, _ = validate_raw_schema(spec, df=df_prints_raw, strict=True)
    assert ok
