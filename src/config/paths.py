from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(str(PROJECT_ROOT / ".env"))


def _resolve_path(var_name: str, default: Path) -> str:
    """Devuelve str si es S3, Path si es local."""
    val = os.getenv(var_name)
    if val and val.startswith("s3://"):
        return val.rstrip("/")
    return str(default)


RAW_DATA_DIR = _resolve_path("RAW_DATA_DIR", PROJECT_ROOT / "data" / "raw")
OUT_DATA_DIR = _resolve_path("OUT_DATA_DIR", PROJECT_ROOT / "data" / "out")
EXPECTATIONS_REPORTS_DIR = _resolve_path(
    "EXPECTATIONS_REPORTS_DIR", PROJECT_ROOT / "expectations" / "reports"
)
