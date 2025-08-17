# src/config/paths.py
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", PROJECT_ROOT / "data" / "raw"))
OUT_DATA_DIR = Path(os.getenv("OUT_DATA_DIR", PROJECT_ROOT / "data" / "out"))
EXPECTATIONS_REPORTS_DIR = Path(
    os.getenv(
        "EXPECTATIONS_REPORTS_DIR", PROJECT_ROOT / "expectations" / "reports"
    )
)
