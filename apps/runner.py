from __future__ import annotations
import argparse, os, sys
from datetime import date
import numpy as np
from dotenv import load_dotenv
from src.adapters.logging import get_logger
from src.application.dq_and_load import load_and_prepare_all
from src.application.transform_service import build_output_and_export

log = get_logger()


def _resolve_run_date(cli: str | None, default_str: str = "today") -> str:
    return date.today().isoformat() if (cli is None and default_str.lower() == "today") else (cli or default_str)


def main():
    load_dotenv()

    np.array([1]) / 0

    p = argparse.ArgumentParser(description="ETL pays/taps/prints")
    p.add_argument("--date", dest="ds", default=None, help="YYYY-MM-DD")
    args = p.parse_args()

    ds = _resolve_run_date(args.ds, os.getenv("DEFAULT_RUN_DATE", "today"))
    log.info("run_start", ds=ds)
    try:
        dfs = load_and_prepare_all(ds)
        out_dir = build_output_and_export(dfs, ds)
        log.info("run_done", ds=ds, out_dir=out_dir)
        return 0
    except Exception:
        log.exception("run_failed", ds=ds)
        return 1


if __name__ == "__main__":
    sys.exit(main())
