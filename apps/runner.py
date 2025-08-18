from __future__ import annotations
import sys
from datetime import date
from src.adapters.logging import get_logger
from src.application.dq_and_load import load_and_prepare_all
from src.application.transform_service import build_output_and_export

log = get_logger()


def main():
    today = date.today().isoformat()
    log.info("run_start", today=today)

    try:
        dfs = load_and_prepare_all()
        out_dir = build_output_and_export(dfs)
        log.info("run_done", today=today, out_dir=out_dir)
        return 0
    except Exception:
        log.exception("run_failed", today=today)
        return 1


if __name__ == "__main__":
    sys.exit(main())
