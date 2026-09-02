"""
Pipeline orchestrator.

Sequences: ingest (batch + incremental) -> transform -> validate -> load.
Each stage is wrapped so a failure in one stage is logged with context and
stops the run cleanly, rather than corrupting downstream tables.

Run: python src/pipeline.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from logger_setup import get_logger
import ingest
import transform_validate

logger = get_logger("pipeline")

STAGES = [
    ("ingest_batch", ingest.load_batch),
    ("ingest_incremental", ingest.load_incremental),
    ("transform_and_validate", transform_validate.run),
]


def run_pipeline():
    logger.info("=" * 60)
    logger.info("PIPELINE RUN START")
    start = time.time()

    for stage_name, stage_fn in STAGES:
        stage_start = time.time()
        try:
            stage_fn()
            logger.info(f"STAGE '{stage_name}' completed in {time.time() - stage_start:.2f}s")
        except Exception as e:
            logger.error(f"STAGE '{stage_name}' FAILED after {time.time() - stage_start:.2f}s: {e}")
            logger.error("PIPELINE RUN ABORTED")
            sys.exit(1)

    logger.info(f"PIPELINE RUN COMPLETE in {time.time() - start:.2f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
