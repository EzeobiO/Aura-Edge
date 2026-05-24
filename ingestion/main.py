"""
Ingestion orchestrator.

Polls OpenSky every POLL_INTERVAL_SEC seconds, writes raw flight states
to one JSONL stream and correlated synthetic telemetry to another.
Handles Ctrl-C cleanly.

Run from the project root:
    python -m ingestion.main
"""

from pathlib import Path
import logging
import signal
import time

from ingestion.opensky_client import OpenSkyClient
from ingestion.telemetry_generator import TelemetryGenerator
from ingestion.file_writer import RotatingJsonlWriter


POLL_INTERVAL_SEC = 10
DATA_ROOT = Path("data/raw")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("ingestion")


def main() -> None:
    client = OpenSkyClient()
    generator = TelemetryGenerator(seed=42)

    stop = False

    def handle_sigint(signum, frame):
        nonlocal stop
        logger.info("Shutdown signal received; finishing current cycle")
        stop = True

    signal.signal(signal.SIGINT, handle_sigint)

    logger.info("Ingestion starting (poll every %ds)", POLL_INTERVAL_SEC)

    with (
        RotatingJsonlWriter(DATA_ROOT / "flight_states", "flight_states") as flight_writer,
        RotatingJsonlWriter(DATA_ROOT / "telemetry", "telemetry") as telemetry_writer,
    ):
        while not stop:
            cycle_start = time.monotonic()

            states = client.fetch_states()
            logger.info("Fetched %d airborne aircraft", len(states))

            anomaly_count = 0
            for state in states:
                flight_writer.write(state.to_dict())
                reading = generator.generate(state)
                if reading.is_anomaly:
                    anomaly_count += 1
                telemetry_writer.write(reading.to_dict())

            if anomaly_count:
                logger.info("%d anomaly readings written this cycle", anomaly_count)

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0.0, POLL_INTERVAL_SEC - elapsed)
            if not stop and sleep_for > 0:
                time.sleep(sleep_for)

    logger.info("Ingestion stopped")


if __name__ == "__main__":
    main()