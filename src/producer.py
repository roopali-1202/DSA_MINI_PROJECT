"""Replay cleaned CSV rows into Kafka as a paced JSON event stream."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from kafka import KafkaProducer
from kafka.errors import KafkaError

try:  # Supports both `python src/producer.py` and `import src.producer`.
    from .config import (
        ACTIVE_POWER_COLUMN,
        CSV_PATH,
        KAFKA_TOPIC,
        LOG_LEVEL,
        READING_TIME_COLUMN,
        STREAM_DELAY,
        bootstrap_servers,
    )
except ImportError:
    from config import (
        ACTIVE_POWER_COLUMN,
        CSV_PATH,
        KAFKA_TOPIC,
        LOG_LEVEL,
        READING_TIME_COLUMN,
        STREAM_DELAY,
        bootstrap_servers,
    )


logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger(__name__)


def parse_power(raw_value: object) -> float | None:
    if raw_value is None or str(raw_value).strip() in {"", "?"}:
        return None
    try:
        value = float(str(raw_value).strip())
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return value


def parse_reading_time(raw_value: object) -> str | None:
    if raw_value is None or not str(raw_value).strip():
        return None
    try:
        value = str(raw_value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def valid_events(csv_path: Path) -> Iterator[dict[str, object]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV dataset not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")
        required = {READING_TIME_COLUMN, ACTIVE_POWER_COLUMN}
        missing = required.difference(reader.fieldnames)
        if missing:
            raise ValueError(f"CSV is missing required columns: {sorted(missing)}")

        for row_number, row in enumerate(reader, start=2):
            reading_time = parse_reading_time(row.get(READING_TIME_COLUMN))
            power = parse_power(row.get(ACTIVE_POWER_COLUMN))
            if reading_time is None or power is None:
                LOGGER.warning("Skipping invalid CSV row %d", row_number)
                continue
            yield {
                "reading_time": reading_time,
                "global_active_power": power,
            }


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers(),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
        request_timeout_ms=10000,
    )


def produce(csv_path: Path = CSV_PATH, delay: float = STREAM_DELAY, max_rows: int | None = None) -> int:
    producer = build_producer()
    sent = 0
    try:
        for event in valid_events(csv_path):
            if max_rows is not None and sent >= max_rows:
                break
            try:
                producer.send(KAFKA_TOPIC, value=event).get(timeout=10)
            except KafkaError:
                LOGGER.exception("Kafka send failed; stopping producer")
                raise
            sent += 1
            if sent == 1 or sent % 10 == 0:
                LOGGER.info("Sent %d: %s", sent, event)
            if delay > 0:
                time.sleep(delay)
    finally:
        producer.flush()
        producer.close()
        LOGGER.info("Producer stopped after sending %d events", sent)
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=CSV_PATH)
    parser.add_argument("--delay", type=float, default=STREAM_DELAY)
    parser.add_argument("--max-rows", type=int, default=None)
    args = parser.parse_args()
    try:
        produce(args.csv, max(0.0, args.delay), args.max_rows)
    except (FileNotFoundError, ValueError, KafkaError) as exc:
        LOGGER.error("Producer stopped: %s", exc)


if __name__ == "__main__":
    main()
