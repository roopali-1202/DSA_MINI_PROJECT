"""Consume Kafka events, compute windowed metrics, score anomalies, and persist."""

from __future__ import annotations

import argparse
import json
import logging
import math
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError

try:  # Supports both `python src/processor.py` and `python -m src.processor`.
    from .config import (
        ACTIVE_POWER_COLUMN,
        ENERGY_INTERVAL_MINUTES,
        KAFKA_AUTO_OFFSET_RESET,
        KAFKA_GROUP_ID,
        KAFKA_TOPIC,
        LOG_LEVEL,
        MODEL_PATH,
        bootstrap_servers,
    )
    from .database import DatabaseWriter
except ImportError:
    from config import (
        ACTIVE_POWER_COLUMN,
        ENERGY_INTERVAL_MINUTES,
        KAFKA_AUTO_OFFSET_RESET,
        KAFKA_GROUP_ID,
        KAFKA_TOPIC,
        LOG_LEVEL,
        MODEL_PATH,
        bootstrap_servers,
    )
    from database import DatabaseWriter


logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger(__name__)


def decode_event(raw_value: bytes) -> dict[str, Any] | None:
    """Decode one event without allowing malformed JSON to kill the consumer."""

    try:
        event = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
        LOGGER.warning("Skipping malformed Kafka event")
        return None
    if not isinstance(event, dict):
        LOGGER.warning("Skipping Kafka event that is not a JSON object")
        return None
    return event


def clean_event(event: dict[str, Any]) -> tuple[datetime, float] | None:
    try:
        raw_time = str(event["reading_time"]).replace("Z", "+00:00")
        reading_time = datetime.fromisoformat(raw_time)
        if reading_time.tzinfo is not None:
            reading_time = reading_time.astimezone(timezone.utc).replace(tzinfo=None)
        power = float(event["global_active_power"])
    except (KeyError, TypeError, ValueError, OverflowError):
        LOGGER.warning("Skipping Kafka event with invalid fields: %s", event)
        return None
    if not math.isfinite(power) or power < 0:
        LOGGER.warning("Skipping Kafka event with invalid power: %s", event)
        return None
    return reading_time, power


def load_model(model_path: Path) -> Any:
    if not model_path.exists():
        raise FileNotFoundError(
            f"Model not found at {model_path}. Run 'python src/train_model.py' first."
        )
    return joblib.load(model_path)


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=bootstrap_servers(),
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        value_deserializer=lambda value: value,
        consumer_timeout_ms=-1,
    )


def run(model_path: Path = MODEL_PATH, disable_ml: bool = False) -> None:
    try:
        model = None if disable_ml else load_model(model_path)
    except FileNotFoundError as exc:
        LOGGER.error("Processor cannot start: %s", exc)
        return
    database = DatabaseWriter()
    consumer: KafkaConsumer | None = None
    window: deque[float] = deque(maxlen=30)
    peak_power = 0.0
    total_energy = 0.0
    anomaly_count = 0
    processed = 0

    try:
        restored = database.restore_state()
        metrics = restored["metrics"]
        window.extend(restored["recent_powers"])
        peak_power = float(metrics.get("peak_power") or 0.0)
        total_energy = float(metrics.get("total_energy") or 0.0)
        anomaly_count = int(metrics.get("anomaly_count") or 0)

        consumer = build_consumer()
        LOGGER.info("Processor listening on topic '%s'", KAFKA_TOPIC)

        for message in consumer:
            event = decode_event(message.value)
            if event is None:
                consumer.commit()
                continue
            cleaned = clean_event(event)
            if cleaned is None:
                consumer.commit()
                continue

            reading_time, power = cleaned
            window.append(power)
            rolling_average = sum(window) / len(window)
            peak_power = max(peak_power, power)
            total_energy += power * ENERGY_INTERVAL_MINUTES / 60.0

            prediction = 1
            anomaly_score: float | None = None
            if model is not None:
                features = pd.DataFrame([[power]], columns=[ACTIVE_POWER_COLUMN])
                prediction = int(model.predict(features)[0])  # 1 normal, -1 anomaly
                anomaly_score = float(model.decision_function(features)[0])
            is_anomaly = 1 if prediction == -1 else 0
            anomaly_count += is_anomaly

            database.write_processed_reading(
                reading_time=reading_time,
                power=power,
                rolling_average=rolling_average,
                is_anomaly=is_anomaly,
                anomaly_score=anomaly_score,
                peak_power=peak_power,
                total_energy=total_energy,
                anomaly_count=anomaly_count,
            )
            consumer.commit()
            processed += 1
            if processed == 1 or processed % 10 == 0:
                LOGGER.info(
                    "Processed %d | power=%.3f kW | rolling=%.3f | peak=%.3f | anomalies=%d",
                    processed,
                    power,
                    rolling_average,
                    peak_power,
                    anomaly_count,
                )
    except KeyboardInterrupt:
        LOGGER.info("Shutdown requested")
    except KafkaError:
        LOGGER.exception("Kafka connection or consumer error")
    except RuntimeError as exc:
        LOGGER.error("Processor stopped after a database failure: %s", exc)
    except Exception as exc:
        LOGGER.exception("Processor stopped because of an unexpected error: %s", exc)
    finally:
        if consumer is not None:
            consumer.close()
        database.close()
        LOGGER.info("Processor stopped after processing %d events", processed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, default=MODEL_PATH)
    parser.add_argument(
        "--disable-ml",
        action="store_true",
        help="Milestone 2 option: persist metrics without loading the model",
    )
    args = parser.parse_args()
    run(args.model, args.disable_ml)


if __name__ == "__main__":
    main()
