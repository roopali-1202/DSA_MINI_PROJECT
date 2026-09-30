"""Milestone 1 consumer: print Kafka events without database or ML."""

from __future__ import annotations

import json
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError

try:  # Supports both `python src/consumer.py` and `python -m src.consumer`.
    from .config import (
        KAFKA_AUTO_OFFSET_RESET,
        KAFKA_GROUP_ID,
        KAFKA_TOPIC,
        LOG_LEVEL,
        bootstrap_servers,
    )
except ImportError:
    from config import (
        KAFKA_AUTO_OFFSET_RESET,
        KAFKA_GROUP_ID,
        KAFKA_TOPIC,
        LOG_LEVEL,
        bootstrap_servers,
    )


logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=bootstrap_servers(),
            group_id=f"{KAFKA_GROUP_ID}-terminal",
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
            enable_auto_commit=True,
            value_deserializer=lambda value: value,
        )
    except KafkaError as exc:
        LOGGER.error("Terminal consumer cannot connect to Kafka: %s", exc)
        return
    LOGGER.info("Waiting for events on '%s'", KAFKA_TOPIC)
    try:
        for message in consumer:
            try:
                print(json.loads(message.value.decode("utf-8")), flush=True)
            except (UnicodeDecodeError, json.JSONDecodeError):
                LOGGER.warning("Skipped malformed Kafka event")
    except KeyboardInterrupt:
        LOGGER.info("Terminal consumer stopped")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
