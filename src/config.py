"""Central configuration for the smart-meter streaming project.

All credentials and runtime endpoints come from environment variables.  The
defaults are deliberately suitable for a single-machine Windows demo.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def _project_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT_DIR / path


def bootstrap_servers() -> list[str]:
    """Return Kafka bootstrap servers as a list accepted by kafka-python."""

    return [
        server.strip()
        for server in _env("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092").split(",")
        if server.strip()
    ]


DATA_DIR = ROOT_DIR / "data"
MODEL_DIR = ROOT_DIR / "models"
CSV_PATH = _project_path(_env("CSV_PATH", "data/household_power.csv"))
MODEL_PATH = _project_path(_env("MODEL_PATH", "models/anomaly_model.pkl"))

ACTIVE_POWER_COLUMN = "Global_active_power"
READING_TIME_COLUMN = "Date_Time"

KAFKA_TOPIC = _env("KAFKA_TOPIC", "smartmeter-events")
KAFKA_GROUP_ID = _env("KAFKA_GROUP_ID", "smartmeter-processor")
KAFKA_AUTO_OFFSET_RESET = _env("KAFKA_AUTO_OFFSET_RESET", "earliest")
STREAM_DELAY = _env_float("STREAM_DELAY", 0.2)

MYSQL_HOST = _env("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = _env_int("MYSQL_PORT", 3306)
MYSQL_USER = _env("MYSQL_USER", "meter_app")
MYSQL_PASSWORD = _env("MYSQL_PASSWORD", "meter_pass")
MYSQL_DATABASE = _env("MYSQL_DATABASE", "smartmeter")

DASHBOARD_REFRESH_SECONDS = _env_int("DASHBOARD_REFRESH_SECONDS", 2)
DASHBOARD_ROW_LIMIT = _env_int("DASHBOARD_ROW_LIMIT", 200)
ENERGY_INTERVAL_MINUTES = _env_float("ENERGY_INTERVAL_MINUTES", 1.0)
LOG_LEVEL = _env("LOG_LEVEL", "INFO").upper()


def mysql_connection_config() -> dict[str, object]:
    """Build a mysql-connector connection dictionary without exposing secrets."""

    return {
        "host": MYSQL_HOST,
        "port": MYSQL_PORT,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "database": MYSQL_DATABASE,
        "connection_timeout": 10,
    }
