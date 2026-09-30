"""MySQL access shared by the processor and Streamlit dashboard."""

from __future__ import annotations

import logging
from typing import Any

import mysql.connector
import pandas as pd
from mysql.connector import Error

try:  # Supports both `python src/file.py` and `import src.database`.
    from .config import mysql_connection_config
except ImportError:
    from config import mysql_connection_config


LOGGER = logging.getLogger(__name__)


class DatabaseWriter:
    """Keep one MySQL connection open while the stream processor is running."""

    def __init__(self) -> None:
        self.connection: mysql.connector.MySQLConnection | None = None

    def connect(self) -> None:
        if self.connection is not None and self.connection.is_connected():
            return
        self.connection = mysql.connector.connect(**mysql_connection_config())

    def close(self) -> None:
        if self.connection is not None and self.connection.is_connected():
            self.connection.close()
        self.connection = None

    def restore_state(self) -> dict[str, Any]:
        """Restore the latest metrics and window values after a restart."""

        self.connect()
        assert self.connection is not None
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(
                """
                SELECT current_power, rolling_avg, peak_power,
                       total_energy, anomaly_count
                FROM meter_metrics
                WHERE metrics_pk = 1
                """
            )
            metrics = cursor.fetchone() or {
                "current_power": 0.0,
                "rolling_avg": 0.0,
                "peak_power": 0.0,
                "total_energy": 0.0,
                "anomaly_count": 0,
            }

            cursor.execute(
                """
                SELECT global_active_power
                FROM meter_readings
                ORDER BY reading_pk DESC
                LIMIT 30
                """
            )
            powers = [float(row["global_active_power"]) for row in cursor.fetchall()]
            powers.reverse()
            return {"metrics": metrics, "recent_powers": powers}
        finally:
            cursor.close()

    def write_processed_reading(
        self,
        *,
        reading_time: Any,
        power: float,
        rolling_average: float,
        is_anomaly: int,
        anomaly_score: float | None,
        peak_power: float,
        total_energy: float,
        anomaly_count: int,
    ) -> None:
        """Insert one reading and update the singleton current-state row.

        A failed transaction is retried once after reconnecting.  Both SQL
        statements are parameterized, and they commit together.
        """

        last_error: Exception | None = None
        for attempt in range(2):
            try:
                self.connect()
                assert self.connection is not None
                cursor = self.connection.cursor()
                try:
                    cursor.execute(
                        """
                        INSERT INTO meter_readings
                            (reading_time, global_active_power,
                             rolling_avg_30, is_anomaly, anomaly_score)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            reading_time,
                            power,
                            rolling_average,
                            is_anomaly,
                            anomaly_score,
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO meter_metrics
                            (metrics_pk, current_power, rolling_avg,
                             peak_power, total_energy, anomaly_count)
                        VALUES (1, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            current_power = VALUES(current_power),
                            rolling_avg = VALUES(rolling_avg),
                            peak_power = VALUES(peak_power),
                            total_energy = VALUES(total_energy),
                            anomaly_count = VALUES(anomaly_count)
                        """,
                        (
                            power,
                            rolling_average,
                            peak_power,
                            total_energy,
                            anomaly_count,
                        ),
                    )
                    self.connection.commit()
                finally:
                    cursor.close()
                return
            except Error as exc:
                last_error = exc
                LOGGER.error(
                    "MySQL write failed (attempt %d/2): %s", attempt + 1, exc
                )
                if self.connection is not None:
                    try:
                        self.connection.rollback()
                    except Error:
                        pass
                self.close()

        raise RuntimeError("Unable to write the processed reading to MySQL") from last_error


def fetch_latest_metrics() -> dict[str, Any] | None:
    """Read the singleton metrics row for one dashboard refresh."""

    connection = mysql.connector.connect(**mysql_connection_config())
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT current_power, rolling_avg, peak_power,
                   total_energy, anomaly_count, updated_at
            FROM meter_metrics
            WHERE metrics_pk = 1
            """
        )
        return cursor.fetchone()
    finally:
        cursor.close()
        connection.close()


def fetch_recent_readings(limit: int = 200) -> pd.DataFrame:
    """Read only the latest rows, then restore chronological plot order."""

    safe_limit = max(1, min(int(limit), 1000))
    connection = mysql.connector.connect(**mysql_connection_config())
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT reading_pk, reading_time, global_active_power,
                   rolling_avg_30, is_anomaly, anomaly_score
            FROM meter_readings
            ORDER BY reading_pk DESC
            LIMIT %s
            """,
            (safe_limit,),
        )
        rows = cursor.fetchall()
        columns = [
            "reading_pk",
            "reading_time",
            "global_active_power",
            "rolling_avg_30",
            "is_anomaly",
            "anomaly_score",
        ]
        frame = pd.DataFrame(rows, columns=columns)
        if not frame.empty:
            frame["reading_time"] = pd.to_datetime(frame["reading_time"])
            frame = frame.sort_values("reading_time").reset_index(drop=True)
        return frame
    finally:
        cursor.close()
        connection.close()
