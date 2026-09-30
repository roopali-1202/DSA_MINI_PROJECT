"""Live Streamlit dashboard for smart-meter metrics and anomalies."""

from __future__ import annotations

import logging

import pandas as pd
import streamlit as st

try:  # Supports both `streamlit run src/dashboard.py` and package imports.
    from .config import DASHBOARD_REFRESH_SECONDS, DASHBOARD_ROW_LIMIT, LOG_LEVEL
    from .database import fetch_latest_metrics, fetch_recent_readings
except ImportError:
    from config import DASHBOARD_REFRESH_SECONDS, DASHBOARD_ROW_LIMIT, LOG_LEVEL
    from database import fetch_latest_metrics, fetch_recent_readings


logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    st.set_page_config(page_title="Smart Meter Live Analytics", layout="wide")
    st.markdown(
        f"<meta http-equiv='refresh' content='{DASHBOARD_REFRESH_SECONDS}'>",
        unsafe_allow_html=True,
    )
    st.title("⚡ Smart-Meter Real-Time Streaming Analytics")
    st.caption(
        f"The page re-queries MySQL every {DASHBOARD_REFRESH_SECONDS} seconds. "
        "Start the producer to watch new readings arrive."
    )

    try:
        metrics = fetch_latest_metrics()
        recent = fetch_recent_readings(DASHBOARD_ROW_LIMIT)
    except Exception as exc:  # Dashboard should show an actionable error instead of crashing.
        LOGGER.exception("Dashboard database read failed")
        st.error(f"Could not read MySQL: {exc}")
        st.info("Check that MySQL is running, .env is configured, and sql/setup.sql was applied.")
        return

    if metrics is None:
        st.warning("No metrics exist yet. Start the processor and producer.")
        metrics = {
            "current_power": 0,
            "rolling_avg": 0,
            "peak_power": 0,
            "total_energy": 0,
            "anomaly_count": 0,
            "updated_at": None,
        }

    columns = st.columns(5)
    columns[0].metric("Current Power (kW)", f"{float(metrics['current_power'] or 0):.3f}")
    columns[1].metric("30-Reading Average (kW)", f"{float(metrics['rolling_avg'] or 0):.3f}")
    columns[2].metric("Peak Power (kW)", f"{float(metrics['peak_power'] or 0):.3f}")
    columns[3].metric("Total Energy (kWh)", f"{float(metrics['total_energy'] or 0):.3f}")
    columns[4].metric("Anomaly Count", int(metrics["anomaly_count"] or 0))

    if recent.empty:
        st.info("No readings have been stored yet.")
        return

    recent["is_anomaly"] = recent["is_anomaly"].astype(int)
    chart_data = recent.set_index("reading_time")[
        ["global_active_power", "rolling_avg_30"]
    ].rename(
        columns={
            "global_active_power": "Power (kW)",
            "rolling_avg_30": "Rolling average (kW)",
        }
    )
    st.subheader("Live power consumption")
    st.line_chart(chart_data, use_container_width=True)

    anomalies = recent[recent["is_anomaly"] == 1].copy()
    if not anomalies.empty:
        st.subheader("Detected anomalies")
        st.caption("Red points are readings classified as unusual by Isolation Forest.")
        st.scatter_chart(
            anomalies,
            x="reading_time",
            y="global_active_power",
            color="#ff0000",
            use_container_width=True,
        )
        st.dataframe(
            anomalies.sort_values("reading_time", ascending=False).head(20)[
                ["reading_time", "global_active_power", "rolling_avg_30", "anomaly_score"]
            ],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No anomalies in the latest window.")

    st.subheader("Latest readings")
    display_frame = recent.sort_values("reading_time", ascending=False).head(20).copy()
    st.dataframe(
        display_frame[
            [
                "reading_time",
                "global_active_power",
                "rolling_avg_30",
                "is_anomaly",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
    if metrics.get("updated_at") is not None:
        st.caption(f"Metrics last updated: {metrics['updated_at']}")


if __name__ == "__main__":
    main()
