"""Train the offline Isolation Forest anomaly detector."""

from __future__ import annotations

import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

try:  # Supports both `python src/train_model.py` and `python -m src.train_model`.
    from .config import ACTIVE_POWER_COLUMN, CSV_PATH, LOG_LEVEL, MODEL_PATH
except ImportError:
    from config import ACTIVE_POWER_COLUMN, CSV_PATH, LOG_LEVEL, MODEL_PATH


logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger(__name__)


def load_training_data(csv_path: Path) -> pd.DataFrame:
    """Load and clean the single feature used by the model."""

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV dataset not found: {csv_path}")

    try:
        frame = pd.read_csv(csv_path, usecols=[ACTIVE_POWER_COLUMN])
    except ValueError as exc:
        raise ValueError(
            f"CSV must contain a '{ACTIVE_POWER_COLUMN}' column"
        ) from exc

    values = pd.to_numeric(frame[ACTIVE_POWER_COLUMN], errors="coerce")
    values = values.replace([np.inf, -np.inf], np.nan).dropna()
    values = values[values >= 0]
    cleaned = pd.DataFrame({ACTIVE_POWER_COLUMN: values})
    if len(cleaned) < 10:
        raise ValueError("At least 10 valid power readings are needed for training")
    return cleaned


def train_and_save(csv_path: Path = CSV_PATH, model_path: Path = MODEL_PATH) -> None:
    data = load_training_data(csv_path)

    # Isolation Forest isolates unusual points using random decision trees.
    # It is useful here because smart-meter data does not need hand-labelled
    # anomaly examples.  contamination=0.01 says that about 1% of the
    # training readings are expected to be unusual.  Training happens once
    # offline; the live processor only loads and scores the saved artifact.
    model = IsolationForest(contamination=0.01, random_state=42)
    model.fit(data[[ACTIVE_POWER_COLUMN]])

    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)
    LOGGER.info("Saved model to %s using %d clean readings", model_path, len(data))


if __name__ == "__main__":
    train_and_save()
