"""Walk-forward validation for chronological fight prediction.

Trains on an expanding window, predicts the next chunk of fights,
then advances. All fold boundaries snap to event-date boundaries
so same-day fights are never split across train/test.
"""
import numpy as np
import pandas as pd

from .evaluation import pipeline


def align_to_event(dates, raw_index):
    """Snap an index to the start of its event date.

    If raw_index falls mid-event, moves back to the first fight on that date.
    If raw_index exceeds the series length, returns len(dates).
    """
    if raw_index >= len(dates):
        return len(dates)
    target_date = dates.iloc[raw_index]
    return int((dates < target_date).sum())


def walk_forward_predict(df, y, x, decimal, estimator_factory,
                          min_train=4000, step=200):
    """Run walk-forward validation with expanding training window.

    Args:
        df: DataFrame with 'date' column, sorted chronologically.
        y: Series of labels (0=Red, 1=Blue).
        x: Feature DataFrame (same index as df).
        decimal: Array of decimal odds, shape (n, 2).
        estimator_factory: Callable returning a fresh sklearn estimator.
        min_train: Minimum number of training rows before first prediction.
        step: Approximate number of fights per fold (snapped to event dates).

    Returns:
        (probabilities, y_true, decimal_odds) arrays for all predicted rows.
    """
    dates = df['date']
    n = len(df)
    train_end = align_to_event(dates, min_train)

    all_proba = []
    all_y = []
    all_decimal = []

    while train_end < n:
        test_end = align_to_event(dates, train_end + step)
        if test_end <= train_end:
            test_end = n

        model = pipeline(estimator_factory(), x.columns)
        model.fit(x.iloc[:train_end], y.iloc[:train_end])

        proba = model.predict_proba(x.iloc[train_end:test_end])
        all_proba.append(proba)
        all_y.append(y.iloc[train_end:test_end].values)
        all_decimal.append(decimal[train_end:test_end])

        train_end = test_end

    return (
        np.concatenate(all_proba),
        np.concatenate(all_y),
        np.concatenate(all_decimal),
    )
