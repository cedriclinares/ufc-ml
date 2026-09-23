"""Shared chronological features, preprocessing, and flat-stake evaluation."""
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, brier_score_loss, confusion_matrix, log_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .features import BASE_FEATURES, RATIOS, DIFFERENCES


def split_index(dates, fraction=0.8):
    """Move the split to the start of its event date."""
    cutoff = dates.iloc[int(len(dates) * fraction)]
    index = int((dates < cutoff).sum())
    if not 0 < index < len(dates):
        raise ValueError('Insufficient distinct dates for chronological split')
    return index


def make_features(df):
    x = df[BASE_FEATURES].copy()
    history = {}
    rest = {corner: [] for corner in ('r', 'b')}
    for date, group in df.groupby('date', sort=False):
        for corner in ('r', 'b'):
            for fighter in group[f'{corner}_id']:
                rest[corner].append((date - history[fighter]).days if fighter in history else np.nan)
        for corner in ('r', 'b'):
            for fighter in group[f'{corner}_id'].dropna():
                history[fighter] = date
    derived = {}
    for corner in ('r', 'b'):
        for name in RATIOS:
            base = name.removesuffix('_accuracy')
            derived[f'{corner}_{name}'] = x[f'{corner}_{base}_landed'] / x[f'{corner}_{base}_attempted'].replace(0, np.nan)
        derived[f'{corner}_days_since_last_fight'] = rest[corner]
        derived[f'{corner}_is_debut'] = np.isnan(rest[corner]).astype(float)
    for name in DIFFERENCES:
        base = name.removesuffix('_diff')
        derived[f'r_{name}'] = x[f'r_{base}'] - x[f'b_{base}']
    return pd.concat([x, pd.DataFrame(derived, index=x.index)], axis=1).replace([np.inf, -np.inf], np.nan)


def pipeline(estimator, columns):
    categorical = [c for c in ('gender', 'r_stance', 'b_stance') if c in columns]
    numeric = [c for c in columns if c not in categorical]
    preprocess = ColumnTransformer([
        ('numeric', make_pipeline(SimpleImputer(strategy='median', keep_empty_features=True), StandardScaler()), numeric),
        ('categorical', make_pipeline(SimpleImputer(strategy='constant', fill_value='Unknown'), OneHotEncoder(handle_unknown='ignore', sparse_output=False)), categorical),
    ])
    return make_pipeline(preprocess, estimator)


def decimal_odds(df):
    odds = df[['r_fighter_odds', 'b_fighter_odds']].to_numpy(float)
    if not np.isfinite(odds).all() or (np.abs(odds) < 100).any():
        raise ValueError('Comparison requires valid American odds for both fighters on every row')
    return 1 + np.where(odds > 0, odds / 100, 100 / np.abs(odds))


def evaluate(y, probabilities, decimal):
    probabilities = np.asarray(probabilities)
    predicted = probabilities.argmax(axis=1)  # Red wins exact ties.
    selected = decimal[np.arange(len(y)), predicted]
    profits = np.where(predicted == np.asarray(y), selected - 1, -1)
    p_blue = probabilities[:, 1]
    bins = []
    for i in range(10):
        mask = (p_blue >= i / 10) & ((p_blue < (i + 1) / 10) if i < 9 else (p_blue <= 1))
        if mask.any():
            bins.append({'lower': i / 10, 'count': int(mask.sum()), 'mean_probability': float(p_blue[mask].mean()), 'blue_frequency': float(np.asarray(y)[mask].mean())})
    return {
        'test_accuracy': accuracy_score(y, predicted),
        'confusion_matrix': confusion_matrix(y, predicted, labels=[0, 1]).tolist(),
        'brier_score': brier_score_loss(y, p_blue),
        'log_loss': log_loss(y, probabilities, labels=[0, 1]),
        'ece': sum(b['count'] * abs(b['mean_probability'] - b['blue_frequency']) for b in bins) / len(y),
        'calibration_bins': bins, 'bets': len(y), 'betting_units': float(profits.sum()),
        'roi': float(profits.mean()),
    }, predicted, profits
