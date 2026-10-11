"""Train fight-length classifiers predicting 0.0, 0.5, ..., 5.0 rounds."""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import platform

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier
from threadpoolctl import threadpool_limits

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from models.comparisons.evaluation import make_features, pipeline, split_index
from models.rounds.duration import (elapsed_seconds, duration_features, duration_metrics,
                                   HalfRoundRegressor, HalfRoundSurvivalClassifier)


def build_round_features(history, raw_fights):
    """Use this same builder for training and upcoming rows (history sorted by date)."""
    raw_fights = raw_fights.rename(columns={'r_fighter_id': 'r_id', 'b_fighter_id': 'b_id'})
    return pd.concat([make_features(history), duration_features(history, raw_fights)], axis=1)


def ending_labels(fights):
    scheduled = pd.to_numeric(fights.number_of_rounds, errors='coerce')
    finished = pd.to_numeric(fights.finish_round, errors='coerce')
    elapsed = fights.finish_time.map(elapsed_seconds)
    valid = (scheduled.isin([3, 5]) & finished.between(1, scheduled)
             & (finished % 1 == 0) & elapsed.between(0, 300))
    labels = pd.Series(np.nan, index=fights.index, dtype=float)
    length = finished.loc[valid] - 1 + elapsed.loc[valid] / 300
    labels.loc[valid] = np.ceil(length * 2) / 2
    # User definition applies regardless of win method, including full-time draws.
    distance = valid & (finished == scheduled) & (elapsed == 300)
    labels.loc[distance] = scheduled.loc[distance]
    return labels


def load_dataset(data_path, fights_path):
    data = pd.read_csv(data_path)
    fights = pd.read_csv(fights_path)
    data['date'] = pd.to_datetime(data.date, errors='raise')
    fights['date'] = pd.to_datetime(fights.date, errors='raise')
    keys = ['date', 'r_id', 'b_id']
    fights = fights.rename(columns={'r_fighter_id': 'r_id', 'b_fighter_id': 'b_id'})
    if data[keys].isna().any().any():
        raise ValueError('Feature data has missing dates or fighter IDs')
    if data.duplicated(keys).any() or fights.duplicated(keys).any():
        raise ValueError('Ambiguous date/fighter matchup keys; cannot safely join outcomes')
    outcomes = fights[keys + ['id', 'number_of_rounds', 'finish_round', 'finish_time']].copy()
    outcomes['rounds_last'] = ending_labels(outcomes)
    outcomes['gone_distance'] = (outcomes.rounds_last.notna()
                                & outcomes.finish_round.eq(outcomes.number_of_rounds)
                                & outcomes.finish_time.map(elapsed_seconds).eq(300))
    joined = data.merge(outcomes, on=keys, how='left', validate='one_to_one')
    joined = joined.sort_values('date', kind='stable').reset_index(drop=True)
    # Build rest history before excluding unsupported or missing target rows.
    features = build_round_features(joined, fights)
    usable = joined.rounds_last.notna()
    return joined, features, usable, {
        'feature_rows': len(data), 'matched_outcomes': int(joined.id.notna().sum()),
        'usable_targets': int(usable.sum()), 'excluded_rows': int((~usable).sum()),
    }


def train_models(data_path, fights_path, output, fraction=.8, iterations=200):
    df, features, usable, coverage = load_dataset(data_path, fights_path)
    output.mkdir(parents=True, exist_ok=True)
    metadata = {
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'distance_rule': 'finish_round == number_of_rounds and elapsed finish_time == 300 seconds',
        'time_formats': ['MM:SS:00 (legacy)', 'MM:SS'],
        'target': 'elapsed rounds rounded up to the next 0.5 increment',
        'rounding_formula': 'ceil(2 * (finish_round - 1 + elapsed_seconds / 300)) / 2',
        'feature_columns': features.columns.tolist(), 'coverage': coverage,
        'data_sha256': hashlib.sha256(data_path.read_bytes()).hexdigest(),
        'fights_sha256': hashlib.sha256(fights_path.read_bytes()).hexdigest(),
        'train_fraction': fraction, 'seed': 0,
        'versions': {'python': platform.python_version(), 'sklearn': sklearn.__version__,
                     'pandas': pd.__version__, 'numpy': np.__version__},
        'feature_note': 'Exported statistics assumed pre-fight. Duration history uses only earlier event dates from raw fights, including rows outside the feature export.',
        'evaluation': 'Chronological holdout, no hyperparameter tuning or walk-forward evaluation',
        'duration_history': 'Lifetime and recent five bouts; opponent residual uses opponent mean available before that bout',
    }
    results, predictions = {}, []
    for scheduled in (3, 5):
        mask = usable & df.number_of_rounds.eq(scheduled)
        subset = df.loc[mask].reset_index(drop=True)
        x = features.loc[mask].reset_index(drop=True)
        if len(subset) < 10 or subset.date.nunique() < 2:
            results[str(scheduled)] = {'skipped': 'Insufficient dated fights'}
            continue
        boundary = split_index(subset.date, fraction)
        # String labels let sklearn classify half-rounds rather than treat them as continuous targets.
        y = subset.rounds_last.map(lambda value: f'{value:.1f}')
        expected_classes = [f'{tick / 2:.1f}' for tick in range(scheduled * 2 + 1)]
        if y.iloc[:boundary].nunique() < 2:
            results[str(scheduled)] = {'skipped': 'Training period has fewer than two outcomes'}
            continue
        base_columns = [c for c in x.columns if 'duration_' not in c]
        specs = {
            'classifier_base': (HistGradientBoostingClassifier, features.loc[mask, base_columns].reset_index(drop=True)),
            'classifier_duration': (HistGradientBoostingClassifier, x),
            'regression_duration': (HalfRoundRegressor, x),
            'survival_duration': (HalfRoundSurvivalClassifier, x),
        }
        train_y = y.iloc[:boundary]
        actual = subset.rounds_last.iloc[boundary:].to_numpy()
        majority = float(train_y.value_counts().idxmax())
        median = float(np.ceil(subset.rounds_last.iloc[:boundary].median()*2)/2)
        common = {
            'train_rows': boundary, 'test_rows': len(actual),
            'train_dates': [str(subset.date.iloc[0].date()), str(subset.date.iloc[boundary-1].date())],
            'test_dates': [str(subset.date.iloc[boundary].date()), str(subset.date.iloc[-1].date())],
            'classes': expected_classes,
        }
        schedule_results = dict(common, models={})
        for name, constant in [('majority_baseline', majority), ('median_baseline', median)]:
            schedule_results['models'][name] = duration_metrics(
                actual, np.full(len(actual), constant), classes=[float(c) for c in expected_classes])
        for name, (kind, model_x) in specs.items():
            print(f'Training {scheduled}-round {name}: {boundary:,} train / '
                  f'{len(actual):,} test fights...', flush=True)
            if kind == HistGradientBoostingClassifier:
                estimator = kind(max_iter=iterations, max_leaf_nodes=15, learning_rate=.05,
                                 l2_regularization=2, early_stopping=False, random_state=0)
            else:
                estimator = kind(scheduled_rounds=scheduled, max_iter=iterations)
            model = pipeline(estimator, model_x.columns)
            fit_y = train_y
            if kind == HalfRoundRegressor:
                train_rows = subset.iloc[:boundary]
                fit_y = train_rows.finish_round - 1 + train_rows.finish_time.map(elapsed_seconds)/300
            model.fit(model_x.iloc[:boundary], fit_y)
            predicted = model.predict(model_x.iloc[boundary:]).astype(float)
            aligned = None
            if hasattr(model, 'predict_proba'):
                probabilities = model.predict_proba(model_x.iloc[boundary:])
                aligned = np.zeros((len(actual), len(expected_classes)))
                for index, label in enumerate(model.classes_):
                    aligned[:, expected_classes.index(str(label))] = probabilities[:, index]
            metrics = duration_metrics(actual, predicted, aligned, [float(c) for c in expected_classes])
            metrics['parameters'] = estimator.get_params()
            metrics['feature_columns'] = model_x.columns.tolist()
            schedule_results['models'][name] = metrics
            result = subset.iloc[boundary:][['id', 'date', 'r_name', 'b_name', 'number_of_rounds',
                                             'finish_round', 'finish_time', 'rounds_last', 'gone_distance']].copy()
            result['model'] = name
            result['prediction'] = predicted
            result['absolute_error_rounds'] = np.abs(predicted-actual)
            if aligned is not None:
                for index, label in enumerate(expected_classes):
                    result[f'p_{label}'] = aligned[:, index]
            predictions.append(result)
            joblib.dump(model, output / f'rounds_{scheduled}_{name}.joblib')
            if name == 'classifier_duration':
                joblib.dump(model, output / f'rounds_{scheduled}.joblib')
            print(f'  Exact: {metrics["accuracy"]:.1%}; within 0.5: '
                  f'{metrics["within_half_round_accuracy"]:.1%}; MAE: '
                  f'{metrics["mean_absolute_error_rounds"]:.3f} rounds')
        results[str(scheduled)] = schedule_results
    if not predictions:
        raise ValueError('No models trained; check target coverage and event dates')
    pd.concat(predictions, ignore_index=True).to_csv(output / 'predictions.csv', index=False)
    (output / 'results.json').write_text(json.dumps({'metadata': metadata, 'results': results}, indent=2) + '\n')
    lines = ['# Round-duration model comparison', '',
             'Chronological holdout; targets and predictions use ceiling to half-rounds.', '',
             '| Scheduled rounds | Model | Exact accuracy | Within 0.5 | MAE (rounds) | Log loss | Confidence ECE | Survival Brier |',
             '|---:|---|---:|---:|---:|---:|---:|---:|']
    for scheduled, schedule in results.items():
        for name, values in schedule.get('models', {}).items():
            probability_values = ['—' if values[key] is None else f'{values[key]:.4f}'
                                  for key in ('log_loss', 'confidence_ece', 'survival_brier')]
            lines.append(f'| {scheduled} | {name} | {values["accuracy"]:.1%} | '
                         f'{values["within_half_round_accuracy"]:.1%} | '
                         f'{values["mean_absolute_error_rounds"]:.3f} | '
                         + ' | '.join(probability_values) + ' |')
    lines += ['', 'Lower MAE, log loss, ECE, and Brier are better. Regression produces point predictions;',
              'probability metrics are unavailable. Models are not selected or tuned using this holdout.', '']
    (output / 'report.md').write_text('\n'.join(lines))
    print(f'Target coverage: {coverage["usable_targets"]:,}/{coverage["feature_rows"]:,} fights; '
          f'{coverage["excluded_rows"]:,} excluded. Saved to {output}')
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, default=ROOT / 'final-dataset-names-and-ids.csv')
    parser.add_argument('--fights', type=Path, default=ROOT / 'fights.csv')
    parser.add_argument('--output', type=Path, default=ROOT / 'models/rounds/artifacts')
    parser.add_argument('--train-fraction', type=float, default=.8)
    parser.add_argument('--iterations', type=int, default=200)
    args = parser.parse_args()
    if not 0 < args.train_fraction < 1 or args.iterations < 1:
        parser.error('Train fraction must be between 0 and 1; iterations must be positive')
    try:
        with threadpool_limits(limits=1):
            train_models(args.data, args.fights, args.output, args.train_fraction, args.iterations)
    except (ValueError, OSError) as error:
        parser.error(str(error))


if __name__ == '__main__':
    main()
