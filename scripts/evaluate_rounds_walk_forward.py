"""Expanding-window walk-forward evaluation of the round-duration models."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import HistGradientBoostingClassifier
from threadpoolctl import threadpool_limits

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.train_rounds_model import load_dataset, elapsed_seconds
from models.comparisons.evaluation import pipeline, split_index
from models.rounds.duration import HalfRoundRegressor, HalfRoundSurvivalClassifier, duration_metrics


def walk_forward_windows(dates, initial_train, step):
    """Keep dates intact; advance at least one event even when step is small."""
    dates = pd.Series(pd.to_datetime(dates)).reset_index(drop=True)
    if dates.isna().any() or not dates.is_monotonic_increasing:
        raise ValueError('Walk-forward dates must be complete and sorted')
    if step < 1 or not 0 < initial_train < len(dates):
        raise ValueError('Need positive step and an initial training window before the last row')
    start = int((dates < dates.iloc[initial_train]).sum())
    if start == 0:
        raise ValueError('Initial training window has no earlier event dates')
    folds = []
    while start < len(dates):
        raw_end = min(start + step, len(dates))
        end = len(dates) if raw_end == len(dates) else int((dates < dates.iloc[raw_end]).sum())
        if end <= start:
            end = int((dates <= dates.iloc[start]).sum())
        folds.append((start, end))
        start = end
    return folds


def estimator_for(name, scheduled, iterations):
    if name.startswith('classifier'):
        return HistGradientBoostingClassifier(max_iter=iterations, max_leaf_nodes=15,
                                             learning_rate=.05, l2_regularization=2,
                                             early_stopping=False, random_state=0)
    if name == 'regression_duration':
        return HalfRoundRegressor(scheduled, iterations)
    return HalfRoundSurvivalClassifier(scheduled, iterations)


def evaluate(data_path, fights_path, output, fraction=.8, step=100, five_step=40, iterations=200):
    df, features, usable, coverage = load_dataset(data_path, fights_path)
    output.mkdir(parents=True, exist_ok=True)
    results, predictions, fold_records = {}, [], []
    for scheduled in (3, 5):
        mask = usable & df.number_of_rounds.eq(scheduled)
        subset = df.loc[mask].reset_index(drop=True)
        x = features.loc[mask].reset_index(drop=True)
        if len(subset) < 10 or subset.date.nunique() < 2:
            results[str(scheduled)] = {'skipped': 'Insufficient dated fights'}
            continue
        boundary = split_index(subset.date, fraction)
        windows = walk_forward_windows(subset.date, boundary, five_step if scheduled == 5 else step)
        classes = [f'{tick/2:.1f}' for tick in range(scheduled*2+1)]
        names = ['majority_baseline', 'median_baseline', 'classifier_base',
                 'classifier_duration', 'regression_duration', 'survival_duration']
        collected = {name: [] for name in names}
        records = []
        print(f'{scheduled}-round bouts: {len(windows)} folds, {len(subset)-boundary:,} evaluated fights', flush=True)
        for fold, (start, end) in enumerate(windows, 1):
            train = subset.iloc[:start]
            test = subset.iloc[start:end]
            if not train.date.max() < test.date.min():
                raise ValueError('Training/test event dates overlap')
            record = dict(fold=fold, train_rows=start, test_rows=end-start,
                          train_start=str(train.date.min().date()), train_end=str(train.date.max().date()),
                          test_start=str(test.date.min().date()), test_end=str(test.date.max().date()))
            records.append(record)
            y = train.rounds_last.map(lambda v: f'{v:.1f}')
            print(f'  Fold {fold}/{len(windows)}: train through {record["train_end"]}, '
                  f'test {record["test_start"]}–{record["test_end"]}', flush=True)
            for name in names:
                probabilities = None
                if name == 'majority_baseline':
                    predicted = np.full(len(test), float(y.value_counts().idxmax()))
                elif name == 'median_baseline':
                    predicted = np.full(len(test), np.ceil(train.rounds_last.median()*2)/2)
                else:
                    model_x = x[[c for c in x.columns if 'duration_' not in c]] if name == 'classifier_base' else x
                    estimator = estimator_for(name, scheduled, iterations)
                    if name.startswith('classifier') and y.nunique() == 1:
                        estimator = DummyClassifier(strategy='most_frequent')
                    model = pipeline(estimator, model_x.columns)
                    fit_y = train.finish_round - 1 + train.finish_time.map(elapsed_seconds)/300 if name == 'regression_duration' else y
                    model.fit(model_x.iloc[:start], fit_y)
                    predicted = model.predict(model_x.iloc[start:end]).astype(float)
                    if hasattr(model, 'predict_proba'):
                        raw = model.predict_proba(model_x.iloc[start:end])
                        probabilities = np.zeros((len(test), len(classes)))
                        for i, label in enumerate(model.classes_):
                            probabilities[:, classes.index(str(label))] = raw[:, i]
                    if fold == len(windows):
                        joblib.dump(model, output / f'rounds_{scheduled}_{name}_last_fold.joblib')
                metric = duration_metrics(test.rounds_last, predicted, probabilities, [float(c) for c in classes])
                fold_records.append(dict(number_of_rounds=scheduled, model=name, **record,
                                         **{key: metric[key] for key in ('accuracy', 'within_half_round_accuracy',
                                           'mean_absolute_error_rounds', 'log_loss', 'confidence_ece', 'survival_brier')}))
                rows = test[['id', 'date', 'r_name', 'b_name', 'number_of_rounds',
                             'finish_round', 'finish_time', 'rounds_last', 'gone_distance']].copy()
                rows['model'], rows['fold'], rows['train_through'] = name, fold, record['train_end']
                rows['prediction'] = predicted
                rows['absolute_error_rounds'] = np.abs(predicted-test.rounds_last.to_numpy())
                if probabilities is not None:
                    for i, label in enumerate(classes):
                        rows[f'p_{label}'] = probabilities[:, i]
                collected[name].append(rows)
        aggregate = {}
        for name, chunks in collected.items():
            combined = pd.concat(chunks, ignore_index=True)
            probabilities = combined[[f'p_{c}' for c in classes]].to_numpy() if f'p_{classes[0]}' in combined else None
            aggregate[name] = duration_metrics(combined.rounds_last, combined.prediction,
                                               probabilities, [float(c) for c in classes])
            predictions.append(combined)
            print(f'  {name}: exact {aggregate[name]["accuracy"]:.1%}; '
                  f'within 0.5 {aggregate[name]["within_half_round_accuracy"]:.1%}; '
                  f'MAE {aggregate[name]["mean_absolute_error_rounds"]:.3f}', flush=True)
        results[str(scheduled)] = dict(initial_train_rows=boundary, test_rows=len(subset)-boundary,
                                      folds=records, classes=classes, models=aggregate)
    if not predictions:
        raise ValueError('No schedules could be evaluated')
    metadata = dict(created_utc=datetime.now(timezone.utc).isoformat(), evaluation='Expanding-window walk-forward',
                    initial_train_fraction=fraction, step=step, five_round_step=five_step, iterations=iterations,
                    seed=0, coverage=coverage, feature_columns=features.columns.tolist(),
                    data_sha256=hashlib.sha256(data_path.read_bytes()).hexdigest(),
                    fights_sha256=hashlib.sha256(fights_path.read_bytes()).hexdigest(),
                    target='Ceiling elapsed rounds to 0.5; exact distance rule retained',
                    preprocessing='Fresh pipeline fitted only on each expanding training window',
                    history='Causal pre-fight history can incorporate earlier test results; same-date results excluded',
                    statistics_note='Existing exported statistics assumed pre-fight, not independently reconstructed',
                    saved_models='Last evaluation fold models; excludes last fold test outcomes, not fitted on all data')
    (output / 'results.json').write_text(json.dumps(dict(metadata=metadata, results=results), indent=2)+'\n')
    pd.concat(predictions, ignore_index=True).to_csv(output / 'predictions.csv', index=False)
    pd.DataFrame(fold_records).to_csv(output / 'fold_metrics.csv', index=False)
    lines = ['# Walk-forward round-duration evaluation', '',
             f'Initial training fraction: {fraction:.0%}. Steps: {step} three-round bouts / {five_step} five-round bouts.',
             'Event dates stay intact. All metrics pool one out-of-sample prediction per evaluated fight and model.', '',
             '| Rounds | Model | Exact | Within 0.5 | MAE | Log loss | Confidence ECE | Survival Brier |',
             '|---:|---|---:|---:|---:|---:|---:|---:|']
    for scheduled, schedule in results.items():
        for name, metrics in schedule.get('models', {}).items():
            values = ['—' if metrics[k] is None else f'{metrics[k]:.4f}' for k in ('log_loss','confidence_ece','survival_brier')]
            lines.append(f'| {scheduled} | {name} | {metrics["accuracy"]:.1%} | '
                         f'{metrics["within_half_round_accuracy"]:.1%} | {metrics["mean_absolute_error_rounds"]:.3f} | '
                         + ' | '.join(values) + ' |')
    lines += ['', 'Models use the same configurations as the chronological holdout. No tuning uses evaluation outcomes.',
              'Last-fold saved models exclude that fold’s test outcomes. Existing exported statistics are assumed pre-fight.', '']
    (output / 'report.md').write_text('\n'.join(lines))
    print(f'Saved walk-forward results to {output}', flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, default=ROOT/'final-dataset-names-and-ids.csv')
    parser.add_argument('--fights', type=Path, default=ROOT/'fights.csv')
    parser.add_argument('--output', type=Path, default=ROOT/'models/rounds/walk_forward')
    parser.add_argument('--initial-train-fraction', type=float, default=.8)
    parser.add_argument('--step', type=int, default=100)
    parser.add_argument('--five-round-step', type=int, default=40)
    parser.add_argument('--iterations', type=int, default=200)
    args = parser.parse_args()
    if not 0 < args.initial_train_fraction < 1 or min(args.step,args.five_round_step,args.iterations) < 1:
        parser.error('Fraction must be between 0 and 1; steps and iterations must be positive')
    try:
        with threadpool_limits(limits=1):
            evaluate(args.data,args.fights,args.output,args.initial_train_fraction,
                     args.step,args.five_round_step,args.iterations)
    except (ValueError,OSError) as error:
        parser.error(str(error))


if __name__ == '__main__':
    main()
