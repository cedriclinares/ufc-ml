"""Run with: python -m models.comparisons.train (from the repository root)."""
import argparse
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.calibration import CalibratedClassifierCV
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
from sklearn.frozen import FrozenEstimator
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
from threadpoolctl import threadpool_limits

from .evaluation import decimal_odds, evaluate, make_features, pipeline, split_index

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, default=ROOT / 'final-dataset-names-and-ids.csv')
    parser.add_argument('--output', type=Path, default=ROOT / 'models/comparisons/artifacts')
    parser.add_argument('--report', type=Path, default=ROOT / 'model_results.md')
    args = parser.parse_args()
    df = pd.read_csv(args.data)
    df['date'] = pd.to_datetime(df['date'], errors='raise')
    if df.date.isna().any():
        raise ValueError('Missing fight date')
    df = df.sort_values('date', kind='stable').reset_index(drop=True)
    y = df.winner.map({'Red': 0, 'Blue': 1})
    if y.isna().any():
        raise ValueError('Expected only Red/Blue winners')
    y = y.astype(int)
    decimal = decimal_odds(df)
    implied = 1 / decimal
    market = implied / implied.sum(axis=1, keepdims=True)
    odds_features = pd.DataFrame(market, columns=['red_implied', 'blue_implied'])
    x = make_features(df)
    boundary = split_index(df.date)
    calibration_boundary = split_index(df.date.iloc[:boundary])
    args.output.mkdir(parents=True, exist_ok=True)
    results = {}
    specs = {
        'majority_class': (DummyClassifier(strategy='prior'), odds_features),
        'odds_only': (LogisticRegression(max_iter=2000, random_state=0), odds_features),
        'logistic_regression': (LogisticRegression(max_iter=2000, random_state=0), x),
        'random_forest': (RandomForestClassifier(n_estimators=300, min_samples_leaf=5, max_features='sqrt', random_state=0, n_jobs=1), x),
        'gradient_boosting': (HistGradientBoostingClassifier(max_iter=200, max_leaf_nodes=15, learning_rate=0.05, l2_regularization=1, early_stopping=False, random_state=0), x),
        'mlp': (MLPClassifier(hidden_layer_sizes=(100,), alpha=1, max_iter=1000, random_state=0), x),
    }
    predictions = df.iloc[boundary:][['date', 'r_id', 'b_id', 'winner']].copy()

    def record(name, p, train_accuracy):
        metrics, labels, profits = evaluate(y.iloc[boundary:], p, decimal[boundary:])
        metrics['train_accuracy'] = train_accuracy
        results[name] = metrics
        predictions[f'{name}_blue_probability'] = p[:, 1]
        predictions[f'{name}_prediction'] = labels
        predictions[f'{name}_units'] = profits
        print(f'{name}: accuracy={metrics["test_accuracy"]:.4f}, units={metrics["betting_units"]:+.2f}', flush=True)

    record('betting_favorite', market[boundary:], float((market[:boundary].argmax(axis=1) == y.iloc[:boundary]).mean()))
    for name, (estimator, features) in specs.items():
        print(f'Training {name}...', flush=True)
        model = pipeline(estimator, features.columns)
        model.fit(features.iloc[:boundary], y.iloc[:boundary])
        record(name, model.predict_proba(features.iloc[boundary:]), model.score(features.iloc[:boundary], y.iloc[:boundary]))
        joblib.dump(model, args.output / f'{name}.joblib')
        if name in ('logistic_regression', 'random_forest', 'gradient_boosting'):
            base = pipeline(sklearn.base.clone(estimator), features.columns)
            base.fit(features.iloc[:calibration_boundary], y.iloc[:calibration_boundary])
            calibrated = CalibratedClassifierCV(FrozenEstimator(base), method='sigmoid')
            calibrated.fit(features.iloc[calibration_boundary:boundary], y.iloc[calibration_boundary:boundary])
            record(f'{name}_calibrated', calibrated.predict_proba(features.iloc[boundary:]), calibrated.score(features.iloc[:boundary], y.iloc[:boundary]))
            joblib.dump(calibrated, args.output / f'{name}_calibrated.joblib')
    metadata = {
        'created_utc': datetime.now(timezone.utc).isoformat(), 'dataset': str(args.data.resolve()),
        'dataset_sha256': hashlib.sha256(args.data.read_bytes()).hexdigest(),
        'python': platform.python_version(), 'sklearn': sklearn.__version__, 'numpy': np.__version__, 'pandas': pd.__version__,
        'train_rows': boundary, 'test_rows': len(df) - boundary,
        'train_dates': [str(df.date.iloc[0].date()), str(df.date.iloc[boundary-1].date())],
        'test_dates': [str(df.date.iloc[boundary].date()), str(df.date.iloc[-1].date())],
        'calibration_start': str(df.date.iloc[calibration_boundary].date()),
        'calibration_rows': boundary - calibration_boundary,
        'feature_columns': x.columns.tolist(),
        'parameters': {name: estimator.get_params() for name, (estimator, _) in specs.items()},
    }
    (args.output / 'results.json').write_text(json.dumps({'metadata': metadata, 'results': results}, indent=2) + '\n')
    predictions.to_csv(args.output / 'predictions.csv', index=False)
    report = [
        '## Reproducible model comparison', '',
        f'Run: {metadata["created_utc"]}. Dataset: `{args.data.name}`; SHA-256: `{metadata["dataset_sha256"]}`.', '',
        f'Train: {metadata["train_dates"]} ({boundary:,} fights). Test: {metadata["test_dates"]} ({len(df)-boundary:,} fights). The chronological 80/20 boundary moves to the start of its event date.', '',
        'All models use the same test fights. One unit is staked on each predicted winner; wins earn decimal odds minus one, losses cost one unit. Exact ties select Red. Both odds must be valid. No confidence threshold or value-bet filter is applied.', '',
        'Statistics models use the explicit MLP feature list, matchup ratios/differences, and rest time across both corners. IDs and names are excluded from predictors; odds are used only by the favorite and odds-only models. Preprocessing is fitted only on training data. Debut means first appearance in this dataset. Exported statistics are assumed to be pre-fight; this runner does not independently reconstruct them.', '',
        f'Calibrated variants fit their base model before {metadata["calibration_start"]}, then fit sigmoid calibration on the final {metadata["calibration_rows"]:,} training fights. Thus their base models see fewer fights than uncalibrated models. Test outcomes never fit calibration.', '',
        'Brier score and log loss use Blue probabilities; lower is better. ECE uses 10 equal-width probability bins. The favorite baseline uses normalized market-implied probabilities; the majority baseline uses training class frequencies for probability metrics. Confusion matrices list actual rows and predicted columns in [Red, Blue] order.', '',
        '| Model | Train accuracy | Test accuracy | Brier | Log loss | ECE | Bets | Units | ROI | Confusion matrix |',
        '|---|---:|---:|---:|---:|---:|---:|---:|---:|---|',
    ]
    for name, r in results.items():
        report.append(f'| {name} | {r["train_accuracy"]:.4f} | {r["test_accuracy"]:.4f} | {r["brier_score"]:.4f} | {r["log_loss"]:.4f} | {r["ece"]:.4f} | {r["bets"]:,} | {r["betting_units"]:+.2f} | {r["roi"]:.2%} | `{r["confusion_matrix"]}` |')
    report += ['', 'These results supersede the proposed comparisons below. Historical runs above are not directly comparable because their split and rest-time implementation differ. These are single held-out-period results, without hyperparameter search or uncertainty estimates.', '',
        'Reproduce: `.venv/bin/python -m models.comparisons.train`. See `models/comparisons/README.md` for artifacts and dependencies.', '']
    existing = args.report.read_text() if args.report.exists() else '# Model Results\n\n'
    marker = '<!-- model-comparison:start -->'
    end = '<!-- model-comparison:end -->'
    section = marker + '\n' + '\n'.join(report) + end
    if marker in existing:
        before, rest = existing.split(marker, 1)
        _, after = rest.split(end, 1)
        existing = before + section + after
    else:
        existing += '\n' + section + '\n'
    existing = existing.replace('These are recommended comparison points, but no results are recorded for them yet:', 'These comparison points are now evaluated in the reproducible model comparison below:')
    existing = existing.replace('supersede the proposed comparisons below', 'complete the proposed comparisons above')
    args.report.write_text(existing)


if __name__ == '__main__':
    with threadpool_limits(limits=1):
        main()
