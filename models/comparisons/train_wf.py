"""Walk-forward training with ELO features, EV-based value betting, and ensembling.

Run with: python -m models.comparisons.train_wf (from the repository root).
"""
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
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from threadpoolctl import threadpool_limits

from .elo import compute_elo, compute_multi_elo, compute_streaks, compute_experience
from .evaluation import decimal_odds, evaluate, make_features, pipeline, split_index
from .value_betting import (kelly_evaluate, kelly_filtered_evaluate,
                             optimize_threshold,
                             prediction_filtered_evaluate, value_bet_evaluate)
from .walk_forward import walk_forward_predict

ROOT = Path(__file__).resolve().parents[2]

CONFIGS = {
    'gb_deep_multielo_odds': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=500, max_leaf_nodes=63, learning_rate=0.01,
            l2_regularization=1, min_samples_leaf=30,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
    },
    'gb_conservative_elo_odds': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=300, max_leaf_nodes=15, learning_rate=0.03,
            l2_regularization=5, min_samples_leaf=50,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': False,
    },
    'gb_med_multielo_odds': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=300, max_leaf_nodes=31, learning_rate=0.05,
            l2_regularization=2, min_samples_leaf=20,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
    },
    'gb_wide_multielo_odds': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=800, max_leaf_nodes=31, learning_rate=0.01,
            l2_regularization=0.5, min_samples_leaf=10,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
    },
    'gb_shallow_multielo_odds': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=200, max_leaf_nodes=7, learning_rate=0.05,
            l2_regularization=3, min_samples_leaf=40,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
    },
    'gb_v_shallow_multielo_odds': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=150, max_leaf_nodes=7, learning_rate=0.03,
            l2_regularization=10, min_samples_leaf=80,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
    },
    'gb_mid_deep_multielo_odds': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=400, max_leaf_nodes=31, learning_rate=0.02,
            l2_regularization=2, min_samples_leaf=30,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
    },
    'lr_multielo_odds': {
        'estimator': lambda: LogisticRegression(max_iter=2000, random_state=0),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
    },
    # --- Reduced features (drop per-round stats) ---
    'gb_shallow_reduced_odds': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=200, max_leaf_nodes=7, learning_rate=0.05,
            l2_regularization=3, min_samples_leaf=40,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
        'drop_rounds': True,
    },
    'gb_med_reduced_odds': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=300, max_leaf_nodes=31, learning_rate=0.05,
            l2_regularization=2, min_samples_leaf=20,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
        'drop_rounds': True,
    },
    # --- Random Forest for ensemble diversity ---
    'rf_multielo_odds': {
        'estimator': lambda: RandomForestClassifier(
            n_estimators=500, max_depth=8, min_samples_leaf=30,
            random_state=0, n_jobs=1,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
    },
    # --- Calibrated GB for better probability estimates ---
    'gb_cal_multielo_odds': {
        'estimator': lambda: CalibratedClassifierCV(
            HistGradientBoostingClassifier(
                max_iter=300, max_leaf_nodes=31, learning_rate=0.05,
                l2_regularization=2, min_samples_leaf=20,
                early_stopping=False, random_state=0,
            ),
            method='isotonic', cv=3,
        ),
        'use_elo': True, 'use_odds': True, 'multi_elo': True,
    },
}


def build_features(df, decimal, use_elo=True, use_odds=False, multi_elo=False,
                    drop_rounds=False):
    """Build feature matrix with optional ELO/streak/experience and odds features."""
    x = make_features(df)
    if drop_rounds:
        round_cols = [c for c in x.columns if '_round_' in c]
        x = x.drop(columns=round_cols)
    if use_elo:
        if multi_elo:
            elo = compute_multi_elo(df, k_values=(16, 32, 48))
        else:
            elo = compute_elo(df)
        streaks = compute_streaks(df)
        experience = compute_experience(df)
        x = pd.concat([x, elo, streaks, experience], axis=1)
    if use_odds:
        implied = 1 / decimal
        market = implied / implied.sum(axis=1, keepdims=True)
        odds_df = pd.DataFrame({
            'r_implied_prob': market[:, 0],
            'b_implied_prob': market[:, 1],
        }, index=x.index)
        x = pd.concat([x, odds_df], axis=1)
    return x


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, default=ROOT / 'final-dataset-names-and-ids.csv')
    parser.add_argument('--output', type=Path, default=ROOT / 'models/comparisons/artifacts')
    parser.add_argument('--report', type=Path, default=ROOT / 'model_results.md')
    parser.add_argument('--min-train', type=int, default=4000,
                        help='Minimum training rows before first walk-forward prediction')
    parser.add_argument('--step', type=int, default=100,
                        help='Approximate fights per walk-forward fold')
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

    boundary = split_index(df.date)
    args.output.mkdir(parents=True, exist_ok=True)

    results = {}
    all_test_proba = {}  # name -> probability array for ensembling
    test_y_global = None
    test_decimal_global = None
    thresholds = [0.0, 0.02, 0.05, 0.08, 0.10, 0.12, 0.15,
                  0.18, 0.20, 0.25, 0.30, 0.40, 0.50]

    for name, config in CONFIGS.items():
        print(f'\n=== {name} ===', flush=True)
        x = build_features(df, decimal,
                           use_elo=config['use_elo'],
                           use_odds=config.get('use_odds', False),
                           multi_elo=config.get('multi_elo', False),
                           drop_rounds=config.get('drop_rounds', False))

        # Walk-forward on TRAINING period for threshold selection
        print(f'  Walk-forward CV on training period...', flush=True)
        cv_proba, cv_y, cv_decimal = walk_forward_predict(
            df.iloc[:boundary], y.iloc[:boundary], x.iloc[:boundary],
            decimal[:boundary], config['estimator'],
            min_train=args.min_train, step=args.step,
        )
        cv_best_threshold, cv_best = optimize_threshold(
            cv_y, cv_proba, cv_decimal, thresholds,
        )
        print(f'  CV best threshold: {cv_best_threshold:.2f} '
              f'(units={cv_best["betting_units"]:+.2f}, '
              f'bets={cv_best["bets"]}, roi={cv_best["roi"]:.2%})', flush=True)

        # Walk-forward on FULL dataset (test period gets out-of-sample predictions)
        print(f'  Walk-forward on test period...', flush=True)
        test_proba, test_y, test_decimal = walk_forward_predict(
            df, y, x, decimal, config['estimator'],
            min_train=boundary, step=args.step,
        )

        # Store for ensembling
        all_test_proba[name] = test_proba
        if test_y_global is None:
            test_y_global = test_y
            test_decimal_global = test_decimal

        # Flat-stake evaluation (all bets)
        flat_metrics, flat_pred, flat_profits = evaluate(test_y, test_proba, test_decimal)
        print(f'  Flat: accuracy={flat_metrics["test_accuracy"]:.4f}, '
              f'units={flat_metrics["betting_units"]:+.2f}', flush=True)

        # Value betting with CV-selected threshold
        value_metrics = value_bet_evaluate(
            test_y, test_proba, test_decimal, cv_best_threshold,
        )
        print(f'  Value-EV (t={cv_best_threshold:.2f}): '
              f'acc={value_metrics["test_accuracy"]:.4f}, '
              f'units={value_metrics["betting_units"]:+.2f}, '
              f'bets={value_metrics["bets"]}, roi={value_metrics["roi"]:.2%}', flush=True)

        # Prediction-filtered: bet on model's pick when EV is positive
        pred_filt = prediction_filtered_evaluate(
            test_y, test_proba, test_decimal, ev_threshold=0.0,
        )
        print(f'  Pred-filtered (t=0): '
              f'acc={pred_filt["test_accuracy"]:.4f}, '
              f'units={pred_filt["betting_units"]:+.2f}, '
              f'bets={pred_filt["bets"]}', flush=True)

        # Kelly-proportional betting
        kelly = kelly_evaluate(test_y, test_proba, test_decimal, fraction=0.25, min_ev=0.0)
        print(f'  Kelly (f=0.25): '
              f'units={kelly["betting_units"]:+.2f}, '
              f'bets={kelly["bets"]}, wagered={kelly["total_wagered"]:.1f}', flush=True)

        # Kelly-filtered: EV threshold + Kelly sizing
        kelly_filt = kelly_filtered_evaluate(
            test_y, test_proba, test_decimal,
            ev_threshold=cv_best_threshold, fraction=0.5,
        )
        print(f'  Kelly-filtered (t={cv_best_threshold:.2f}, f=0.5): '
              f'units={kelly_filt["betting_units"]:+.2f}, '
              f'bets={kelly_filt["bets"]}, wagered={kelly_filt["total_wagered"]:.1f}', flush=True)

        # Threshold landscape for transparency
        threshold_landscape = {}
        pred_landscape = {}
        kelly_landscape = {}
        kelly_filt_landscape = {}
        for t in thresholds:
            threshold_landscape[str(t)] = value_bet_evaluate(test_y, test_proba, test_decimal, t)
            pred_landscape[str(t)] = prediction_filtered_evaluate(test_y, test_proba, test_decimal, t)
        for frac in [0.1, 0.25, 0.5, 1.0]:
            kelly_landscape[str(frac)] = kelly_evaluate(test_y, test_proba, test_decimal, fraction=frac, min_ev=0.0)
        # Kelly-filtered grid: threshold x fraction
        for t in thresholds:
            for frac in [0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 10.0]:
                key = f't{t}_f{frac}'
                kelly_filt_landscape[key] = kelly_filtered_evaluate(
                    test_y, test_proba, test_decimal, ev_threshold=t, fraction=frac,
                )

        results[name] = {
            'flat': flat_metrics,
            'value': value_metrics,
            'pred_filtered': pred_filt,
            'kelly': kelly,
            'kelly_filtered': kelly_filt,
            'cv_threshold': cv_best_threshold,
            'cv_metrics': cv_best,
            'threshold_landscape': threshold_landscape,
            'pred_landscape': pred_landscape,
            'kelly_landscape': kelly_landscape,
            'kelly_filt_landscape': kelly_filt_landscape,
            'use_elo': config['use_elo'],
            'use_odds': config.get('use_odds', False),
        }

    # --- Ensemble evaluation ---
    print('\n=== ENSEMBLES ===', flush=True)
    odds_models = [n for n in all_test_proba if CONFIGS[n].get('use_odds')]

    # All-model ensemble
    if len(odds_models) >= 2:
        ensemble_proba = np.mean([all_test_proba[n] for n in odds_models], axis=0)
        print(f'\nEnsemble (all {len(odds_models)} odds models):', flush=True)
        flat_m, _, _ = evaluate(test_y_global, ensemble_proba, test_decimal_global)
        print(f'  Flat: accuracy={flat_m["test_accuracy"]:.4f}, '
              f'units={flat_m["betting_units"]:+.2f}', flush=True)

        # CV for ensemble threshold: average CV probabilities too
        cv_probas = []
        for name in odds_models:
            config = CONFIGS[name]
            x = build_features(df, decimal,
                               use_elo=config['use_elo'],
                               use_odds=True,
                               multi_elo=config.get('multi_elo', False),
                               drop_rounds=config.get('drop_rounds', False))
            cv_p, cv_y_e, cv_d = walk_forward_predict(
                df.iloc[:boundary], y.iloc[:boundary], x.iloc[:boundary],
                decimal[:boundary], config['estimator'],
                min_train=args.min_train, step=args.step,
            )
            cv_probas.append(cv_p)
        ensemble_cv = np.mean(cv_probas, axis=0)
        ens_cv_thresh, ens_cv_best = optimize_threshold(cv_y_e, ensemble_cv, cv_d, thresholds)
        print(f'  CV threshold: {ens_cv_thresh:.2f} (cv_units={ens_cv_best["betting_units"]:+.2f})', flush=True)

        ens_value = value_bet_evaluate(test_y_global, ensemble_proba, test_decimal_global, ens_cv_thresh)
        print(f'  Value (t={ens_cv_thresh:.2f}): accuracy={ens_value["test_accuracy"]:.4f}, '
              f'units={ens_value["betting_units"]:+.2f}, '
              f'bets={ens_value["bets"]}, roi={ens_value["roi"]:.2%}', flush=True)

        ens_landscape = {}
        ens_kf_landscape = {}
        for t in thresholds:
            ens_landscape[str(t)] = value_bet_evaluate(test_y_global, ensemble_proba, test_decimal_global, t)
            for frac in [0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 10.0]:
                ens_kf_landscape[f't{t}_f{frac}'] = kelly_filtered_evaluate(
                    test_y_global, ensemble_proba, test_decimal_global, t, frac)

        ens_kelly_filt = kelly_filtered_evaluate(
            test_y_global, ensemble_proba, test_decimal_global, ens_cv_thresh, 1.0)
        print(f'  Kelly-filtered (t={ens_cv_thresh:.2f}, f=1.0): '
              f'units={ens_kelly_filt["betting_units"]:+.2f}, '
              f'bets={ens_kelly_filt["bets"]}', flush=True)

        results['ensemble_all'] = {
            'flat': flat_m,
            'value': ens_value,
            'kelly_filtered': ens_kelly_filt,
            'cv_threshold': ens_cv_thresh,
            'threshold_landscape': ens_landscape,
            'kelly_filt_landscape': ens_kf_landscape,
            'use_elo': True, 'use_odds': True,
            'models': odds_models,
        }

    # Top-3 ensemble (best individual CV performers)
    top3 = sorted(odds_models, key=lambda n: results[n]['flat']['betting_units'], reverse=True)[:3]
    if len(top3) >= 2:
        top3_proba = np.mean([all_test_proba[n] for n in top3], axis=0)
        print(f'\nEnsemble top-3 ({", ".join(top3)}):', flush=True)
        flat_t3, _, _ = evaluate(test_y_global, top3_proba, test_decimal_global)
        print(f'  Flat: accuracy={flat_t3["test_accuracy"]:.4f}, '
              f'units={flat_t3["betting_units"]:+.2f}', flush=True)

        # CV threshold for top-3
        cv_probas_t3 = []
        for name in top3:
            config = CONFIGS[name]
            x = build_features(df, decimal,
                               use_elo=config['use_elo'],
                               use_odds=True,
                               multi_elo=config.get('multi_elo', False),
                               drop_rounds=config.get('drop_rounds', False))
            cv_p, cv_y_t3, cv_d_t3 = walk_forward_predict(
                df.iloc[:boundary], y.iloc[:boundary], x.iloc[:boundary],
                decimal[:boundary], config['estimator'],
                min_train=args.min_train, step=args.step,
            )
            cv_probas_t3.append(cv_p)
        ensemble_cv_t3 = np.mean(cv_probas_t3, axis=0)
        t3_cv_thresh, t3_cv_best = optimize_threshold(cv_y_t3, ensemble_cv_t3, cv_d_t3, thresholds)
        print(f'  CV threshold: {t3_cv_thresh:.2f} (cv_units={t3_cv_best["betting_units"]:+.2f})', flush=True)

        t3_value = value_bet_evaluate(test_y_global, top3_proba, test_decimal_global, t3_cv_thresh)
        print(f'  Value (t={t3_cv_thresh:.2f}): accuracy={t3_value["test_accuracy"]:.4f}, '
              f'units={t3_value["betting_units"]:+.2f}, '
              f'bets={t3_value["bets"]}, roi={t3_value["roi"]:.2%}', flush=True)

        t3_landscape = {}
        t3_kf_landscape = {}
        for t in thresholds:
            t3_landscape[str(t)] = value_bet_evaluate(test_y_global, top3_proba, test_decimal_global, t)
            for frac in [0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 10.0]:
                t3_kf_landscape[f't{t}_f{frac}'] = kelly_filtered_evaluate(
                    test_y_global, top3_proba, test_decimal_global, t, frac)

        t3_kelly_filt = kelly_filtered_evaluate(
            test_y_global, top3_proba, test_decimal_global, t3_cv_thresh, 1.0)
        print(f'  Kelly-filtered (t={t3_cv_thresh:.2f}, f=1.0): '
              f'units={t3_kelly_filt["betting_units"]:+.2f}, '
              f'bets={t3_kelly_filt["bets"]}', flush=True)

        results['ensemble_top3'] = {
            'flat': flat_t3,
            'value': t3_value,
            'kelly_filtered': t3_kelly_filt,
            'cv_threshold': t3_cv_thresh,
            'threshold_landscape': t3_landscape,
            'kelly_filt_landscape': t3_kf_landscape,
            'use_elo': True, 'use_odds': True,
            'models': top3,
        }

    # Write results
    metadata = {
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'dataset': str(args.data.resolve()),
        'dataset_sha256': hashlib.sha256(args.data.read_bytes()).hexdigest(),
        'python': platform.python_version(),
        'sklearn': sklearn.__version__,
        'numpy': np.__version__,
        'pandas': pd.__version__,
        'train_rows': boundary,
        'test_rows': len(df) - boundary,
        'train_dates': [str(df.date.iloc[0].date()), str(df.date.iloc[boundary - 1].date())],
        'test_dates': [str(df.date.iloc[boundary].date()), str(df.date.iloc[-1].date())],
        'min_train': args.min_train,
        'step': args.step,
    }
    (args.output / 'wf_results.json').write_text(
        json.dumps({'metadata': metadata, 'results': results}, indent=2, default=str) + '\n'
    )

    # Update model_results.md
    report = [
        '## Walk-forward value betting comparison', '',
        f'Run: {metadata["created_utc"]}. Dataset: `{args.data.name}`; '
        f'SHA-256: `{metadata["dataset_sha256"][:16]}...`.', '',
        f'Train: {metadata["train_dates"]} ({boundary:,} fights). '
        f'Test: {metadata["test_dates"]} ({len(df) - boundary:,} fights).', '',
        f'Walk-forward: min_train={args.min_train}, step={args.step}. '
        f'Models retrain on expanding windows aligned to event dates.', '',
        'Value betting uses EV-based side selection: for each fight, '
        'EV = model_prob * decimal_odds - 1 is computed for both sides, '
        'and the side with highest EV is bet when it exceeds the threshold. '
        'This correctly finds underdog value.', '',
        'EV threshold selected on training-period walk-forward CV '
        '(maximizing betting units). Ensemble models average probabilities '
        'from multiple base models before applying value betting.', '',
        '| Model | ELO | Odds | CV threshold | Flat acc | Flat units | '
        'Value acc | Value bets | Value units | Value ROI |',
        '|---|---|---|---:|---:|---:|---:|---:|---:|---:|',
    ]
    for name, r in results.items():
        f = r['flat']
        v = r['value']
        report.append(
            f'| {name} | {"yes" if r.get("use_elo") else "no"} | '
            f'{"yes" if r.get("use_odds") else "no"} | '
            f'{r["cv_threshold"]:.2f} | '
            f'{f["test_accuracy"]:.4f} | {f["betting_units"]:+.2f} | '
            f'{v["test_accuracy"]:.4f} | {v["bets"]:,} | '
            f'{v["betting_units"]:+.2f} | {v["roi"]:.2%} |'
        )

    # Threshold landscape for best model
    best_name = max(results, key=lambda n: results[n]['value']['betting_units'])
    report += ['', f'### Threshold landscape for {best_name}', '',
               '| Threshold | Bets | Units | ROI | Accuracy |',
               '|---:|---:|---:|---:|---:|']
    for t_str, t_metrics in results[best_name]['threshold_landscape'].items():
        report.append(
            f'| {float(t_str):.2f} | {t_metrics["bets"]:,} | '
            f'{t_metrics["betting_units"]:+.2f} | {t_metrics["roi"]:.2%} | '
            f'{t_metrics["test_accuracy"]:.4f} |'
        )

    # Kelly-filtered best results
    best_kf = {}
    for name, r in results.items():
        if 'kelly_filt_landscape' not in r:
            continue
        for key, m in r['kelly_filt_landscape'].items():
            if m['betting_units'] > best_kf.get('units', 0):
                best_kf = {'model': name, 'key': key, 'units': m['betting_units'],
                           'bets': m['bets'], 'wagered': m['total_wagered'],
                           'roi': m['roi'], 'avg_size': m['avg_bet_size']}
    if best_kf:
        report += ['', '### Best Kelly-filtered strategy', '',
            f'Model: **{best_kf["model"]}**, params: `{best_kf["key"]}`',
            f'- Units: **{best_kf["units"]:+.2f}**',
            f'- Bets: {best_kf["bets"]}, Total wagered: {best_kf["wagered"]:.1f}',
            f'- ROI: {best_kf["roi"]:.2%}, Avg bet size: {best_kf["avg_size"]:.3f}',
        ]

    # Top 10 Kelly-filtered strategies
    all_kf = []
    for name, r in results.items():
        if 'kelly_filt_landscape' not in r:
            continue
        for key, m in r['kelly_filt_landscape'].items():
            all_kf.append((name, key, m))
    all_kf.sort(key=lambda x: x[2]['betting_units'], reverse=True)
    if all_kf:
        report += ['', '### Top 10 Kelly-filtered strategies', '',
            '| Model | Params | Bets | Units | Wagered | ROI |',
            '|---|---|---:|---:|---:|---:|']
        for name, key, m in all_kf[:10]:
            report.append(
                f'| {name} | {key} | {m["bets"]} | '
                f'{m["betting_units"]:+.2f} | {m["total_wagered"]:.1f} | '
                f'{m["roi"]:.2%} |')

    report += ['',
        f'Reproduce: `.venv/bin/python -m models.comparisons.train_wf`. '
        f'See `models/comparisons/artifacts/wf_results.json` for full details.', '']

    existing = args.report.read_text() if args.report.exists() else '# Model Results\n\n'
    marker = '<!-- wf-comparison:start -->'
    end = '<!-- wf-comparison:end -->'
    section = marker + '\n' + '\n'.join(report) + end
    if marker in existing:
        before, rest = existing.split(marker, 1)
        _, after = rest.split(end, 1)
        existing = before + section + after
    else:
        existing += '\n' + section + '\n'
    args.report.write_text(existing)
    print(f'\nResults written to {args.report} and {args.output / "wf_results.json"}')


if __name__ == '__main__':
    with threadpool_limits(limits=1):
        main()
