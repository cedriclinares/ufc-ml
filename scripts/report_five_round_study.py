"""Audit and summarize a paired multi-seed, five-round transformer study."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from models.transformers.data import decimal_price
from models.transformers.evaluation import binary_metrics
from models.transformers.training import load_checkpoint

MODELS = (
    "history_stats",
    "history_market",
    "tree_stats",
    "tree_market",
    "tree_calibrated_stats",
    "tree_calibrated_market",
    "prior_stats",
)
ACCURACIES = {"winner_accuracy", "duration_accuracy", "distance_accuracy"}


def per_fight_scores(rows):
    """Keep paired fight-level scores; missing targets remain missing."""
    y = rows.winner_target.to_numpy(float)
    p = np.clip(rows.p_red.to_numpy(float), 1e-15, 1 - 1e-15)
    duration = rows.rounds_target.to_numpy(float)
    distribution = rows[[f"p_{i / 2:.1f}" for i in range(11)]].to_numpy(float)
    scores = dict(
        winner_accuracy=np.where(np.isfinite(y), (p >= 0.5) == y, np.nan),
        winner_log_loss=-(y * np.log(p) + (1 - y) * np.log1p(-p)),
        winner_brier=(p - y) ** 2,
        duration_accuracy=np.where(
            np.isfinite(duration), rows.prediction_rounds.to_numpy() == duration, np.nan
        ),
        duration_mae=np.abs(rows.prediction_rounds.to_numpy() - duration),
    )
    nll = np.full(len(rows), np.nan)
    valid = np.isfinite(duration)
    indices = np.flatnonzero(valid)
    nll[valid] = -np.log(
        np.clip(distribution[indices, (duration[valid] * 2).astype(int)], 1e-15, 1)
    )
    scores["duration_log_loss"] = nll
    cutoff_errors = []
    for line in np.arange(0, 5, 0.5):
        over = distribution[:, np.arange(11) / 2 > line].sum(axis=1)
        score = np.where(valid, (over - (duration > line)) ** 2, np.nan)
        scores[f"over_{line:.1f}_brier"] = score
        cutoff_errors.append(score)
    scores["duration_survival_brier"] = np.mean(cutoff_errors, axis=0)
    d = rows.distance_target.to_numpy(float)
    pdistance = rows.p_distance.to_numpy(float)
    scores["distance_accuracy"] = np.where(
        np.isfinite(d), (pdistance >= 0.5) == d, np.nan
    )
    scores["distance_brier"] = (pdistance - d) ** 2
    return scores


def paired_interval(differences, dates, samples=10000):
    """Resample event dates, keeping same-date bouts together and pairing models."""
    differences = np.asarray(differences, float)
    dates = np.asarray(dates)
    mask = np.isfinite(differences)
    differences, dates = differences[mask], dates[mask]
    if not len(differences):
        raise ValueError("No paired valid outcomes")
    _, groups = np.unique(dates, return_inverse=True)
    counts = np.bincount(groups)
    sums = np.bincount(groups, weights=differences)
    rng = np.random.default_rng(20261004)
    selected = rng.integers(0, len(counts), (samples, len(counts)))
    boot = sums[selected].sum(axis=1) / counts[selected].sum(axis=1)
    lo, hi = np.quantile(boot, [0.025, 0.975])
    return dict(
        difference=float(differences.mean()),
        low=float(lo),
        high=float(hi),
        fights=len(differences),
        event_dates=len(counts),
    )


def audit_run(directory):
    result = json.loads((directory / "results.json").read_text())
    metadata = result["metadata"]
    if not metadata["evaluation_complete"]:
        raise ValueError(f"Incomplete study: {directory}")
    if metadata["training_coverage"] != "odds-available":
        raise ValueError("Input variants must have common training coverage")
    if metadata["scheduled_rounds"] != "5":
        raise ValueError("Expected five-round-only evaluation")
    rows = pd.read_csv(directory / "predictions.csv").sort_values(["model", "id"])
    calibrated = pd.read_csv(directory / "calibrated-tree-predictions.csv")
    calibration_records = json.loads((directory / "tree-calibration.json").read_text())
    rows = pd.concat([rows, calibrated], ignore_index=True)
    if rows.duplicated(["model", "id"]).any():
        raise ValueError("Duplicate out-of-sample prediction")
    if not rows.number_of_rounds.eq(5).all() or not rows.odds_available.all():
        raise ValueError("Unexpected evaluation coverage")
    model_ids = [set(rows.loc[rows.model.eq(m), "id"]) for m in MODELS]
    if not model_ids[0] or any(ids != model_ids[0] for ids in model_ids):
        raise ValueError("Models have different test fights")
    records = []
    for fold in sorted(rows.fold.unique()):
        matching = []
        for mode in ("stats", "market"):
            _, _, _, info = load_checkpoint(
                directory / "checkpoints" / f"history_{mode}_{fold}.pt"
            )
            training, calibration, test = (
                set(info[key])
                for key in ("optimizer_train_ids", "calibration_ids", "test_ids")
            )
            tree_records = [
                r
                for r in calibration_records
                if r["mode"] == mode and r["fold"] == fold
            ]
            if len(tree_records) != 1:
                raise ValueError("Missing or duplicate calibrated-tree provenance")
            tree_info = tree_records[0]
            if any(
                set(tree_info[key]) != set(info[key])
                for key in ("optimizer_train_ids", "calibration_ids", "test_ids")
            ):
                raise ValueError(
                    "GBT calibration/training/test IDs differ from the transformer"
                )
            if training & calibration or training & test or calibration & test:
                raise ValueError("Training/calibration/test ID overlap")
            if pd.Timestamp(info["train_through"]) >= pd.Timestamp(info["test_start"]):
                raise ValueError("Training/calibration dates reach the test window")
            matching.append((training, calibration, test, set(info["validation_ids"])))
            records.append(
                dict(
                    seed=metadata["training_config"]["seed"],
                    mode=mode,
                    fold=fold,
                    optimizer_fights=len(training),
                    calibration_fights=len(calibration),
                    test_fights=len(test),
                    selected_epoch=info["selected_epoch"],
                    stopped_epoch=len(info["learning_curve"]),
                    train_through=info["train_through"],
                    test_start=info["test_start"],
                    temperatures=info["temperatures"],
                )
            )
        if matching[0] != matching[1]:
            raise ValueError(
                "Stats/market training, validation, calibration or test IDs differ"
            )
    return rows, metadata, records


def summarize(root, seeds, output):
    runs, metadata, folds = {}, [], []
    for seed in seeds:
        rows, meta, records = audit_run(root / f"seed-{seed}" / "walk-forward")
        runs[seed] = rows
        metadata.append(meta)
        folds.extend(records)
    settings = [
        {k: v for k, v in m["training_config"].items() if k != "seed"} for m in metadata
    ]
    if any(s != settings[0] for s in settings):
        raise ValueError("Training configurations differ across seeds")
    if any(
        m["audit"]["fingerprints"] != metadata[0]["audit"]["fingerprints"]
        for m in metadata
    ):
        raise ValueError("Data fingerprints differ across seeds")
    groups = {
        (seed, model): run[run.model.eq(model)].sort_values("id").reset_index(drop=True)
        for seed, run in runs.items()
        for model in MODELS
    }
    reference = groups[seeds[0], MODELS[0]]
    truth_cols = [
        "id",
        "date",
        "winner_target",
        "rounds_target",
        "distance_target",
        "fold",
    ]
    for group in groups.values():
        pd.testing.assert_frame_equal(reference[truth_cols], group[truth_cols])
        probabilities = group[[f"p_{i / 2:.1f}" for i in range(11)]].to_numpy(float)
        if (probabilities < 0).any() or not np.allclose(
            probabilities.sum(axis=1), 1, atol=1e-5
        ):
            raise ValueError("Invalid duration probabilities")
    scores = {key: per_fight_scores(group) for key, group in groups.items()}
    summaries, average_scores = {}, {}
    for model in MODELS:
        summary, averaged = {}, {}
        for metric in scores[seeds[0], model]:
            stack = np.stack([scores[s, model][metric] for s in seeds])
            per_seed = np.nanmean(stack, axis=1)
            summary[metric] = dict(
                mean=float(per_seed.mean()),
                sd=float(per_seed.std(ddof=1)) if len(seeds) > 1 else 0.0,
                per_seed={str(s): float(v) for s, v in zip(seeds, per_seed)},
            )
            averaged[metric] = np.nanmean(stack, axis=0)
        summaries[model], average_scores[model] = summary, averaged
    comparisons = {}
    pairs = [
        ("history_stats", "tree_stats"),
        ("history_market", "tree_market"),
        ("history_market", "history_stats"),
        ("history_stats", "tree_market"),
        ("history_stats", "tree_calibrated_stats"),
        ("history_market", "tree_calibrated_market"),
        ("history_stats", "tree_calibrated_market"),
    ]
    for left, right in pairs:
        comparisons[f"{left} minus {right}"] = {
            metric: paired_interval(
                average_scores[left][metric] - average_scores[right][metric],
                reference.date,
            )
            for metric in average_scores[left]
        }
    ensembles, ensemble_scores = {}, {}
    for model in MODELS:
        group = groups[seeds[0], model].copy()
        columns = ["p_red", "p_distance"] + [f"p_{i / 2:.1f}" for i in range(11)]
        group[columns] = np.mean(
            [groups[s, model][columns].to_numpy() for s in seeds], axis=0
        )
        group["prediction_rounds"] = (
            group[[f"p_{i / 2:.1f}" for i in range(11)]].to_numpy().argmax(axis=1) / 2
        )
        ensemble_scores[model] = per_fight_scores(group)
        ensembles[model] = {
            k: float(np.nanmean(v)) for k, v in ensemble_scores[model].items()
        }
    ensemble_comparisons = {
        f"ensemble_{model} minus tree_calibrated_market": {
            metric: paired_interval(
                ensemble_scores[model][metric]
                - average_scores["tree_calibrated_market"][metric],
                reference.date,
            )
            for metric in (
                "winner_accuracy",
                "winner_log_loss",
                "duration_mae",
                "duration_survival_brier",
            )
        }
        for model in ("history_stats", "history_market")
    }
    raw = pd.read_csv(ROOT / "fights.csv").set_index("id").loc[reference.id]
    implied = np.column_stack(
        [
            1 / raw[f"{corner}_fighter_odds"].map(decimal_price).to_numpy()
            for corner in ("r", "b")
        ]
    )
    market_p = implied[:, 0] / implied.sum(axis=1)
    market_metrics = binary_metrics(reference.winner_target.to_numpy(), market_p)
    market_rows = reference.copy()
    market_rows["p_red"] = market_p
    market_scores = per_fight_scores(market_rows)
    for model in ("history_stats", "history_market"):
        comparisons[f"{model} minus odds_alone"] = {
            metric: paired_interval(
                average_scores[model][metric] - market_scores[metric], reference.date
            )
            for metric in ("winner_accuracy", "winner_log_loss", "winner_brier")
        }
    fold_metrics = {}
    for fold in sorted(reference.fold.unique()):
        mask = reference.fold.eq(fold).to_numpy()
        fold_metrics[fold] = dict(
            fights=int(mask.sum()),
            test_start=reference.loc[mask, "date"].min(),
            test_end=reference.loc[mask, "date"].max(),
            models={
                model: {
                    metric: float(np.nanmean(values[mask]))
                    for metric, values in score.items()
                }
                for model, score in average_scores.items()
            },
        )
    output.mkdir(parents=True, exist_ok=True)
    all_rows = pd.concat(
        [run.assign(seed=seed) for seed, run in runs.items()], ignore_index=True
    )
    all_rows.to_csv(output / "predictions.csv", index=False)
    results = dict(
        seeds=seeds,
        fights=len(reference),
        event_dates=reference.date.nunique(),
        test_start=reference.date.min(),
        test_end=reference.date.max(),
        metadata=metadata,
        folds=folds,
        summaries=summaries,
        comparisons=comparisons,
        ensembles=ensembles,
        ensemble_comparisons=ensemble_comparisons,
        market_only=market_metrics,
        fold_metrics=fold_metrics,
        audit_passed=True,
        bootstrap_samples=10000,
    )
    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    pd.DataFrame(folds).to_csv(output / "training.csv", index=False)
    write_report(output / "report.md", results)
    print(
        f"Audited {len(reference)} test fights across {len(seeds)} seeds. Report: {output / 'report.md'}"
    )


def write_report(path, results):
    summary = results["summaries"]
    lines = [
        "# Five-round history transformer versus GBT",
        "",
        f"Evaluation: {results['fights']} unique fights on {results['event_dates']} event dates, {results['test_start']} through {results['test_end']}.",
        f"Seeds: {results['seeds']}. Four chronological test folds per variant and seed.",
        "Both variants use identical odds-available training, validation, calibration and test IDs. Training pools three- and five-round fights.",
        "Transformer epoch ceiling 100, patience 10; validation chooses refit epochs. GBT uses 200 iterations. All models retain the same optimizer-fit partition.",
        "Transformer heads receive temperature calibration on held-out pre-test dates. The report includes both the original uncalibrated GBT and GBT with temperature calibration on exactly the same calibration IDs.",
        "Early stopping minimizes the combined multitask validation loss over pooled three- and five-round fights; it is not tuned specifically to five-round winner or duration accuracy.",
        "The GBT receives reconstructed summaries and context; the history transformer also receives ten-fight sequences. This compares complete models, not architecture with identical representations.",
        "",
        "## Winner predictions",
        "",
        "Values are mean ± sample standard deviation across seeds. Seed runs share the same test fights; sample size is not multiplied by three.",
        "",
        "| Model | Accuracy | Log loss | Brier |",
        "|---|---:|---:|---:|",
    ]

    def fmt(model, metric):
        stat = summary[model][metric]
        if metric in ACCURACIES:
            return f"{stat['mean']:.1%} ± {stat['sd'] * 100:.1f} pp"
        return f"{stat['mean']:.4f} ± {stat['sd']:.4f}"

    for model in MODELS:
        lines.append(
            f"| {model} | {fmt(model, 'winner_accuracy')} | {fmt(model, 'winner_log_loss')} | {fmt(model, 'winner_brier')} |"
        )
    market = results["market_only"]
    lines.extend(
        [
            f"| Odds alone (normalized implied probabilities) | {market['accuracy']:.1%} | {market['log_loss']:.4f} | {market['brier']:.4f} |",
            "",
            "## Duration predictions",
            "",
            "Exact accuracy and MAE use the most probable half-round category. Actual duration is ceiling-rounded to half-rounds.",
            "",
            "| Model | Exact accuracy | MAE (rounds) | Duration log loss | Survival Brier |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for model in MODELS:
        lines.append(
            f"| {model} | {fmt(model, 'duration_accuracy')} | {fmt(model, 'duration_mae')} | {fmt(model, 'duration_log_loss')} | {fmt(model, 'duration_survival_brier')} |"
        )
    lines.extend(
        [
            "",
            "Survival Brier averages squared probability error across all half-round cutoffs below five rounds; lower is better.",
            "",
            "| Model | Over 2.5 Brier | Over 3.5 Brier | Over 4.5 Brier | Distance accuracy | Distance Brier |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for model in MODELS:
        lines.append(
            f"| {model} | {fmt(model, 'over_2.5_brier')} | {fmt(model, 'over_3.5_brier')} | {fmt(model, 'over_4.5_brier')} | {fmt(model, 'distance_accuracy')} | {fmt(model, 'distance_brier')} |"
        )
    lines.extend(
        [
            "",
            "## Paired differences and uncertainty",
            "",
            "Differences are first model minus second, averaged across seeds for each fight. Positive accuracy differences favor the first model; negative error differences favor it.",
            "95% intervals use 10,000 paired event-date cluster bootstrap samples. They describe test-sample uncertainty conditional on these fits, not a full retraining bootstrap. Intervals are exploratory and unadjusted for multiple comparisons.",
            "",
            "| Comparison | Metric | Difference | 95% interval |",
            "|---|---|---:|---:|",
        ]
    )
    for comparison, metrics in results["comparisons"].items():
        for metric in (
            "winner_accuracy",
            "winner_log_loss",
            "winner_brier",
            "duration_accuracy",
            "duration_mae",
            "duration_survival_brier",
            "over_4.5_brier",
        ):
            if metric not in metrics:
                continue
            entry = metrics[metric]
            scale = 100 if metric in ACCURACIES else 1
            suffix = " pp" if metric in ACCURACIES else ""
            lines.append(
                f"| {comparison} | {metric} | {entry['difference'] * scale:+.4f}{suffix} | [{entry['low'] * scale:+.4f}, {entry['high'] * scale:+.4f}]{suffix} |"
            )
    lines.extend(
        [
            "",
            "## Seed probability ensembles (additional diagnostic)",
            "",
            "These average the three seeds' probabilities before selecting classes; they are different models from the mean single-seed results above.",
            "",
            "| Model | Winner accuracy | Winner log loss | Duration exact | Duration MAE | Survival Brier |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for model, metrics in results["ensembles"].items():
        lines.append(
            f"| {model} | {metrics['winner_accuracy']:.1%} | {metrics['winner_log_loss']:.4f} | {metrics['duration_accuracy']:.1%} | {metrics['duration_mae']:.4f} | {metrics['duration_survival_brier']:.4f} |"
        )
    lines.extend(
        [
            "",
            "Paired ensemble differences versus calibrated market GBT use the same event-date bootstrap:",
            "",
            "| Ensemble | Metric | Difference | 95% interval |",
            "|---|---|---:|---:|",
        ]
    )
    for comparison, entries in results["ensemble_comparisons"].items():
        for metric, entry in entries.items():
            scale, suffix = (100, " pp") if metric in ACCURACIES else (1, "")
            lines.append(
                f"| {comparison} | {metric} | {entry['difference'] * scale:+.4f}{suffix} | [{entry['low'] * scale:+.4f}, {entry['high'] * scale:+.4f}]{suffix} |"
            )
    lines.extend(
        [
            "",
            "## Results by test fold",
            "",
            "Winner accuracy and duration MAE below are means across seeds; GBT uses market inputs. Calibration preserves GBT class predictions.",
            "",
            "| Fold | Dates | Fights | History stats win | History market win | GBT market win |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for fold, item in results["fold_metrics"].items():
        m = item["models"]
        lines.append(
            f"| {fold} | {item['test_start']} to {item['test_end']} | {item['fights']} | {m['history_stats']['winner_accuracy']:.1%} | {m['history_market']['winner_accuracy']:.1%} | {m['tree_market']['winner_accuracy']:.1%} |"
        )
    lines.extend(
        [
            "",
            "| Fold | History stats MAE | History market MAE | GBT market MAE |",
            "|---|---:|---:|---:|",
        ]
    )
    for fold, item in results["fold_metrics"].items():
        m = item["models"]
        lines.append(
            f"| {fold} | {m['history_stats']['duration_mae']:.4f} | {m['history_market']['duration_mae']:.4f} | {m['tree_market']['duration_mae']:.4f} |"
        )
    lines.extend(
        [
            "",
            "## Selected training epochs",
            "",
            "| Seed | Variant | Fold | Selected epoch | Stopped epoch | Optimizer fights | Calibration fights | Test fights |",
            "|---:|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for f in results["folds"]:
        lines.append(
            f"| {f['seed']} | {f['mode']} | {f['fold']} | {f['selected_epoch']} | {f['stopped_epoch']} | {f['optimizer_fights']} | {f['calibration_fights']} | {f['test_fights']} |"
        )
    learned = [m for m in MODELS if not m.startswith("prior")]
    best_winner = min(learned, key=lambda m: summary[m]["winner_log_loss"]["mean"])
    best_duration = min(learned, key=lambda m: summary[m]["duration_mae"]["mean"])
    clear_winner_improvements = [
        m
        for m in ("history_stats", "history_market")
        if results["comparisons"][f"{m} minus tree_calibrated_market"][
            "winner_log_loss"
        ]["high"]
        < 0
    ]
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            f"The lowest mean single-seed winner log loss among trained candidates is {best_winner}: {summary[best_winner]['winner_log_loss']['mean']:.4f}. Odds alone scores {results['market_only']['log_loss']:.4f}.",
            f"The lowest mean duration MAE is {best_duration}: {summary[best_duration]['duration_mae']['mean']:.4f} rounds. Temperature calibration preserves its most probable class, so raw and calibrated GBT point predictions tie.",
        ]
    )
    if clear_winner_improvements:
        lines.append(
            f"The exploratory paired interval indicates lower winner log loss than calibrated market GBT for: {', '.join(clear_winner_improvements)}. This remains conditional on these fits and the reused test sample."
        )
    else:
        lines.append(
            "Neither history variant establishes lower winner log loss than calibrated market GBT in the paired 95% intervals; both intervals include zero."
        )
    lines.extend(
        [
            "Duration probability scores should guide an over/under assessment alongside MAE; a better modal duration estimate does not automatically imply better cutoff probabilities. See the cutoff Brier scores and their paired intervals in results.json.",
            "Keep production betting models unchanged while evaluating challengers on future unseen fights. This study does not establish a profitable deployment rule or optimized EV thresholds.",
        ]
    )
    lines.extend(
        [
            "",
            "## Limits and provenance",
            "",
            "All common-coverage, ID-disjointness, chronological cutoff and probability checks passed. Seed directories retain checkpoints, coverage hashes, folds and per-fight predictions; results.json stores all metric values and intervals.",
            "These test fights were already examined in the initial study, so this is a follow-up on an existing holdout, not an independent confirmation set. Early stopping and temperature calibration use pre-test dates, never these test outcomes.",
            "Earlier duration GBT results (five-round MAE 1.307) used a different dataset/feature representation and schedule-specific training; they are not part of the controlled comparison above.",
            "Historical odds capture timestamps and static fighter measurements are unverified. No betting ROI is established; historical over/under prices are unavailable in this study. Distance and duration are independent heads.",
            "The choice of a five-round-specific deployment policy must still be tested on future, unseen fights. Do not infer statistical significance from a higher point estimate alone.",
            "",
        ]
    )
    path.write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT / "models/transformers/artifacts/five-round-study",
    )
    parser.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2])
    parser.add_argument(
        "--output", type=Path, default=ROOT / "logs/five-round-transformer-study"
    )
    args = parser.parse_args()
    if len(args.seeds) != len(set(args.seeds)):
        parser.error("Seeds must be unique")
    summarize(args.root, args.seeds, args.output)


if __name__ == "__main__":
    main()
