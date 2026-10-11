"""Same-date folds, common-coverage metrics, and freshly fitted tree baselines."""

import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    brier_score_loss,
    log_loss,
)
from threadpoolctl import threadpool_limits

from models.rounds.duration import duration_metrics
from models.comparisons.evaluation import split_index
from scripts.train_rounds_model import load_dataset
from scripts.evaluate_rounds_walk_forward import walk_forward_windows
from .network import Preprocessor


def reference_folds(
    reference_data,
    fights_path,
    mode="walk-forward",
    fraction=0.8,
    step=100,
    five_step=40,
):
    df, _, usable, _ = load_dataset(reference_data, fights_path)
    records = []
    for scheduled in (3, 5):
        subset = df.loc[usable & df.number_of_rounds.eq(scheduled)].reset_index(
            drop=True
        )
        boundary = split_index(subset.date, fraction)
        windows = walk_forward_windows(
            subset.date, boundary, five_step if scheduled == 5 else step
        )
        if mode == "holdout":
            windows = [(boundary, len(subset))]
        for fold, (start, end) in enumerate(windows, 1):
            test = subset.iloc[start:end]
            records.append(
                dict(
                    scheduled=scheduled,
                    fold=fold,
                    test_start=test.date.min(),
                    test_end=test.date.max(),
                    ids=test.id.astype(int).tolist(),
                )
            )
    return records


def binary_metrics(actual, probability):
    mask = np.isfinite(actual) & np.isfinite(probability)
    y, p = (
        np.asarray(actual, dtype=float)[mask],
        np.asarray(probability, dtype=float)[mask],
    )
    if not len(y):
        return None
    bins = []
    for i in range(10):
        selected = (p >= i / 10) & ((p < (i + 1) / 10) if i < 9 else (p <= 1))
        if selected.any():
            bins.append(
                dict(
                    lower=i / 10,
                    count=int(selected.sum()),
                    mean_probability=float(p[selected].mean()),
                    observed_frequency=float(y[selected].mean()),
                )
            )
    return dict(
        rows=len(y),
        accuracy=float(accuracy_score(y, p >= 0.5)),
        balanced_accuracy=float(balanced_accuracy_score(y, p >= 0.5)),
        brier=float(brier_score_loss(y, p)),
        log_loss=float(log_loss(y, np.column_stack([1 - p, p]), labels=[0, 1])),
        ece=float(
            sum(
                b["count"] * abs(b["mean_probability"] - b["observed_frequency"])
                for b in bins
            )
            / len(y)
        ),
        calibration_bins=bins,
    )


def prediction_rows(data, probability, model, fold, train_through):
    result = data.rows.copy()
    result["model"] = model
    result["fold"] = fold
    result["train_through"] = train_through
    result["p_red"] = probability["winner"]
    result["p_blue"] = 1 - probability["winner"]
    result["p_distance"] = probability["distance"]
    result["prediction_rounds"] = probability["rounds"].argmax(axis=1) / 2
    for i in range(11):
        result[f"p_{i/2:.1f}"] = probability["rounds"][:, i]
    return result


def metrics(rows):
    result = {}
    for (name, scheduled), group in rows.groupby(["model", "number_of_rounds"]):
        for coverage in ("all", "odds_available"):
            selected = group if coverage == "all" else group[group.odds_available]
            if selected.empty:
                continue
            entry = dict(
                rows=len(selected),
                winner=binary_metrics(
                    selected.winner_target.to_numpy(), selected.p_red.to_numpy()
                ),
                distance=binary_metrics(
                    selected.distance_target.to_numpy(), selected.p_distance.to_numpy()
                ),
            )
            valid = selected.rounds_target.notna()
            d = selected.loc[valid]
            if len(d):
                classes = np.arange(scheduled * 2 + 1) / 2
                p = d[[f"p_{c:.1f}" for c in classes]].to_numpy(dtype=float)
                p /= p.sum(axis=1, keepdims=True)
                entry["duration"] = duration_metrics(
                    d.rounds_target, d.prediction_rounds, p, classes
                )
                entry["duration"]["cutoff_brier"] = {
                    f"{line:.1f}": float(
                        np.mean(
                            (
                                p[:, classes > line].sum(axis=1)
                                - (d.rounds_target.to_numpy() > line)
                            )
                            ** 2
                        )
                    )
                    for line in classes[:-1]
                }
            else:
                entry["duration"] = None
            result[f"{name}|{scheduled}|{coverage}"] = entry
    return result


def baselines(train, test, odds_mode, iterations=200, seed=0):
    """Trees and training-prior baselines use the same optimizer-fit partition as transformers."""
    prep = Preprocessor().fit(train)

    def features(data):
        x = prep.transform(data, odds_mode)
        return np.concatenate(
            [
                x["summary"].numpy().reshape(len(data), -1),
                x["summary_missing"].numpy().reshape(len(data), -1),
                x["stance"].numpy(),
                x["context"].numpy(),
            ],
            axis=1,
        )

    x, z = features(train), features(test)
    output = {name: {} for name in ("prior", "tree")}
    for task, column in [
        ("winner", "winner_target"),
        ("rounds", "rounds_target"),
        ("distance", "distance_target"),
    ]:
        mask = train.rows[column].notna().to_numpy()
        y = train.rows.loc[mask, column].to_numpy()
        if task == "rounds":
            y = (y * 2).astype(int)
        for name in output:
            if not len(y):
                probabilities = (
                    np.full((len(test), 11), 1 / 11)
                    if task == "rounds"
                    else np.full(len(test), 0.5)
                )
            elif name == "prior":
                probabilities = (
                    np.zeros((len(test), 11))
                    if task == "rounds"
                    else np.zeros(len(test))
                )
                # Scheduled-length priors avoid making five-round baselines inherit
                # the overwhelmingly more common three-round duration distribution.
                for scheduled in (3, 5):
                    matched = (
                        train.rows.loc[mask, "number_of_rounds"].to_numpy() == scheduled
                    )
                    local = y[matched] if matched.any() else y
                    selected = test.rows.number_of_rounds.to_numpy() == scheduled
                    if task == "rounds":
                        counts = np.bincount(local, minlength=11).astype(float)
                        probabilities[selected] = counts / counts.sum()
                    else:
                        probabilities[selected] = local.mean()
            else:
                estimator = (
                    DummyClassifier(strategy="prior")
                    if len(np.unique(y)) < 2
                    else HistGradientBoostingClassifier(
                        max_iter=iterations,
                        max_leaf_nodes=15,
                        learning_rate=0.05,
                        l2_regularization=2,
                        early_stopping=False,
                        random_state=seed,
                    )
                )
                with threadpool_limits(limits=1):
                    estimator.fit(x[mask], y)
                    raw = estimator.predict_proba(z)
                if task == "rounds":
                    probabilities = np.zeros((len(test), 11))
                    probabilities[:, estimator.classes_.astype(int)] = raw
                else:
                    probabilities = (
                        raw[:, list(estimator.classes_).index(1)]
                        if 1 in estimator.classes_
                        else np.zeros(len(test))
                    )
            if task == "rounds":
                allowed = (
                    np.arange(11)[None, :]
                    <= test.rows.number_of_rounds.to_numpy()[:, None] * 2
                )
                probabilities = np.where(allowed, probabilities, 0)
                # Smooth zero-probability classes for finite baseline log loss.
                probabilities += allowed * 1e-7
                probabilities /= probabilities.sum(axis=1, keepdims=True)
            output[name][task] = probabilities
    return output


def write_report(path, results, metadata):
    lines = [
        "# Transformer walk-forward comparison",
        "",
        f"Mode: {metadata['mode']}. Epoch limit: {metadata['training_config']['epochs']}. Seed: {metadata['training_config']['seed']}.",
        f"Tree baseline iterations: {metadata.get('baseline_iterations',200)}. Evaluation complete: {metadata.get('evaluation_complete',False)}.",
        "All histories are reconstructed from raw records; event dates remain intact.",
        "Historical odds capture times are unverified. Distance and duration are separate prediction heads.",
        "",
        "| Model | Rounds | Coverage | Fights | Win accuracy | Win log loss | Win Brier | Duration exact | Duration MAE | Duration log loss | Distance accuracy |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    def fmt(value, percent=False):
        return "—" if value is None else f"{value:.1%}" if percent else f"{value:.4f}"

    for key, entry in sorted(results.items()):
        name, scheduled, coverage = key.split("|")
        w, d, g = (
            entry["winner"] or {},
            entry["duration"] or {},
            entry["distance"] or {},
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    name,
                    scheduled,
                    coverage,
                    str(entry["rows"]),
                    fmt(w.get("accuracy"), True),
                    fmt(w.get("log_loss")),
                    fmt(w.get("brier")),
                    fmt(d.get("accuracy"), True),
                    fmt(d.get("mean_absolute_error_rounds")),
                    fmt(d.get("log_loss")),
                    fmt(g.get("accuracy"), True),
                ]
            )
            + " |"
        )
    lines += [
        "",
        "The odds_available rows are the paired comparison subset for stats-only and odds-assisted models.",
        "Prior and tree baselines are freshly fitted on the same optimizer-training rows as each corresponding transformer.",
        "See results.json for calibration bins and half-round cutoff scores; predictions.csv contains every out-of-sample prediction.",
        "",
    ]
    path.write_text("\n".join(lines))
