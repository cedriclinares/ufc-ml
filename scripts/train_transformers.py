"""Train shared UFC winner/duration transformers and compare chronological predictions."""

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from models.transformers.data import load_data
from models.transformers.network import ModelConfig
from models.transformers.training import (
    TrainingConfig,
    fit_model,
    predict,
    save_checkpoint,
    load_checkpoint,
    chronological_parts,
    device_for,
)
from models.transformers.evaluation import (
    reference_folds,
    baselines,
    prediction_rows,
    metrics,
    write_report,
)


def run(args):
    if args.mode == "production" and not args.before:
        raise ValueError("Production training requires --before YYYY-MM-DD")
    data = load_data(args.fights, args.fighters, args.stats, before=args.before)
    architectures = (
        ["features", "history"] if args.architecture == "all" else [args.architecture]
    )
    modes = ["stats", "market"] if args.odds_mode == "all" else [args.odds_mode]
    config = TrainingConfig(
        epochs=args.epochs,
        batch_size=args.batch_size,
        patience=args.patience,
        seed=args.seed,
        device=args.device,
        threads=args.threads,
    )
    output = args.output / args.mode
    output.mkdir(parents=True, exist_ok=True)
    metadata = dict(
        mode=args.mode,
        training_config=asdict(config),
        audit=data.audit,
        baseline_iterations=args.baseline_iterations,
        training_coverage=args.training_coverage,
        scheduled_rounds=args.scheduled_rounds,
    )
    (output / "coverage.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(
        f'Reconstructed {len(data):,} matchups; {len(data.audit["exclusions"])} exclusions. Device: {device_for(args.device)}.',
        flush=True,
    )
    if args.mode == "production":
        for architecture in architectures:
            for mode in modes:
                selected = (
                    data.take(np.flatnonzero(data.rows.odds_available))
                    if mode == "market" or args.training_coverage == "odds-available"
                    else data
                )
                name = f"{architecture}_{mode}"
                print(
                    f"Training production {name} through {selected.rows.date.max().date()}...",
                    flush=True,
                )
                model, prep, t, info = fit_model(
                    selected,
                    ModelConfig(architecture=architecture, odds_mode=mode),
                    config,
                )
                info.update(mode="production", prediction_date=args.before)
                save_checkpoint(output / f"{name}.pt", model, prep, t, info)
                (output / f"{name}.json").write_text(json.dumps(info, indent=2) + "\n")
        return
    folds = reference_folds(
        args.reference_data,
        args.fights,
        args.mode,
        args.initial_train_fraction,
        args.step,
        args.five_round_step,
    )
    if args.scheduled_rounds != "all":
        folds = [f for f in folds if f["scheduled"] == int(args.scheduled_rounds)]
    metadata["reference_fold_count"] = len(folds)
    if args.max_folds:
        folds = folds[: args.max_folds]
    metadata["expected_model_folds"] = len(folds) * len(architectures) * len(modes)
    metadata["evaluation_complete"] = False
    chunks, fold_records = [], []
    for architecture in architectures:
        for mode in modes:
            name = f"{architecture}_{mode}"
            for record in folds:
                start = record["test_start"]
                train_idx = np.flatnonzero(
                    (data.rows.date < start)
                    & (
                        data.rows.odds_available
                        if mode == "market"
                        or args.training_coverage == "odds-available"
                        else True
                    )
                )
                test_idx = np.flatnonzero(data.rows.id.isin(record["ids"]))
                missing = set(record["ids"]) - set(data.rows.iloc[test_idx].id)
                if missing:
                    raise ValueError(
                        f"Reference test IDs missing from reconstruction: {sorted(missing)}"
                    )
                if mode == "market":
                    test_idx = test_idx[
                        data.rows.iloc[test_idx].odds_available.to_numpy()
                    ]
                if not len(test_idx):
                    continue
                train, test = data.take(train_idx), data.take(test_idx)
                if train.rows.date.max() >= test.rows.date.min():
                    raise ValueError("Train/test dates overlap")
                fold = f"{record['scheduled']}r-{record['fold']}"
                print(
                    f"{name} {fold}: {len(train):,} train, {len(test)} test, cutoff {start.date()}",
                    flush=True,
                )
                model_config = ModelConfig(architecture=architecture, odds_mode=mode)
                checkpoint = output / "checkpoints" / f"{name}_{fold}.pt"
                cached = None
                if args.resume and checkpoint.exists():
                    cached = load_checkpoint(checkpoint, device_for(args.device))
                    cached_info = cached[3]
                    expected_ids = set(train.rows.id.astype(int))
                    actual_ids = set(
                        cached_info["optimizer_train_ids"]
                        + cached_info["calibration_ids"]
                    )
                    if (
                        cached_info["model_config"] != asdict(model_config)
                        or cached_info["training_config"] != asdict(config)
                        or expected_ids != actual_ids
                        or cached_info["test_ids"] != test.rows.id.astype(int).tolist()
                        or cached_info["audit"].get("fingerprints")
                        != data.audit.get("fingerprints")
                    ):
                        raise ValueError(
                            f"Cannot resume incompatible checkpoint {checkpoint}; use a new output directory"
                        )
                    print("    Reusing compatible checkpoint", flush=True)
                model, prep, t, info = cached or fit_model(train, model_config, config)
                info.update(
                    mode=args.mode,
                    fold=fold,
                    test_ids=test.rows.id.astype(int).tolist(),
                    test_start=str(start.date()),
                )
                p = predict(model, prep, t, test, device_for(args.device))
                chunks.append(
                    prediction_rows(test, p, name, fold, info["train_through"])
                )
                if cached is None:
                    save_checkpoint(checkpoint, model, prep, t, info)
                a, b, _ = chronological_parts(train)
                # Baselines need only one run per market mode and reference fold.
                if architecture == architectures[0]:
                    for baseline, values in baselines(
                        train.take(np.concatenate([a, b])),
                        test,
                        mode,
                        args.baseline_iterations,
                        args.seed,
                    ).items():
                        chunks.append(
                            prediction_rows(
                                test,
                                values,
                                f"{baseline}_{mode}",
                                fold,
                                info["train_through"],
                            )
                        )
                fold_records.append(
                    dict(
                        model=name,
                        fold=fold,
                        train_through=info["train_through"],
                        optimizer_train_through=info["optimizer_train_through"],
                        test_start=str(start.date()),
                        test_ids=info["test_ids"],
                        selected_epoch=info["selected_epoch"],
                    )
                )
                # Write incremental results so an interrupted long run remains reviewable.
                combined = pd.concat(chunks, ignore_index=True)
                if combined.duplicated(["model", "id"]).any():
                    raise ValueError("Duplicate evaluation prediction")
                combined.to_csv(output / "predictions.csv", index=False)
                result = metrics(combined)
                metadata["evaluation_complete"] = (
                    len(fold_records) == metadata["expected_model_folds"]
                )
                (output / "results.json").write_text(
                    json.dumps(
                        dict(metadata=metadata, folds=fold_records, results=result),
                        indent=2,
                    )
                    + "\n"
                )
                write_report(output / "report.md", result, metadata)
    print(f"Saved to {output}", flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--architecture", choices=["features", "history", "all"], default="all"
    )
    p.add_argument("--odds-mode", choices=["stats", "market", "all"], default="all")
    p.add_argument(
        "--mode",
        choices=["holdout", "walk-forward", "production"],
        default="walk-forward",
    )
    p.add_argument("--fights", type=Path, default=ROOT / "fights.csv")
    p.add_argument("--fighters", type=Path, default=ROOT / "fighters.csv")
    p.add_argument("--stats", type=Path, default=ROOT / "fight_stats.csv")
    p.add_argument(
        "--reference-data", type=Path, default=ROOT / "final-dataset-names-and-ids.csv"
    )
    p.add_argument(
        "--output", type=Path, default=ROOT / "models/transformers/artifacts"
    )
    p.add_argument(
        "--before", help="Exclusive data cutoff; required for production fitting"
    )
    p.add_argument("--epochs", type=int, default=100)
    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--patience", type=int, default=10)
    p.add_argument("--device", default="auto")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--initial-train-fraction", type=float, default=0.8)
    p.add_argument("--step", type=int, default=100)
    p.add_argument("--five-round-step", type=int, default=40)
    p.add_argument(
        "--max-folds", type=int, help="Debugging only: run the first N reference folds"
    )
    p.add_argument("--baseline-iterations", type=int, default=200)
    p.add_argument(
        "--scheduled-rounds",
        choices=["all", "3", "5"],
        default="all",
        help="Evaluate only the selected scheduled length; training still pools both lengths",
    )
    p.add_argument(
        "--training-coverage",
        choices=["all", "odds-available"],
        default="all",
        help="Use odds-available training fights for both input variants in a controlled comparison",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Reuse compatible evaluation checkpoints; recompute metrics and baselines",
    )
    args = p.parse_args()
    if (
        min(
            args.epochs,
            args.batch_size,
            args.patience,
            args.threads,
            args.step,
            args.five_round_step,
            args.baseline_iterations,
        )
        < 1
        or not 0 < args.initial_train_fraction < 1
        or (args.max_folds is not None and args.max_folds < 1)
    ):
        p.error(
            "Counts must be positive; initial train fraction must be between zero and one"
        )
    try:
        run(args)
    except (ValueError, OSError) as error:
        p.error(str(error))


if __name__ == "__main__":
    main()
