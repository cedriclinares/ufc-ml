"""Give study GBTs the same pre-test temperature-calibration opportunity as transformers."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
import torch

from models.transformers.data import load_data
from models.transformers.evaluation import baselines, prediction_rows
from models.transformers.network import probabilities
from models.transformers.training import calibrate, load_checkpoint


def logit_probabilities(values):
    result = {}
    for task in ("winner", "distance"):
        p = np.clip(values[task], 1e-15, 1 - 1e-15)
        result[task] = torch.tensor(np.log(p) - np.log1p(-p), dtype=torch.float64)
    with np.errstate(divide="ignore"):
        result["rounds"] = torch.tensor(np.log(values["rounds"]), dtype=torch.float64)
    return result


def run(root, seeds):
    data = load_data(
        ROOT / "fights.csv", ROOT / "fighters.csv", ROOT / "fight_stats.csv"
    )
    by_id = {int(fid): i for i, fid in enumerate(data.rows.id)}
    torch.set_num_threads(4)
    for seed in seeds:
        directory = root / f"seed-{seed}" / "walk-forward"
        result = json.loads((directory / "results.json").read_text())
        if not result["metadata"]["evaluation_complete"]:
            raise ValueError(f"Incomplete run: {directory}")
        existing = pd.read_csv(directory / "predictions.csv")
        rows, records = [], []
        for mode in ("stats", "market"):
            for fold in sorted(existing.fold.unique()):
                _, _, _, info = load_checkpoint(
                    directory / "checkpoints" / f"history_{mode}_{fold}.pt"
                )
                train = data.take([by_id[i] for i in info["optimizer_train_ids"]])
                calibration = data.take([by_id[i] for i in info["calibration_ids"]])
                test = data.take([by_id[i] for i in info["test_ids"]])
                combined = data.take(
                    [by_id[i] for i in info["calibration_ids"] + info["test_ids"]]
                )
                values = baselines(
                    train,
                    combined,
                    mode,
                    result["metadata"]["baseline_iterations"],
                    seed,
                )["tree"]
                logits = logit_probabilities(values)
                split = len(calibration)
                temperatures, notes = calibrate(
                    {k: v[:split] for k, v in logits.items()}, calibration
                )
                raw_test = {k: v[split:] for k, v in values.items()}
                expected = (
                    existing[existing.model.eq(f"tree_{mode}") & existing.fold.eq(fold)]
                    .set_index("id")
                    .loc[test.rows.id]
                )
                if not np.allclose(
                    raw_test["winner"], expected.p_red.to_numpy(), atol=1e-12
                ):
                    raise ValueError(
                        "Refitted GBT winner probabilities differ from the original study fit"
                    )
                if not np.allclose(
                    raw_test["distance"], expected.p_distance.to_numpy(), atol=1e-12
                ):
                    raise ValueError(
                        "Refitted GBT distance probabilities differ from the original study fit"
                    )
                if not np.allclose(
                    raw_test["rounds"],
                    expected[[f"p_{i / 2:.1f}" for i in range(11)]].to_numpy(),
                    atol=1e-12,
                ):
                    raise ValueError(
                        "Refitted GBT duration probabilities differ from the original study fit"
                    )
                calibrated = {
                    k: v.numpy()
                    for k, v in probabilities(
                        {k: v[split:] for k, v in logits.items()}, temperatures
                    ).items()
                }
                rows.append(
                    prediction_rows(
                        test,
                        calibrated,
                        f"tree_calibrated_{mode}",
                        fold,
                        info["train_through"],
                    )
                )
                records.append(
                    dict(
                        mode=mode,
                        fold=fold,
                        seed=seed,
                        temperatures=temperatures,
                        notes=notes,
                        calibration_ids=info["calibration_ids"],
                        optimizer_train_ids=info["optimizer_train_ids"],
                        test_ids=info["test_ids"],
                    )
                )
                print(
                    f"Calibrated seed {seed} {mode} {fold}: {temperatures}", flush=True
                )
        pd.concat(rows, ignore_index=True).to_csv(
            directory / "calibrated-tree-predictions.csv", index=False
        )
        (directory / "tree-calibration.json").write_text(
            json.dumps(records, indent=2) + "\n"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT / "models/transformers/artifacts/five-round-study",
    )
    parser.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2])
    args = parser.parse_args()
    run(args.root, args.seeds)


if __name__ == "__main__":
    main()
