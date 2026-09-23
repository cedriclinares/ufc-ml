# Model comparisons

From the repository root:

```sh
.venv/bin/python -m models.comparisons.train
.venv/bin/python -m unittest discover -s tests -p 'test_model_comparisons.py'
```

The runner trains majority-class, odds-only logistic regression, full-feature logistic regression, random forest, histogram gradient boosting, and MLP models, plus sigmoid-calibrated logistic regression, random forest, and gradient boosting variants. It also evaluates the betting-favorite rule. All randomness uses seed 0. Model parameters and package versions are recorded in `artifacts/results.json`.

Optional arguments: `--data PATH`, `--output DIRECTORY`, `--report PATH`. Defaults use the repository's `final-dataset-names-and-ids.csv`, this directory's `artifacts/`, and the root `model_results.md`. The report's generated section is replaced on reruns; historical results are preserved. Artifacts in the selected output directory are overwritten.

Requires NumPy, pandas, scikit-learn >= 1.6 (for FrozenEstimator), joblib, and threadpoolctl. The completed run's exact versions are recorded in results.json.

Files:

- `features.py`: explicit pre-fight feature allowlist.
- `evaluation.py`: shared date split, feature construction, preprocessing, and settlement.
- `train.py`: estimator definitions, chronological calibration, training, and reports.
- `artifacts/results.json`: metrics, calibration bins, dates, parameters, and dataset fingerprint.
- `artifacts/predictions.csv`: per-fight probabilities, picks, and betting profits for audit.
- `artifacts/*.joblib`: fitted models including preprocessing (generated locally and ignored by Git).

To predict with a statistics model, build features with `make_features` using chronological history including the new fights, then pass the desired rows to the saved pipeline. Odds-only and majority models expect `red_implied` and `blue_implied` columns normalized to sum to one. There is no serialized estimator for the favorite rule. Only load trusted joblib files.

Historical results are not directly comparable: the new split keeps dates intact, and rest time follows fighters across both corners. Calibration reserves the last 20% of the training period; base models for calibrated variants therefore train on fewer rows. This evaluates held-out calibration performance but does not isolate calibration from reduced base training data. No tuning uses the test period. All settlement uses one-unit stakes on every test fight, with exact prediction ties selecting Red.
