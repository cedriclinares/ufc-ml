# Model Results

These results are recorded from the training runs completed in this project. Accuracy is classification accuracy; betting return is measured in units using the script's existing odds calculation.

| Run | Features / evaluation | Train accuracy | Test accuracy | Correct | Incorrect | Betting units |
|---|---|---:|---:|---:|---:|---:|
| Historical model | Earlier model and split; details were not preserved | 1.000 | 0.860 | 1,126 | 183 | -11.01 |
| Baseline MLP | Original chronological MLP features | 0.778 | 0.603 | 970 | 638 | -20.06 |
| MLP with ratios and differences | Chronological 80/20 split; matchup ratio and red-minus-blue difference features | 0.830 | 0.595 | 956 | 652 | -9.21 |
| MLP with debut fallback | Added debut flags; rest-time values were unavailable in the older CSV and were imputed | 0.842 | 0.583 | 938 | 670 | -21.99 |
| MLP with IDs and rest time | Chronological 80/20 split; fighter IDs used to calculate days since each fighter's previous bout, plus debut flags | 0.855 | 0.588 | 945 | 663 | **+15.22** |

## Latest confusion matrix

The latest run produced:

```text
[[633, 268],
 [395, 312]]
```

## Interpretation

The rest-time and debut features did not improve raw test accuracy relative to the ratio-and-difference run. They did improve the simulated betting result from **-9.21 units** to **+15.22 units**.

The historical 86.0% result is not directly comparable because its split and feature-processing setup were different and were not preserved in the run output. The latest comparisons use chronological splits, which are more appropriate for fight prediction.

## Baselines and other models to evaluate

These comparison points are now evaluated in the reproducible model comparison below:

- **Majority-class baseline:** always predict the most common winner class. This shows whether the MLP beats a trivial classifier.
- **Betting-favorite baseline:** always choose the fighter with the shorter decimal-implied odds. This is the most relevant benchmark for betting performance.
- **Odds-only model:** use the two moneylines, or their normalized implied probabilities, without fighter statistics.
- **Logistic regression:** a simple linear probability model and a useful check for whether the MLP is learning more than linear relationships.
- **Random forest:** a non-linear tree ensemble that handles interactions and can provide feature importance.
- **Gradient boosting:** often a strong tabular-data baseline and a useful comparison against the neural network.
- **Calibrated models:** compare probability calibration and betting returns, not only classification accuracy.

Each comparison should use the same chronological train/test dates and the same bet-settlement rules. Results should include test accuracy, confusion matrix, calibration, number of bets, and betting units.

<!-- model-comparison:start -->
## Reproducible model comparison

Run: 2026-09-22T23:47:35.993185+00:00. Dataset: `final-dataset-names-and-ids.csv`; SHA-256: `26dc6f7982f7eeeb6e49eef154a61c28f374e40ed068c2390c7368d4decc194d`.

Train: ['2002-11-22', '2023-07-29'] (6,430 fights). Test: ['2023-08-05', '2026-09-19'] (1,610 fights). The chronological 80/20 boundary moves to the start of its event date.

All models use the same test fights. One unit is staked on each predicted winner; wins earn decimal odds minus one, losses cost one unit. Exact ties select Red. Both odds must be valid. No confidence threshold or value-bet filter is applied.

Statistics models use the explicit MLP feature list, matchup ratios/differences, and rest time across both corners. IDs and names are excluded from predictors; odds are used only by the favorite and odds-only models. Preprocessing is fitted only on training data. Debut means first appearance in this dataset. Exported statistics are assumed to be pre-fight; this runner does not independently reconstruct them.

Calibrated variants fit their base model before 2020-12-19, then fit sigmoid calibration on the final 1,294 training fights. Thus their base models see fewer fights than uncalibrated models. Test outcomes never fit calibration.

Brier score and log loss use Blue probabilities; lower is better. ECE uses 10 equal-width probability bins. The favorite baseline uses normalized market-implied probabilities; the majority baseline uses training class frequencies for probability metrics. Confusion matrices list actual rows and predicted columns in [Red, Blue] order.

| Model | Train accuracy | Test accuracy | Brier | Log loss | ECE | Bets | Units | ROI | Confusion matrix |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| betting_favorite | 0.6600 | 0.6994 | 0.1991 | 0.5837 | 0.0352 | 1,610 | -3.05 | -0.19% | `[[695, 208], [276, 431]]` |
| majority_class | 0.6317 | 0.5609 | 0.2513 | 0.6963 | 0.0709 | 1,610 | -36.18 | -2.25% | `[[903, 0], [707, 0]]` |
| odds_only | 0.6701 | 0.6882 | 0.2025 | 0.5905 | 0.0619 | 1,610 | -13.50 | -0.84% | `[[757, 146], [356, 351]]` |
| logistic_regression | 0.6726 | 0.6280 | 0.2289 | 0.6535 | 0.0405 | 1,610 | +1.04 | 0.06% | `[[704, 199], [400, 307]]` |
| logistic_regression_calibrated | 0.6591 | 0.6168 | 0.2323 | 0.6568 | 0.0324 | 1,610 | -2.06 | -0.13% | `[[765, 138], [479, 228]]` |
| random_forest | 0.9639 | 0.6112 | 0.2307 | 0.6533 | 0.0394 | 1,610 | -31.93 | -1.98% | `[[784, 119], [507, 200]]` |
| random_forest_calibrated | 0.8899 | 0.6149 | 0.2315 | 0.6551 | 0.0269 | 1,610 | -4.24 | -0.26% | `[[760, 143], [477, 230]]` |
| gradient_boosting | 0.8188 | 0.6317 | 0.2266 | 0.6450 | 0.0252 | 1,610 | +1.91 | 0.12% | `[[735, 168], [425, 282]]` |
| gradient_boosting_calibrated | 0.7824 | 0.6193 | 0.2314 | 0.6550 | 0.0218 | 1,610 | -15.52 | -0.96% | `[[748, 155], [458, 249]]` |
| mlp | 0.8006 | 0.6012 | 0.2472 | 0.7116 | 0.1020 | 1,610 | -12.72 | -0.79% | `[[647, 256], [386, 321]]` |

These results complete the proposed comparisons above. Historical runs above are not directly comparable because their split and rest-time implementation differ. These are single held-out-period results, without hyperparameter search or uncertainty estimates.

Reproduce: `.venv/bin/python -m models.comparisons.train`. See `models/comparisons/README.md` for artifacts and dependencies.
<!-- model-comparison:end -->
