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

<!-- wf-comparison:start -->
## Walk-forward value betting comparison

Run: 2026-09-23T23:15:25.838607+00:00. Dataset: `final-dataset-names-and-ids.csv`; SHA-256: `26dc6f7982f7eeeb...`.

Train: ['2002-11-22', '2023-07-29'] (6,430 fights). Test: ['2023-08-05', '2026-09-19'] (1,610 fights).

Walk-forward: min_train=4000, step=100. Models retrain on expanding windows aligned to event dates.

Value betting uses EV-based side selection: for each fight, EV = model_prob * decimal_odds - 1 is computed for both sides, and the side with highest EV is bet when it exceeds the threshold. This correctly finds underdog value.

EV threshold selected on training-period walk-forward CV (maximizing betting units). Ensemble models average probabilities from multiple base models before applying value betting.

| Model | ELO | Odds | CV threshold | Flat acc | Flat units | Value acc | Value bets | Value units | Value ROI |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| gb_deep_multielo_odds | yes | yes | 0.50 | 0.6807 | -11.07 | 0.3594 | 64 | +9.50 | 14.84% |
| gb_conservative_elo_odds | yes | yes | 0.40 | 0.6832 | -5.91 | 0.3448 | 87 | +11.92 | 13.70% |
| gb_med_multielo_odds | yes | yes | 0.20 | 0.6764 | -9.08 | 0.4794 | 436 | +19.00 | 4.36% |
| gb_wide_multielo_odds | yes | yes | 0.40 | 0.6826 | -7.47 | 0.3774 | 106 | +7.21 | 6.80% |
| gb_shallow_multielo_odds | yes | yes | 0.50 | 0.6919 | +13.95 | 0.2917 | 48 | +2.42 | 5.04% |
| gb_v_shallow_multielo_odds | yes | yes | 0.40 | 0.6901 | -2.39 | 0.2708 | 96 | +12.95 | 13.49% |
| gb_mid_deep_multielo_odds | yes | yes | 0.40 | 0.6876 | +14.37 | 0.3211 | 109 | -4.73 | -4.34% |
| lr_multielo_odds | yes | yes | 0.50 | 0.6839 | +4.46 | 0.3293 | 82 | -2.71 | -3.30% |
| gb_shallow_reduced_odds | yes | yes | 0.40 | 0.6925 | +13.53 | 0.2500 | 80 | -8.38 | -10.48% |
| gb_med_reduced_odds | yes | yes | 0.40 | 0.6832 | +16.30 | 0.3889 | 144 | +16.75 | 11.63% |
| rf_multielo_odds | yes | yes | 0.50 | 0.6652 | +3.21 | 0.2177 | 248 | -14.65 | -5.91% |
| gb_cal_multielo_odds | yes | yes | 0.50 | 0.6826 | +14.05 | 0.2623 | 183 | +14.00 | 7.65% |
| ensemble_all | yes | yes | 0.50 | 0.6876 | +0.16 | 0.2881 | 59 | +10.10 | 17.12% |
| ensemble_top3 | yes | yes | 0.20 | 0.6807 | -7.21 | 0.3695 | 341 | -0.19 | -0.06% |

### Threshold landscape for gb_med_multielo_odds

| Threshold | Bets | Units | ROI | Accuracy |
|---:|---:|---:|---:|---:|
| 0.00 | 1,391 | +2.86 | 0.21% | 0.5938 |
| 0.02 | 1,285 | -8.23 | -0.64% | 0.5829 |
| 0.05 | 1,113 | -1.09 | -0.10% | 0.5696 |
| 0.08 | 929 | -11.44 | -1.23% | 0.5436 |
| 0.10 | 831 | -14.99 | -1.80% | 0.5295 |
| 0.12 | 732 | +4.29 | 0.59% | 0.5273 |
| 0.15 | 586 | -11.69 | -1.99% | 0.4881 |
| 0.18 | 496 | +2.13 | 0.43% | 0.4778 |
| 0.20 | 436 | +19.00 | 4.36% | 0.4794 |
| 0.25 | 309 | +16.82 | 5.44% | 0.4401 |
| 0.30 | 234 | +26.75 | 11.43% | 0.4316 |
| 0.40 | 156 | +29.74 | 19.06% | 0.4231 |
| 0.50 | 102 | +11.08 | 10.86% | 0.3725 |

### Best Kelly-filtered strategy

Model: **gb_med_multielo_odds**, params: `t0.4_f10.0`
- Units: **+108.44**
- Bets: 156, Total wagered: 566.0
- ROI: 19.16%, Avg bet size: 3.628

### Top 10 Kelly-filtered strategies

| Model | Params | Bets | Units | Wagered | ROI |
|---|---|---:|---:|---:|---:|
| gb_med_multielo_odds | t0.4_f10.0 | 156 | +108.44 | 566.0 | 19.16% |
| gb_med_multielo_odds | t0.2_f10.0 | 436 | +103.64 | 1524.3 | 6.80% |
| gb_med_multielo_odds | t0.3_f10.0 | 234 | +100.39 | 834.9 | 12.03% |
| gb_med_reduced_odds | t0.5_f10.0 | 98 | +93.47 | 386.8 | 24.17% |
| gb_med_multielo_odds | t0.4_f8.0 | 156 | +86.76 | 452.8 | 19.16% |
| gb_med_multielo_odds | t0.2_f8.0 | 436 | +82.91 | 1219.4 | 6.80% |
| gb_med_multielo_odds | t0.3_f8.0 | 234 | +80.31 | 667.9 | 12.03% |
| gb_med_multielo_odds | t0.12_f10.0 | 732 | +77.00 | 2469.9 | 3.12% |
| gb_wide_multielo_odds | t0.25_f10.0 | 229 | +76.65 | 628.1 | 12.20% |
| lr_multielo_odds | t0.25_f10.0 | 310 | +76.01 | 956.4 | 7.95% |

Reproduce: `.venv/bin/python -m models.comparisons.train_wf`. See `models/comparisons/artifacts/wf_results.json` for full details.
<!-- wf-comparison:end -->
