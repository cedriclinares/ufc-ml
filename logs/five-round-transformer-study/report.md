# Five-round history transformer versus GBT

Evaluation: 150 unique fights on 122 event dates, 2023-10-14 through 2026-09-19.
Seeds: [0, 1, 2]. Four chronological test folds per variant and seed.
Both variants use identical odds-available training, validation, calibration and test IDs. Training pools three- and five-round fights.
Transformer epoch ceiling 100, patience 10; validation chooses refit epochs. GBT uses 200 iterations. All models retain the same optimizer-fit partition.
Transformer heads receive temperature calibration on held-out pre-test dates. The report includes both the original uncalibrated GBT and GBT with temperature calibration on exactly the same calibration IDs.
Early stopping minimizes the combined multitask validation loss over pooled three- and five-round fights; it is not tuned specifically to five-round winner or duration accuracy.
The GBT receives reconstructed summaries and context; the history transformer also receives ten-fight sequences. This compares complete models, not architecture with identical representations.

## Winner predictions

Values are mean ± sample standard deviation across seeds. Seed runs share the same test fights; sample size is not multiplied by three.

| Model | Accuracy | Log loss | Brier |
|---|---:|---:|---:|
| history_stats | 66.0% ± 2.4 pp | 0.6311 ± 0.0121 | 0.2192 ± 0.0060 |
| history_market | 66.0% ± 1.3 pp | 0.6238 ± 0.0045 | 0.2168 ± 0.0020 |
| tree_stats | 64.0% ± 0.0 pp | 0.6523 ± 0.0000 | 0.2296 ± 0.0000 |
| tree_market | 62.0% ± 0.0 pp | 0.6417 ± 0.0000 | 0.2262 ± 0.0000 |
| tree_calibrated_stats | 64.0% ± 0.0 pp | 0.6484 ± 0.0000 | 0.2280 ± 0.0000 |
| tree_calibrated_market | 62.0% ± 0.0 pp | 0.6346 ± 0.0000 | 0.2230 ± 0.0000 |
| prior_stats | 56.7% ± 0.0 pp | 0.7057 ± 0.0000 | 0.2555 ± 0.0000 |
| Odds alone (normalized implied probabilities) | 65.3% | 0.6154 | 0.2143 |

## Duration predictions

Exact accuracy and MAE use the most probable half-round category. Actual duration is ceiling-rounded to half-rounds.

| Model | Exact accuracy | MAE (rounds) | Duration log loss | Survival Brier |
|---|---:|---:|---:|---:|
| history_stats | 48.7% ± 0.7 pp | 1.4678 ± 0.0135 | 1.7449 ± 0.0266 | 0.1649 ± 0.0003 |
| history_market | 47.6% ± 1.7 pp | 1.4789 ± 0.0587 | 1.7280 ± 0.0032 | 0.1635 ± 0.0015 |
| tree_stats | 48.7% ± 0.0 pp | 1.3967 ± 0.0000 | 1.9121 ± 0.0000 | 0.1728 ± 0.0000 |
| tree_market | 48.7% ± 0.0 pp | 1.3800 ± 0.0000 | 1.9035 ± 0.0000 | 0.1714 ± 0.0000 |
| tree_calibrated_stats | 48.7% ± 0.0 pp | 1.3967 ± 0.0000 | 1.7791 ± 0.0000 | 0.1722 ± 0.0000 |
| tree_calibrated_market | 48.7% ± 0.0 pp | 1.3800 ± 0.0000 | 1.7758 ± 0.0000 | 0.1724 ± 0.0000 |
| prior_stats | 48.0% ± 0.0 pp | 1.5800 ± 0.0000 | 1.7878 ± 0.0000 | 0.1902 ± 0.0000 |

Survival Brier averages squared probability error across all half-round cutoffs below five rounds; lower is better.

| Model | Over 2.5 Brier | Over 3.5 Brier | Over 4.5 Brier | Distance accuracy | Distance Brier |
|---|---:|---:|---:|---:|---:|
| history_stats | 0.2176 ± 0.0017 | 0.2235 ± 0.0017 | 0.2202 ± 0.0024 | 66.4% ± 3.4 pp | 0.2230 ± 0.0036 |
| history_market | 0.2122 ± 0.0031 | 0.2231 ± 0.0032 | 0.2193 ± 0.0034 | 64.0% ± 1.2 pp | 0.2259 ± 0.0038 |
| tree_stats | 0.2105 ± 0.0000 | 0.2417 ± 0.0000 | 0.2307 ± 0.0000 | 64.7% ± 0.0 pp | 0.2221 ± 0.0000 |
| tree_market | 0.2097 ± 0.0000 | 0.2398 ± 0.0000 | 0.2279 ± 0.0000 | 68.0% ± 0.0 pp | 0.2108 ± 0.0000 |
| tree_calibrated_stats | 0.2120 ± 0.0000 | 0.2392 ± 0.0000 | 0.2240 ± 0.0000 | 64.7% ± 0.0 pp | 0.2237 ± 0.0000 |
| tree_calibrated_market | 0.2137 ± 0.0000 | 0.2396 ± 0.0000 | 0.2243 ± 0.0000 | 68.0% ± 0.0 pp | 0.2136 ± 0.0000 |
| prior_stats | 0.2439 ± 0.0000 | 0.2552 ± 0.0000 | 0.2571 ± 0.0000 | 54.0% ± 0.0 pp | 0.2574 ± 0.0000 |

## Paired differences and uncertainty

Differences are first model minus second, averaged across seeds for each fight. Positive accuracy differences favor the first model; negative error differences favor it.
95% intervals use 10,000 paired event-date cluster bootstrap samples. They describe test-sample uncertainty conditional on these fits, not a full retraining bootstrap. Intervals are exploratory and unadjusted for multiple comparisons.

| Comparison | Metric | Difference | 95% interval |
|---|---|---:|---:|
| history_stats minus tree_stats | winner_accuracy | +2.0000 pp | [-6.1002, +10.3290] pp |
| history_stats minus tree_stats | winner_log_loss | -0.0212 | [-0.0848, +0.0387] |
| history_stats minus tree_stats | winner_brier | -0.0104 | [-0.0387, +0.0164] |
| history_stats minus tree_stats | duration_accuracy | -0.0000 pp | [-4.1667, +3.9080] pp |
| history_stats minus tree_stats | duration_mae | +0.0711 | [-0.0844, +0.2386] |
| history_stats minus tree_stats | duration_survival_brier | -0.0079 | [-0.0263, +0.0099] |
| history_stats minus tree_stats | over_4.5_brier | -0.0105 | [-0.0453, +0.0221] |
| history_market minus tree_market | winner_accuracy | +4.0000 pp | [-3.4019, +11.5306] pp |
| history_market minus tree_market | winner_log_loss | -0.0179 | [-0.0777, +0.0388] |
| history_market minus tree_market | winner_brier | -0.0094 | [-0.0354, +0.0151] |
| history_market minus tree_market | duration_accuracy | -1.1111 pp | [-5.4795, +3.0303] pp |
| history_market minus tree_market | duration_mae | +0.0989 | [-0.0785, +0.2820] |
| history_market minus tree_market | duration_survival_brier | -0.0079 | [-0.0259, +0.0090] |
| history_market minus tree_market | over_4.5_brier | -0.0086 | [-0.0425, +0.0239] |
| history_market minus history_stats | winner_accuracy | +0.0000 pp | [-3.5320, +3.4632] pp |
| history_market minus history_stats | winner_log_loss | -0.0072 | [-0.0225, +0.0076] |
| history_market minus history_stats | winner_brier | -0.0024 | [-0.0091, +0.0043] |
| history_market minus history_stats | duration_accuracy | -1.1111 pp | [-2.8698, +0.4474] pp |
| history_market minus history_stats | duration_mae | +0.0111 | [-0.0476, +0.0743] |
| history_market minus history_stats | duration_survival_brier | -0.0014 | [-0.0052, +0.0023] |
| history_market minus history_stats | over_4.5_brier | -0.0009 | [-0.0075, +0.0058] |
| history_stats minus tree_market | winner_accuracy | +4.0000 pp | [-3.1404, +10.9589] pp |
| history_stats minus tree_market | winner_log_loss | -0.0106 | [-0.0737, +0.0495] |
| history_stats minus tree_market | winner_brier | -0.0070 | [-0.0343, +0.0190] |
| history_stats minus tree_market | duration_accuracy | -0.0000 pp | [-4.3084, +4.2559] pp |
| history_stats minus tree_market | duration_mae | +0.0878 | [-0.0948, +0.2714] |
| history_stats minus tree_market | duration_survival_brier | -0.0065 | [-0.0246, +0.0105] |
| history_stats minus tree_market | over_4.5_brier | -0.0077 | [-0.0420, +0.0239] |
| history_stats minus tree_calibrated_stats | winner_accuracy | +2.0000 pp | [-6.1002, +10.3290] pp |
| history_stats minus tree_calibrated_stats | winner_log_loss | -0.0173 | [-0.0687, +0.0332] |
| history_stats minus tree_calibrated_stats | winner_brier | -0.0088 | [-0.0325, +0.0144] |
| history_stats minus tree_calibrated_stats | duration_accuracy | -0.0000 pp | [-4.1667, +3.9080] pp |
| history_stats minus tree_calibrated_stats | duration_mae | +0.0711 | [-0.0844, +0.2386] |
| history_stats minus tree_calibrated_stats | duration_survival_brier | -0.0073 | [-0.0229, +0.0083] |
| history_stats minus tree_calibrated_stats | over_4.5_brier | -0.0038 | [-0.0286, +0.0206] |
| history_market minus tree_calibrated_market | winner_accuracy | +4.0000 pp | [-3.4019, +11.5306] pp |
| history_market minus tree_calibrated_market | winner_log_loss | -0.0108 | [-0.0603, +0.0371] |
| history_market minus tree_calibrated_market | winner_brier | -0.0062 | [-0.0285, +0.0153] |
| history_market minus tree_calibrated_market | duration_accuracy | -1.1111 pp | [-5.4795, +3.0303] pp |
| history_market minus tree_calibrated_market | duration_mae | +0.0989 | [-0.0785, +0.2820] |
| history_market minus tree_calibrated_market | duration_survival_brier | -0.0089 | [-0.0238, +0.0061] |
| history_market minus tree_calibrated_market | over_4.5_brier | -0.0050 | [-0.0284, +0.0189] |
| history_stats minus tree_calibrated_market | winner_accuracy | +4.0000 pp | [-3.1404, +10.9589] pp |
| history_stats minus tree_calibrated_market | winner_log_loss | -0.0035 | [-0.0565, +0.0480] |
| history_stats minus tree_calibrated_market | winner_brier | -0.0038 | [-0.0276, +0.0193] |
| history_stats minus tree_calibrated_market | duration_accuracy | -0.0000 pp | [-4.3084, +4.2559] pp |
| history_stats minus tree_calibrated_market | duration_mae | +0.0878 | [-0.0948, +0.2714] |
| history_stats minus tree_calibrated_market | duration_survival_brier | -0.0074 | [-0.0230, +0.0078] |
| history_stats minus tree_calibrated_market | over_4.5_brier | -0.0041 | [-0.0272, +0.0189] |
| history_stats minus odds_alone | winner_accuracy | +0.6667 pp | [-6.7568, +8.1679] pp |
| history_stats minus odds_alone | winner_log_loss | +0.0157 | [-0.0237, +0.0554] |
| history_stats minus odds_alone | winner_brier | +0.0049 | [-0.0131, +0.0230] |
| history_market minus odds_alone | winner_accuracy | +0.6667 pp | [-7.0640, +8.6582] pp |
| history_market minus odds_alone | winner_log_loss | +0.0085 | [-0.0291, +0.0465] |
| history_market minus odds_alone | winner_brier | +0.0025 | [-0.0151, +0.0203] |

## Seed probability ensembles (additional diagnostic)

These average the three seeds' probabilities before selecting classes; they are different models from the mean single-seed results above.

| Model | Winner accuracy | Winner log loss | Duration exact | Duration MAE | Survival Brier |
|---|---:|---:|---:|---:|---:|
| history_stats | 69.3% | 0.6263 | 48.0% | 1.5033 | 0.1634 |
| history_market | 67.3% | 0.6183 | 48.7% | 1.4700 | 0.1620 |
| tree_stats | 64.0% | 0.6523 | 48.7% | 1.3967 | 0.1728 |
| tree_market | 62.0% | 0.6417 | 48.7% | 1.3800 | 0.1714 |
| tree_calibrated_stats | 64.0% | 0.6484 | 48.7% | 1.3967 | 0.1722 |
| tree_calibrated_market | 62.0% | 0.6346 | 48.7% | 1.3800 | 0.1724 |
| prior_stats | 56.7% | 0.7057 | 48.0% | 1.5800 | 0.1902 |

Paired ensemble differences versus calibrated market GBT use the same event-date bootstrap:

| Ensemble | Metric | Difference | 95% interval |
|---|---|---:|---:|
| ensemble_history_stats minus tree_calibrated_market | winner_accuracy | +7.3333 pp | [-0.6536, +14.9351] pp |
| ensemble_history_stats minus tree_calibrated_market | winner_log_loss | -0.0083 | [-0.0613, +0.0430] |
| ensemble_history_stats minus tree_calibrated_market | duration_mae | +0.1233 | [-0.0700, +0.3207] |
| ensemble_history_stats minus tree_calibrated_market | duration_survival_brier | -0.0090 | [-0.0245, +0.0061] |
| ensemble_history_market minus tree_calibrated_market | winner_accuracy | +5.3333 pp | [-2.0690, +12.8383] pp |
| ensemble_history_market minus tree_calibrated_market | winner_log_loss | -0.0163 | [-0.0655, +0.0311] |
| ensemble_history_market minus tree_calibrated_market | duration_mae | +0.0900 | [-0.1136, +0.3007] |
| ensemble_history_market minus tree_calibrated_market | duration_survival_brier | -0.0104 | [-0.0253, +0.0047] |

## Results by test fold

Winner accuracy and duration MAE below are means across seeds; GBT uses market inputs. Calibration preserves GBT class predictions.

| Fold | Dates | Fights | History stats win | History market win | GBT market win |
|---|---|---:|---:|---:|---:|
| 5r-1 | 2023-10-14 to 2024-07-20 | 40 | 75.8% | 76.7% | 60.0% |
| 5r-2 | 2024-07-27 to 2025-04-05 | 39 | 67.5% | 68.4% | 61.5% |
| 5r-3 | 2025-04-12 to 2026-02-07 | 40 | 59.2% | 53.3% | 65.0% |
| 5r-4 | 2026-02-21 to 2026-09-19 | 31 | 60.2% | 65.6% | 61.3% |

| Fold | History stats MAE | History market MAE | GBT market MAE |
|---|---:|---:|---:|
| 5r-1 | 1.3500 | 1.3875 | 1.2875 |
| 5r-2 | 1.2350 | 1.3077 | 1.2051 |
| 5r-3 | 1.6083 | 1.5750 | 1.5250 |
| 5r-4 | 1.7312 | 1.6882 | 1.5323 |

## Selected training epochs

| Seed | Variant | Fold | Selected epoch | Stopped epoch | Optimizer fights | Calibration fights | Test fights |
|---:|---|---|---:|---:|---:|---:|---:|
| 0 | stats | 5r-1 | 6 | 16 | 5658 | 1005 | 40 |
| 0 | market | 5r-1 | 5 | 15 | 5658 | 1005 | 40 |
| 0 | stats | 5r-2 | 4 | 14 | 5995 | 1060 | 39 |
| 0 | market | 5r-2 | 4 | 14 | 5995 | 1060 | 39 |
| 0 | stats | 5r-3 | 3 | 13 | 6306 | 1113 | 40 |
| 0 | market | 5r-3 | 7 | 17 | 6306 | 1113 | 40 |
| 0 | stats | 5r-4 | 4 | 14 | 6653 | 1179 | 31 |
| 0 | market | 5r-4 | 6 | 16 | 6653 | 1179 | 31 |
| 1 | stats | 5r-1 | 3 | 13 | 5658 | 1005 | 40 |
| 1 | market | 5r-1 | 5 | 15 | 5658 | 1005 | 40 |
| 1 | stats | 5r-2 | 3 | 13 | 5995 | 1060 | 39 |
| 1 | market | 5r-2 | 6 | 16 | 5995 | 1060 | 39 |
| 1 | stats | 5r-3 | 2 | 12 | 6306 | 1113 | 40 |
| 1 | market | 5r-3 | 3 | 13 | 6306 | 1113 | 40 |
| 1 | stats | 5r-4 | 6 | 16 | 6653 | 1179 | 31 |
| 1 | market | 5r-4 | 4 | 14 | 6653 | 1179 | 31 |
| 2 | stats | 5r-1 | 4 | 14 | 5658 | 1005 | 40 |
| 2 | market | 5r-1 | 9 | 19 | 5658 | 1005 | 40 |
| 2 | stats | 5r-2 | 4 | 14 | 5995 | 1060 | 39 |
| 2 | market | 5r-2 | 3 | 13 | 5995 | 1060 | 39 |
| 2 | stats | 5r-3 | 4 | 14 | 6306 | 1113 | 40 |
| 2 | market | 5r-3 | 3 | 13 | 6306 | 1113 | 40 |
| 2 | stats | 5r-4 | 4 | 14 | 6653 | 1179 | 31 |
| 2 | market | 5r-4 | 6 | 16 | 6653 | 1179 | 31 |

## Interpretation

The lowest mean single-seed winner log loss among trained candidates is history_market: 0.6238. Odds alone scores 0.6154.
The lowest mean duration MAE is tree_market: 1.3800 rounds. Temperature calibration preserves its most probable class, so raw and calibrated GBT point predictions tie.
Neither history variant establishes lower winner log loss than calibrated market GBT in the paired 95% intervals; both intervals include zero.
Duration probability scores should guide an over/under assessment alongside MAE; a better modal duration estimate does not automatically imply better cutoff probabilities. See the cutoff Brier scores and their paired intervals in results.json.
Keep production betting models unchanged while evaluating challengers on future unseen fights. This study does not establish a profitable deployment rule or optimized EV thresholds.

## Limits and provenance

All common-coverage, ID-disjointness, chronological cutoff and probability checks passed. Seed directories retain checkpoints, coverage hashes, folds and per-fight predictions; results.json stores all metric values and intervals.
These test fights were already examined in the initial study, so this is a follow-up on an existing holdout, not an independent confirmation set. Early stopping and temperature calibration use pre-test dates, never these test outcomes.
Earlier duration GBT results (five-round MAE 1.307) used a different dataset/feature representation and schedule-specific training; they are not part of the controlled comparison above.
Historical odds capture timestamps and static fighter measurements are unverified. No betting ROI is established; historical over/under prices are unavailable in this study. Distance and duration are independent heads.
The choice of a five-round-specific deployment policy must still be tested on future, unseen fights. Do not infer statistical significance from a higher point estimate alone.
