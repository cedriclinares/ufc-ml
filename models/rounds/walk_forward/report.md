# Walk-forward round-duration evaluation

Initial training fraction: 80%. Steps: 100 three-round bouts / 40 five-round bouts.
Event dates stay intact. All metrics pool one out-of-sample prediction per evaluated fight and model.

| Rounds | Model | Exact | Within 0.5 | MAE | Log loss | Confidence ECE | Survival Brier |
|---:|---|---:|---:|---:|---:|---:|---:|
| 3 | majority_baseline | 53.5% | 57.9% | 0.802 | — | — | — |
| 3 | median_baseline | 53.5% | 57.9% | 0.802 | — | — | — |
| 3 | classifier_base | 53.9% | 58.9% | 0.787 | 1.4349 | 0.0457 | 0.1627 |
| 3 | classifier_duration | 53.1% | 58.2% | 0.797 | 1.4346 | 0.0615 | 0.1635 |
| 3 | regression_duration | 13.2% | 54.8% | 0.786 | — | — | — |
| 3 | survival_duration | 53.9% | 59.1% | 0.778 | 1.4364 | 0.0547 | 0.1610 |
| 5 | majority_baseline | 48.0% | 49.3% | 1.580 | — | — | — |
| 5 | median_baseline | 6.0% | 13.3% | 1.627 | — | — | — |
| 5 | classifier_base | 46.7% | 52.7% | 1.393 | 2.1492 | 0.2467 | 0.1894 |
| 5 | classifier_duration | 50.7% | 56.0% | 1.307 | 2.2122 | 0.2358 | 0.1909 |
| 5 | regression_duration | 7.3% | 24.7% | 1.410 | — | — | — |
| 5 | survival_duration | 46.7% | 49.3% | 1.510 | 2.2518 | 0.2834 | 0.2068 |

Models use the same configurations as the chronological holdout. No tuning uses evaluation outcomes.
Last-fold saved models exclude that fold’s test outcomes. Existing exported statistics are assumed pre-fight.
