# Round-duration model comparison

Chronological holdout; targets and predictions use ceiling to half-rounds.

| Scheduled rounds | Model | Exact accuracy | Within 0.5 | MAE (rounds) | Log loss | Confidence ECE | Survival Brier |
|---:|---|---:|---:|---:|---:|---:|---:|
| 3 | majority_baseline | 53.5% | 57.9% | 0.802 | — | — | — |
| 3 | median_baseline | 53.5% | 57.9% | 0.802 | — | — | — |
| 3 | classifier_base | 53.0% | 58.2% | 0.798 | 1.4552 | 0.0573 | 0.1649 |
| 3 | classifier_duration | 52.8% | 58.0% | 0.799 | 1.4494 | 0.0638 | 0.1646 |
| 3 | regression_duration | 12.1% | 53.6% | 0.796 | — | — | — |
| 3 | survival_duration | 53.2% | 58.2% | 0.801 | 1.4597 | 0.0664 | 0.1637 |
| 5 | majority_baseline | 48.0% | 49.3% | 1.580 | — | — | — |
| 5 | median_baseline | 6.0% | 13.3% | 1.627 | — | — | — |
| 5 | classifier_base | 47.3% | 54.0% | 1.393 | 2.1742 | 0.2370 | 0.1871 |
| 5 | classifier_duration | 47.3% | 53.3% | 1.400 | 2.1815 | 0.2477 | 0.1909 |
| 5 | regression_duration | 6.7% | 25.3% | 1.417 | — | — | — |
| 5 | survival_duration | 46.0% | 50.7% | 1.507 | 2.2473 | 0.2824 | 0.2033 |

Lower MAE, log loss, ECE, and Brier are better. Regression produces point predictions;
probability metrics are unavailable. Models are not selected or tuned using this holdout.
