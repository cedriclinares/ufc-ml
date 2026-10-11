# Finish versus distance walk-forward accuracy

Distance uses the exact full-final-round rule. Positive class is distance; probability cutoff is 50%.

| Rounds | Model | Accuracy | Balanced accuracy | Finish recall | Distance recall | Distance precision | Brier |
|---:|---|---:|---:|---:|---:|---:|---:|
| 3 | model | 60.8% | 60.8% | 61.4% | 60.1% | 60.6% | 0.2371 |
| 3 | baseline | 50.4% | 50.0% | 100.0% | 0.0% | 0.0% | 0.2502 |
| 5 | model | 62.7% | 62.0% | 70.4% | 53.6% | 60.7% | 0.2481 |
| 5 | baseline | 54.0% | 50.0% | 100.0% | 0.0% | 0.0% | 0.2555 |

See cutoff_accuracy.csv for finish-by-0.5/1.0/etc. accuracy from the saved duration models.
The majority_accuracy column in that cutoff table is descriptive evaluation-period prevalence, not a fitted baseline.
