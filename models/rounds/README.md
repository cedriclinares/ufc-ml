# Fight duration models

Run from the repository root:

```sh
.venv/bin/python scripts/train_rounds_model.py
```

The runner compares four models separately for three- and five-round bouts:

- `classifier_base`: the original multiclass gradient boosting model and features.
- `classifier_duration`: the classifier with duration-history features.
- `regression_duration`: gradient boosting regression on continuous elapsed rounds,
  with predictions clipped to the scheduled length and rounded up to 0.5.
- `survival_duration`: binary gradient boosting models predicting survival beyond
  each half-round boundary conditional on reaching the preceding boundary.
  These are combined into a duration probability distribution.

Targets and final predictions are `0.0`, `0.5`, `1.0`, ..., up to the scheduled
length. Elapsed rounds are `(finish_round - 1) + elapsed_seconds / 300`, rounded
up using `ceil(elapsed_rounds * 2) / 2`. Exact half-round boundaries stay unchanged.
Going the distance means `finish_round == number_of_rounds` and five minutes
elapsed in the last round. Both `05:00:00` and `5:00` are recognized. Distance
maps to 3.0 or 5.0; a finish at 4:59 also rounds up to the full length but has
`gone_distance=False` in the predictions CSV.

The exported pre-fight features are joined to `fights.csv` on event date and
both fighter IDs. Ambiguous joins fail; unsupported scheduled lengths and
missing or invalid endings are excluded and counted. The existing exported
statistics are assumed pre-fight; they are not reconstructed by this runner.

Duration-history features use all valid raw bouts before each event date,
including fights absent from the feature export. Lifetime and recent-five
summaries include average continuous duration, early finishes, early KO/submission
wins and losses, finish wins, rate of being finished, distance rate, and separate
KO/submission win/loss rates. An early finish means within round one. Finish
win/loss rates here mean KO/TKO or submission, excluding decisions and no contests.

Opponent-adjusted duration is the average residual between a historical bout's
length and that opponent's mean duration available before that bout. Opponents
with no prior duration history contribute zero residual. Matchup means and
red-minus-blue differences are also included. Fighters with no prior history
have zero historical bout count and unknown rates; the training-fitted imputer
handles these values. All outcomes on the same event date are applied only after
features have been emitted. A current or future result cannot enter its own
features. Past results in the held-out period may enter later held-out histories;
the fitted estimators remain fixed.

Each model uses the same chronological 80/20 split for its scheduled length,
keeping event dates together. Preprocessing is fitted on training rows only.
Models are saved without refitting on test rows. This command retains the fixed
holdout comparison. The separate walk-forward command below retrains on expanding
windows. No hyperparameter tuning or automatic model selection uses evaluation rows.

Evaluation includes:

- Exact accuracy, accuracy within 0.5 rounds, and mean absolute error in rounds.
- Training-period majority and median baselines.
- Per-class metrics and confusion matrices.
- For probability models: log loss, multiclass Brier score, confidence ECE,
  confidence reliability bins, and Brier/reliability bins for survival beyond
  each half-round boundary. Lower loss, Brier, ECE, and MAE are better.

Each survival classifier is fitted only on training bouts still at risk in its
interval. The cumulative product of conditional survival probabilities forms a
nonincreasing survival curve; differences yield nonnegative class probabilities
summing to one. Calibration is
measured on the holdout, not fitted on it. Regression only produces point
predictions, so its probability metrics are recorded as null.

Default output is `models/rounds/artifacts/`:

- `rounds_3_<model>.joblib` and `rounds_5_<model>.joblib`: fitted pipelines.
- `rounds_3.joblib` and `rounds_5.joblib`: aliases for the duration classifier.
- `report.md`: comparison table for all models and baselines.
- `results.json`: metrics, calibration bins, class order, features, split dates,
  model parameters, package versions, and input fingerprints. Results are nested
  by scheduled length, then `models`.
- `predictions.csv`: held-out fights with a model column, duration target,
  prediction, error, distance flag, and class probabilities where available.

Options: `--data PATH`, `--fights PATH`, `--output DIRECTORY`,
`--train-fraction 0.8`, and `--iterations 200`. Re-running overwrites outputs.
No betting returns are calculated without round-market odds.

For inference, import `build_round_features` from `scripts.train_rounds_model`,
provide chronological pre-fight history including the future matchup, and pass
raw fight history as its second argument. Use the matching scheduled-length
pipeline. Duration class labels are strings like `"0.5"` in classifiers so
scikit-learn treats them as categories; convert `predict()` results to floats.
Regression predictions are already numeric. Probability columns follow the
saved pipeline's `classes_`. To get survival beyond boundary t, sum probabilities
for duration classes greater than t. For a half-round boundary, ceiling targets
preserve whether the original continuous duration was greater than that boundary.

Tests:

```sh
LOKY_MAX_CPU_COUNT=4 .venv/bin/python -m unittest discover -s tests -p 'test_duration_models.py'
.venv/bin/python -m unittest discover -s tests -p 'test_train_rounds_model.py'
```

## Walk-forward evaluation

```sh
LOKY_MAX_CPU_COUNT=4 .venv/bin/python scripts/evaluate_rounds_walk_forward.py
```

This evaluates the same final 20% of bouts as the holdout comparison. Models
retrain on an expanding window before each test chunk, with a fresh preprocessor
fitted only on that window. All train/test boundaries keep event dates intact.
Duration-history features incorporate only earlier event dates, including earlier
test results that would be available by the next prediction date. Majority and
median baselines are recomputed using each fold's training rows.

Defaults: `--initial-train-fraction 0.8`, `--step 100` for three-round bouts,
`--five-round-step 40` for five-round bouts, and `--iterations 200`. Steps are
approximate bout counts aligned to dates; a small step still advances one date.
`--data`, `--fights`, and `--output` can also be overridden.

Output goes to `models/rounds/walk_forward/`, preserving holdout artifacts:

- `report.md` and `results.json`: pooled evaluation metrics and fold boundaries.
- `predictions.csv`: one out-of-sample prediction per fight and model, including
  fold number, training cutoff, half-round output, error, and probabilities.
- `fold_metrics.csv`: per-fold accuracy, error, and probability metrics.
- `*_last_fold.joblib`: final evaluation-fold pipelines; these exclude the
  last test chunk and are not production models trained on the entire dataset.

Run the fold/preprocessing tests with:

```sh
LOKY_MAX_CPU_COUNT=4 .venv/bin/python -m unittest discover -s tests -p 'test_rounds_walk_forward.py'
```

## Finish versus distance

```sh
LOKY_MAX_CPU_COUNT=4 .venv/bin/python scripts/evaluate_finish_decision.py
```

This fits dedicated binary classifiers on the same walk-forward folds, predicting
whether the exact distance rule is met. It distinguishes a late stoppage from
a completed final round even when both share the same ceiling duration label.
It reports accuracy, balanced accuracy, finish and distance recall, distance
precision, Brier score, log loss, and confusion matrices against training-period
prior baselines. Predictions use a fixed 50% probability threshold.

Output goes to `models/rounds/finish_decision/`: `report.md`, `results.json`, and
`finish_distance_predictions.csv`. No production models are saved by this
evaluation. `cutoff_accuracy.csv` also measures finishing by each half-round
boundary using the saved walk-forward duration probabilities, summing duration
classes at or below each boundary. Equality at the boundary counts as finishing
within time. Run the round walk-forward evaluation first to produce those inputs.
The cutoff table's majority accuracy is descriptive evaluation prevalence, not
a trained baseline. Here “decision” means going the distance under the exact
time rule, not checking the win-method text for a decision.
