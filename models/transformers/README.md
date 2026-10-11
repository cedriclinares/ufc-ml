# UFC transformers

Two small supervised architectures share winner, half-round duration, and distance
heads. `features` attends over pre-fight feature columns; `history` attends over
the last ten completed fights and also receives pre-fight summaries. Each uses
shared fighter encoders, two layers, four heads, width 64, and dropout 0.2.
Attention is bidirectional over information available before the prediction date.
There is no text tokenizer or next-token generation.

## Setup

From the project root:

```sh
.venv/bin/python -m pip install -r requirements-transformers.txt
```

PyTorch 2.14.1 was installed for the implementation. The scripts record exact
package versions in checkpoints and reports. Automatic device selection uses
MPS when available, otherwise CPU. `--device cpu` is reproducible on machines
without Metal access; CUDA can be selected explicitly. CPU thread count defaults
to four.

## Evaluation

```sh
LOKY_MAX_CPU_COUNT=4 .venv/bin/python scripts/train_transformers.py
```

Defaults run all four combinations (`features`/`history` × `stats`/`market`) on
the existing duration walk-forward test IDs: 16 three-round folds and four
five-round folds. Models train on both scheduled lengths before each fold's
test-start date. Holdout mode uses the same final 20% of fights in one fold per
scheduled length. Select `--architecture`, `--odds-mode`, or `--mode holdout`
to narrow a run.

The epoch limit defaults to 100, with ten-epoch early-stopping patience. Within
each outer training window, date-aligned 70/15/15 partitions select the epoch,
refit on the first 85%, and calibrate temperatures on the last 15%. Winner and
duration losses have equal weights; distance BCE has weight 0.5. Duration CE is
normalized by the log of the number of eligible classes. All three heads share
the encoder. Temperatures have no intercept, preserving fighter-swap symmetry.

For a quick check rather than a performance study:

```sh
LOKY_MAX_CPU_COUNT=4 .venv/bin/python scripts/train_transformers.py \
  --epochs 2 --max-folds 1 --output /private/tmp/ufc-transformer-smoke
```

`--max-folds` truncates coverage and is for debugging only. The implementation's
initial complete four-variant evaluation uses `--epochs 2 --baseline-iterations 40` across all folds;
interpret it as a short training comparison, not evidence of optimal model
performance. The default supports longer, early-stopped runs.

Tree baselines default to 200 iterations. The initial short run uses 40; both
limits are recorded in the report metadata. Priors are conditioned on scheduled
length. `--resume` reuses checkpoints only when model/training settings, data
fingerprints, training IDs, and test IDs match; it recomputes baselines and
reports. Do not resume checkpoints after changing model or feature-builder code.

Outputs are under `models/transformers/artifacts/<mode>/`:

- `report.md`: winner, duration, and distance comparison, including paired
  odds-available coverage.
- `results.json`: metrics, calibration bins, cutoff Brier scores, folds, and audit.
- `predictions.csv`: one out-of-sample row per model and fight.
- `coverage.json`: input fingerprints, exclusions, and reconstruction notes.
- `checkpoints/`: evaluation models, excluding their test outcomes. These are
  distinct from production checkpoints.

Training-prior and boosted-tree baselines are freshly fitted on the same optimizer
training partition and summaries as each corresponding transformer. Missing
odds exclude rows from market variants, not stats variants. Compare the
`odds_available` metric rows for equal evaluation coverage.
Market variants also have fewer training rows; equal test coverage alone does
not isolate the effect of adding odds from the effect of changing training coverage.

### Controlled five-round study

Use `--scheduled-rounds 5` to evaluate all four five-round folds while retaining
pooled three/five-round training. `--training-coverage odds-available` gives
statistics and market variants exactly the same training and calibration fights.
The follow-up study uses three seeds, early stopping with a 100-epoch ceiling,
and 200 GBT iterations:

```sh
for seed in 0 1 2; do
  LOKY_MAX_CPU_COUNT=4 .venv/bin/python scripts/train_transformers.py \
    --architecture history --odds-mode all --scheduled-rounds 5 \
    --training-coverage odds-available --epochs 100 --baseline-iterations 200 \
    --seed "$seed" --output "models/transformers/artifacts/five-round-study/seed-$seed"
done
LOKY_MAX_CPU_COUNT=4 .venv/bin/python scripts/calibrate_study_baselines.py
.venv/bin/python scripts/report_five_round_study.py
```

The calibration script refits GBTs using checkpoint optimizer IDs, verifies that
their probabilities match the original study, and fits scalar temperatures using
the same held-out calibration IDs as the transformers. It retains both raw and
calibrated GBT predictions. The report audits chronology and common IDs, reports
mean and standard deviation across seeds, compares normalized odds alone, and
computes paired event-date bootstrap intervals without counting seed repetitions
as independent test fights. Results are in
`logs/five-round-transformer-study/report.md` and `results.json`.

These are reused historical test folds, not a new confirmation set. Comparisons
are exploratory and do not establish betting profitability.

## Production fitting and upcoming predictions

```sh
LOKY_MAX_CPU_COUNT=4 .venv/bin/python scripts/train_transformers.py \
  --mode production --architecture history --odds-mode stats --before 2026-10-03

.venv/bin/python scripts/predict_transformers.py \
  examples/ufc-332-transformers.json --csv logs/transformer-predictions-2026-10-03.csv
```

Production requires an exclusive `--before` date. It selects training duration
on historical validation dates and retains a separate temperature-calibration
partition; it does not optimize weights on calibration fights. Prediction loads
the saved model without refitting, reconstructs histories strictly before the
card date, and rejects a checkpoint containing outcomes on/after that date.

The default checkpoint is `artifacts/production/history_stats.pt`. Supply
`--checkpoint` to choose another variant. Inference also accepts `--date`,
`--device`, `--fights`, `--fighters`, and `--stats`. The existing card-file formats
(event object, array, JSON lines, stdin) are supported. This script does not fetch
cards or odds automatically.

Each fight needs `red_name`, `blue_name`, `weight_class`, and
`number_of_rounds` (3 or 5). Optional fields are `championship_fight` (boolean),
`red_odds`, `blue_odds`, `total_line`, `over_odds`, and `under_odds`.

```json
{
  "event": "Example event",
  "date": "2026-10-10",
  "fights": [
    {
      "red_name": "Fighter A",
      "blue_name": "Fighter B",
      "weight_class": 155,
      "number_of_rounds": 3,
      "red_odds": 150,
      "blue_odds": -175,
      "total_line": 1.5,
      "over_odds": -120,
      "under_odds": 100
    }
  ]
}
```

The table prints both winner probabilities, the most probable half-round class,
distance probability, a duration distribution, and moneyline/total EV with units
at 20% and 40% thresholds. Over sums classes greater than the line; Under includes
equality. Only half-round lines between 0.5 and the scheduled length are supported.
Unpriced stats predictions remain available. Market checkpoints require both
moneylines and are explicitly selected; there is no silent model fallback.

Unit sizes retain the project's formula:

```text
EV = probability × decimal_odds − 1
units = max(0, 3 × EV / (decimal_odds − 1))
filtered_units = units if EV ≥ threshold else 0
```

These are project units, not bankroll percentages. Thresholds have not been
optimized for the transformers. CSV output includes both sides' EV and complete
probabilities, with checkpoint provenance saved in an adjacent metadata JSON.

## Data and safeguards

Pre-fight inputs are reconstructed from `fights.csv`, `fight_stats.csv`, and
`fighters.csv`, not persisted cumulative snapshots or the exported feature
table. The feature export supplies existing evaluation matchup IDs and fold
boundaries only. All same-date features, sequences, Elo ratings, and duration
summaries are emitted before incorporating that date's outcomes.

Fight targets use ceiling to a half-round, not nearest rounding. Distance means
finishing the scheduled final round at exactly five minutes, regardless of win
method. A stoppage at 4:59 and a completed final round can have the same duration
label but different distance labels. Distance and duration are independently
trained heads; they do not define a joint distribution.

Draws and no contests mask winner loss. Invalid endings mask duration and
distance losses. Unsupported scheduled lengths and unresolved fighter identities
are audited. Unknown or ambiguous card identities produce an unavailable row;
known debutants receive an empty sequence and zero recorded experience. Records
represent available local history, not guaranteed complete career histories.
Sequence and Elo updates require resolved identities for both historical sides;
reconstructed cumulative summaries retain available raw statistics for known fighters.

Raw-stat foreign-key ownership discrepancies are logged in the audit. The bout's
explicit red/blue stat references define the sides; stat-row fighter IDs never
reassign performance to a different fighter. Current height, reach, stance, and
gender records are static and not historically versioned. Historical moneyline
capture timestamps are unverified; total-market prices are used only for EV.

Checkpoints store tensors and primitive metadata and load with
`torch.load(..., weights_only=True)`. They include schemas, preprocessing,
temperature calibration, optimizer/calibration fight IDs, cutoffs, hashes, and
versions. Generated artifacts are ignored by Git. Existing models and profit
tracking are unchanged.

## Tests

```sh
LOKY_MAX_CPU_COUNT=4 .venv/bin/python -m unittest discover -s tests -p 'test_transformers.py'
```

Tests cover chronological leakage and folds, same-date Elo, sequence order and
truncation, missing measurements, unknown categories, debutants, exact ceiling
boundaries, task masking, training-only preprocessing, padding, both encoders'
swap symmetry, probability masks, finite gradients, checkpoint reloads, and a
trained-model card prediction with EV calculations.
