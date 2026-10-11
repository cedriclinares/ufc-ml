# Transformers for winner and fight-duration predictions

## Summary

Build and compare four compact models:

| Architecture | Statistics only | Statistics + moneyline odds |
|---|---|---|
| Feature-token transformer | Existing pre-fight summaries represented as tokens | Same summaries plus market probabilities |
| Fight-history transformer | Previous fights represented as tokens | Same histories plus market probabilities |

Each model shares its learned representation across three outputs: winner probability, half-round duration probabilities, and probability of going the distance. Deliver training, walk-forward evaluation, saved checkpoints, and upcoming-event prediction tables.

Implement everything in the UFC project. Use `scratch-llm` as an architectural reference; replace its character tokenizer, causal attention, and language-generation objective.

## Data and targets

- Build a common chronological dataset from the local fights, raw statistics, and fighter metadata. Reuse the existing history reconstruction and time parsers. Reconstruct pre-fight summaries rather than trusting exported cumulative values.
- Freeze history at the beginning of each event date. Current-fight results and statistics never enter its inputs. Apply that date’s updates afterward, including Elo, streaks, and experience.
- Join statistics using their referenced IDs; resolve fighter identities with existing aliases. Reject ambiguous joins and report exclusions. Names and IDs identify records but are not learned features.
- Use three- and five-round bouts for training. Historical bouts of other lengths may contribute to fighter history.
- Winner target: Red or Blue. Mask this task for draws and no contests. Mask duration and distance tasks when endings are invalid; valid tasks can still contribute to training.
- Duration target: `ceil(2 × (finish_round − 1 + finish_seconds / 300)) / 2`, preserving `0.0, 0.5, …, 5.0`.
- Distance target: `finish_round == number_of_rounds` and final-round elapsed time equals 300 seconds. A late stoppage remains distinct even when its duration rounds up to the scheduled length.
- Fit imputers, scalers, and categorical vocabularies only on each training partition. Include missing-value indicators and an unknown category. Retain fighters with no previous fights using an explicit empty-history representation.

## Architecture and training

**Feature tokens**

Use the current pre-fight feature allowlist, derived ratios, duration-history summaries, and causal Elo/experience features. Encode each fighter separately with shared weights: numerical values receive learned feature-specific projections, categorical values receive embeddings, and a summary token produces the fighter representation. This follows the [feature-token transformer approach](https://arxiv.org/abs/2106.11959).

**Fight-history tokens**

Use the last ten completed fights per fighter, ordered oldest to newest. Each token contains own and opponent striking, takedown, knockdown, submission, control and reversal statistics; elapsed duration; result and finish method; scheduled length; opponent pre-fight Elo; and time elapsed before the upcoming bout. Include counts, per-minute rates, and missing indicators. Add fight-order embeddings, pad shorter histories, and mask padding.

Both architectures use bidirectional attention over supplied historical inputs. Use two pre-normalized transformer layers, four heads, embedding width 64, feed-forward width 256, and dropout 0.2. Implement with PyTorch encoder components.

**Prediction heads**

Combine the two fighter representations with their difference, elementwise product, and matchup context: weight, gender, scheduled rounds, and championship status. Add normalized no-vig moneyline probabilities only in odds-assisted variants.

Use a 128-unit hidden layer followed by:

- A binary winner logit.
- Eleven duration logits, masking classes above the scheduled length before softmax.
- A binary distance logit.

Evaluate both fighter orientations. Antisymmetrize the winner logit and average duration/distance logits so swapping fighters reverses winner probabilities and preserves duration predictions.

Train with masked winner BCE, duration cross-entropy, and distance BCE. Weight winner and duration losses equally, with distance weighted 0.5; normalize duration loss by the logarithm of its eligible class count.

Defaults: AdamW, learning rate `3e-4`, weight decay `0.01`, batch size 64, maximum 100 epochs, gradient clipping at 1.0, seed 0, and early-stopping patience ten epochs. Use MPS when available, otherwise CPU; allow an explicit device override.

Within each training window, reserve chronological partitions of 70% for fitting, 15% for checkpoint selection, and 15% for calibration, keeping dates intact. Select the epoch using validation loss, refit on the first 85% for that epoch count, and fit separate scalar temperatures on the calibration partition. Outer test outcomes never influence these steps.

## Evaluation and prediction interfaces

Create the reusable implementation under `models/transformers/`, with two entrypoints:

- `scripts/train_transformers.py`: `--architecture features|history|all`, `--odds-mode stats|market|all`, `--mode holdout|walk-forward|production`, output directory, device, seed, and epoch overrides.
- `scripts/predict_transformers.py`: saved checkpoint, existing event-card JSON format, and optional CSV output.

Walk-forward evaluation must reuse the existing duration folds and evaluation fight IDs. For each scheduled-length fold, train the shared model on eligible three- and five-round bouts strictly before its test-start date. Report winner and duration metrics on those same test fights. Recompute tree and simple baselines on identical training data and test IDs; keep prior reports intact.

Report winner accuracy, log loss, Brier score, and calibration; duration exact accuracy, within-0.5 accuracy, MAE, log loss, and half-round cutoff Brier scores; and distance accuracy, balanced accuracy, and Brier score. Separate three- and five-round results. Compare odds variants on their common odds-available subset and report coverage.

Save checkpoints with preprocessing, configuration, class order, training cutoff, calibration parameters, input fingerprints, and package versions. Production fitting uses only data before the requested prediction date and retains a calibration partition.

Extend card inputs with required `number_of_rounds` and optional `total_line`, `over_odds`, and `under_odds`. Print winner probabilities, duration distribution, most likely half-round duration, distance probability, and over/under probabilities. Sum duration probabilities above the supplied line for Over and at/below it for Under; support half-round lines only in this version.

When prices exist, print EV and filtered units at both 20% and 40%, preserving the project’s existing 3× Kelly unit convention. Missing prices suppress EV calculations, not statistics-only predictions. Default to the statistics-only history checkpoint; require explicit selection of odds-assisted checkpoints and complete moneyline inputs.

## Tests and acceptance criteria

- Prove that changing current or future results cannot change earlier inputs; same-date fights share frozen history.
- Verify history order, truncation, corner changes, padding, debut fighters, missing measurements, and unknown categories.
- Test ceiling labels at exact boundaries and distinguish late stoppages from going the distance.
- Verify probability sums, scheduled-length masking, fighter-swap symmetry, and over/under aggregation.
- Verify preprocessing and calibration cutoffs, one prediction per evaluation fight per variant, and checkpoint reload consistency.
- Run a small synthetic training test with finite losses and parameter updates, then complete the four-variant walk-forward comparison and a saved-checkpoint card prediction.
- Preserve existing betting predictors and reports. The new models remain explicitly selectable regardless of whether they outperform the baselines.

Defaults assume local Mac training with one seed and fixed settings. Add PyTorch as a recorded dependency. Historical odds lack verified capture timestamps, so label odds-assisted results accordingly. Automated betting, settlement tracking changes, broad hyperparameter searches, and transformer pretraining are outside this implementation.
