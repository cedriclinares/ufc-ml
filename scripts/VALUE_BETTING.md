# Upcoming UFC value predictions

Run from the repository root:

```sh
.venv/bin/python scripts/predict_value_bets.py examples/ufc-332-2026-10-03.json
```

The example contains the October 3 card and the odds used in the prediction run;
it is a reproducible example, not a live odds feed. For a new event, fetch its
Tapology page and specify its date:

```sh
.venv/bin/python scripts/predict_value_bets.py \
  --url 'https://www.tapology.com/fightcenter/events/EVENT-ID' \
  --date YYYY-MM-DD --headless --csv logs/upcoming-value-predictions.csv
```

Replace the URL and date with the actual event. Fetching needs network access,
Playwright, and a Chromium browser. Omit `--headless` to show the browser. A card
file works without network access. JSON arrays, JSON lines (including the output
of `scripts/fetch_upcoming.py`), and event objects are supported. Use `-` for stdin.

An event object has this structure:

```json
{
  "event": "Upcoming UFC event",
  "date": "2026-10-10",
  "fights": [
    {
      "red_name": "Fighter A",
      "blue_name": "Fighter B",
      "weight_class": "Lightweight",
      "red_odds": 150,
      "blue_odds": -175
    }
  ]
}
```

Names must match the local fighter records or an alias in `combine_tables.py`.
Weight can be pounds or a class name; use `Women's Flyweight`, for example, to
include gender metadata. Odds are American moneylines. `--date` overrides the
file's date and is required for JSON arrays, JSON lines, and fetched cards.

The table prints fight, value side, odds, model win probability, expected return,
qualification, calculated Kelly units, and units after the EV filter. Missing
odds, fighter identities, or histories remain visible with a reason. `--csv PATH`
saves the same results plus both sides' probabilities, EV, and snapshot dates.

Defaults match the prediction script on `walk-forward-value-betting`:

- Model: `gb_med_multielo_odds`, retrained on all local training rows before the
  event date, with statistics, three Elo ratings, streaks, experience, and odds.
- Expected return: `probability * decimal_odds - 1`, calculated for both sides;
  select the side with the highest return, even if it is less likely to win.
- EV threshold: `0.40` (40%). The reported training-CV-selected threshold for
  this model was `0.20`; the branch predictor uses `0.40`.
- Kelly multiplier: `3.0`. Calculated units are
  `max(0, multiplier * expected_return / (decimal_odds - 1))`.
  Filtered units are zero unless the selected side meets the threshold.

To use the 20% threshold and another sizing multiplier:

```sh
.venv/bin/python scripts/predict_value_bets.py examples/ufc-332-2026-10-03.json \
  --ev-threshold 0.20 --kelly-multiplier 3 --csv logs/value-predictions.csv
```

The sizing convention matches the project's unit outputs; these numbers are
not bankroll percentages. Dollars equal units times your dollar value per unit.
The script does not calculate a bankroll allocation or compound returns.

Current inputs use the latest repaired **post-fight** snapshot before the event,
event-date age (in days, matching training), and current fighter metadata.
Each future fight is appended independently so placeholder results cannot
affect another prediction. Elo and rest history follow the model's training
dataset. Historical training features are taken from the existing export.

Update the local datasets before predicting a later event. The script prints
the training history cutoff and does not refresh the fighter database. Paths
can be overridden with `--data`, `--fighters`, `--snapshots`, and `--history`.

Verification:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_predict_value_bets.py'
```
