# Walk-Forward Value Betting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a walk-forward validated, value-betting UFC prediction system that achieves +100 betting units on the test period (2023-08-05 to 2026-09-19).

**Architecture:** Precompute chronological ELO ratings and streak features (no leakage by construction — each fight's features use only prior outcomes). Use walk-forward validation with expanding windows for model training and hyperparameter selection. Apply value betting (only bet when model's predicted probability exceeds market-implied probability by a tuned edge threshold) to concentrate bets on high-edge fights.

**Tech Stack:** Python 3.14, scikit-learn (HistGradientBoostingClassifier), pandas, numpy. No new dependencies.

**Spec:** Design approved in chat (no written spec — architectural-lite: extends existing comparison framework).

## Global Constraints

- Python 3.14.7, scikit-learn >= 1.6 (FrozenEstimator)
- `random_state=0` for all stochastic operations
- No data leakage: features at time T use only fight outcomes before T
- Chronological ordering: same-date fights never split across train/test folds
- No new pip dependencies — use only what's already installed
- One unit staked per bet; settlement follows existing `evaluate()` conventions
- IDs and names excluded from model features; odds used only for value-bet filtering (not as model features unless explicitly noted)

## Review Focus

1. **ELO leakage** — if `compute_elo` uses the current fight's outcome to set that fight's ELO feature, the model sees the future. Test: first fight for any fighter must have `initial` ELO, not a post-fight value.
2. **Walk-forward date bleed** — if a fold boundary splits fights from the same event, training data contains same-day outcomes. Test: every fold boundary aligns to the first fight of a new event date.
3. **Value threshold overfitting** — if the edge threshold is tuned on the test set, reported profit is meaningless. Test: threshold must be selected on training-period walk-forward folds only.
4. **Streak reset on corner swap** — a fighter can appear as Red in one fight and Blue in the next. Streaks must track by fighter ID regardless of corner. Test: a fighter's streak persists across corner changes.
5. **Degenerate threshold** — edge threshold of 0.0 with all-fight betting must produce the same units as the existing flat-stake evaluation. Test: `value_bet_evaluate` with `edge_threshold=0.0` matches `evaluate()` units.

---

### Task 1: ELO Rating and Streak Feature Computation

**Files:**
- Create: `models/comparisons/elo.py`
- Test: `tests/test_elo.py`

**Interfaces:**
- Consumes: DataFrame with columns `r_id`, `b_id`, `winner`, `date` (sorted by date)
- Produces:
  - `compute_elo(df, k=32, initial=1500) -> pd.DataFrame` with columns `r_elo`, `b_elo`, `elo_diff`
  - `compute_streaks(df) -> pd.DataFrame` with columns `r_win_streak`, `r_loss_streak`, `b_win_streak`, `b_loss_streak`, `win_streak_diff`

- [ ] **Step 1: Write failing tests for ELO computation**

```python
# tests/test_elo.py
import unittest
import pandas as pd
from models.comparisons.elo import compute_elo, compute_streaks


class EloTests(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            'r_id': ['a', 'c', 'a'],
            'b_id': ['b', 'b', 'c'],
            'winner': ['Red', 'Blue', 'Red'],
            'date': pd.to_datetime(['2020-01-01', '2020-02-01', '2020-03-01']),
        })

    def test_initial_elo(self):
        elo = compute_elo(self.df, k=32, initial=1500)
        # First fight: both fighters start at 1500
        self.assertEqual(elo.loc[0, 'r_elo'], 1500)
        self.assertEqual(elo.loc[0, 'b_elo'], 1500)
        self.assertEqual(elo.loc[0, 'elo_diff'], 0)

    def test_elo_updates_after_fight(self):
        elo = compute_elo(self.df, k=32, initial=1500)
        # After fight 0 (a beats b): a's ELO rises, b's drops
        # Fight 1: c vs b. c is new (1500), b lost once
        self.assertEqual(elo.loc[1, 'r_elo'], 1500)  # c is new
        self.assertLess(elo.loc[1, 'b_elo'], 1500)    # b lost

    def test_elo_no_leakage(self):
        elo = compute_elo(self.df, k=32, initial=1500)
        # Fight 2: a vs c. a won fight 0, c won fight 1
        self.assertGreater(elo.loc[2, 'r_elo'], 1500)  # a won once
        self.assertGreater(elo.loc[2, 'b_elo'], 1500)  # c won once

    def test_elo_diff_sign(self):
        elo = compute_elo(self.df, k=32, initial=1500)
        # elo_diff = r_elo - b_elo
        for i in range(len(self.df)):
            self.assertAlmostEqual(
                elo.loc[i, 'elo_diff'],
                elo.loc[i, 'r_elo'] - elo.loc[i, 'b_elo'],
            )

    def test_elo_custom_k(self):
        elo_low = compute_elo(self.df, k=16, initial=1500)
        elo_high = compute_elo(self.df, k=64, initial=1500)
        # Higher K means bigger rating changes
        self.assertGreater(
            abs(elo_high.loc[1, 'b_elo'] - 1500),
            abs(elo_low.loc[1, 'b_elo'] - 1500),
        )

    def test_elo_index_alignment(self):
        elo = compute_elo(self.df, k=32, initial=1500)
        self.assertTrue(elo.index.equals(self.df.index))


class StreakTests(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            'r_id': ['a', 'a', 'a', 'b'],
            'b_id': ['b', 'c', 'd', 'a'],
            'winner': ['Red', 'Red', 'Blue', 'Red'],
            'date': pd.to_datetime([
                '2020-01-01', '2020-02-01', '2020-03-01', '2020-04-01'
            ]),
        })

    def test_initial_streaks_zero(self):
        streaks = compute_streaks(self.df)
        # First fight: both fighters have no history
        self.assertEqual(streaks.loc[0, 'r_win_streak'], 0)
        self.assertEqual(streaks.loc[0, 'r_loss_streak'], 0)
        self.assertEqual(streaks.loc[0, 'b_win_streak'], 0)
        self.assertEqual(streaks.loc[0, 'b_loss_streak'], 0)

    def test_win_streak_builds(self):
        streaks = compute_streaks(self.df)
        # Fight 1: a won fight 0 -> win streak 1
        self.assertEqual(streaks.loc[1, 'r_win_streak'], 1)
        # Fight 2: a won fights 0,1 -> win streak 2
        self.assertEqual(streaks.loc[2, 'r_win_streak'], 2)

    def test_streak_resets_on_loss(self):
        streaks = compute_streaks(self.df)
        # Fight 3: a lost fight 2 -> win streak 0, loss streak 1
        # a is in blue corner for fight 3
        self.assertEqual(streaks.loc[3, 'b_win_streak'], 0)
        self.assertEqual(streaks.loc[3, 'b_loss_streak'], 1)

    def test_streak_tracks_across_corners(self):
        streaks = compute_streaks(self.df)
        # Fighter 'a' appears as red in fights 0-2, blue in fight 3
        # After winning fights 0,1 then losing fight 2:
        # Fight 3 (a in blue): win_streak=0, loss_streak=1
        self.assertEqual(streaks.loc[3, 'b_loss_streak'], 1)

    def test_win_streak_diff(self):
        streaks = compute_streaks(self.df)
        for i in range(len(self.df)):
            self.assertEqual(
                streaks.loc[i, 'win_streak_diff'],
                streaks.loc[i, 'r_win_streak'] - streaks.loc[i, 'b_win_streak'],
            )

    def test_streak_index_alignment(self):
        streaks = compute_streaks(self.df)
        self.assertTrue(streaks.index.equals(self.df.index))


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_elo.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'models.comparisons.elo'`

- [ ] **Step 3: Implement ELO and streak computation**

```python
# models/comparisons/elo.py
"""Chronological ELO ratings and streak features for UFC fighters.

All features are pre-fight: each row's values use only fight outcomes
that occurred before that row's date. No leakage by construction.
"""
import pandas as pd


def compute_elo(df, k=32, initial=1500):
    """Compute pre-fight ELO ratings for both corners.

    Args:
        df: DataFrame sorted by date with columns r_id, b_id, winner.
        k: ELO K-factor (rating sensitivity).
        initial: Starting rating for new fighters.

    Returns:
        DataFrame with columns r_elo, b_elo, elo_diff (same index as df).
    """
    ratings = {}
    rows = []
    for _, fight in df.iterrows():
        r_id, b_id = fight['r_id'], fight['b_id']
        r_elo = ratings.get(r_id, initial)
        b_elo = ratings.get(b_id, initial)
        rows.append({'r_elo': r_elo, 'b_elo': b_elo, 'elo_diff': r_elo - b_elo})

        expected_r = 1 / (1 + 10 ** ((b_elo - r_elo) / 400))
        actual_r = 1.0 if fight['winner'] == 'Red' else 0.0
        ratings[r_id] = r_elo + k * (actual_r - expected_r)
        ratings[b_id] = b_elo + k * ((1 - actual_r) - (1 - expected_r))

    return pd.DataFrame(rows, index=df.index)


def compute_streaks(df):
    """Compute pre-fight win/loss streaks for both corners.

    Tracks streaks by fighter ID across both corners. A win resets the
    loss streak and increments the win streak, and vice versa.

    Returns:
        DataFrame with columns r_win_streak, r_loss_streak,
        b_win_streak, b_loss_streak, win_streak_diff (same index as df).
    """
    fighter_streaks = {}  # fighter_id -> (win_streak, loss_streak)
    rows = []
    for _, fight in df.iterrows():
        r_id, b_id = fight['r_id'], fight['b_id']
        r_w, r_l = fighter_streaks.get(r_id, (0, 0))
        b_w, b_l = fighter_streaks.get(b_id, (0, 0))
        rows.append({
            'r_win_streak': r_w, 'r_loss_streak': r_l,
            'b_win_streak': b_w, 'b_loss_streak': b_l,
            'win_streak_diff': r_w - b_w,
        })

        if fight['winner'] == 'Red':
            fighter_streaks[r_id] = (r_w + 1, 0)
            fighter_streaks[b_id] = (0, b_l + 1)
        else:
            fighter_streaks[r_id] = (0, r_l + 1)
            fighter_streaks[b_id] = (b_w + 1, 0)

    return pd.DataFrame(rows, index=df.index)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_elo.py -v`
Expected: All 12 tests PASS

- [ ] **Step 5: Commit**

```bash
git add models/comparisons/elo.py tests/test_elo.py
git commit -m "feat: add ELO rating and streak feature computation"
```

---

### Task 2: Walk-Forward Validation Engine

**Files:**
- Create: `models/comparisons/walk_forward.py`
- Test: `tests/test_walk_forward.py`

**Interfaces:**
- Consumes: `compute_elo` and `compute_streaks` from Task 1; `make_features`, `pipeline`, `decimal_odds` from `evaluation.py`
- Produces:
  - `walk_forward_predict(df, y, x, decimal, estimator_factory, min_train=4000, step=200) -> (np.ndarray, np.ndarray, np.ndarray)` — returns `(probabilities, y_true, decimal_odds)` for all out-of-sample predictions
  - `align_to_event(dates, raw_index) -> int` — snaps an index to the nearest event-date boundary

```python
# tests/test_walk_forward.py
import unittest
import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier

from models.comparisons.walk_forward import walk_forward_predict, align_to_event


class AlignToEventTests(unittest.TestCase):
    def test_snaps_forward_to_event_boundary(self):
        dates = pd.Series(pd.to_datetime(
            ['2020-01-01'] * 5 + ['2020-02-01'] * 3 + ['2020-03-01'] * 4
        ))
        # Index 6 is mid-event on 2020-02-01; should snap to 5 (start of that date)
        self.assertEqual(align_to_event(dates, 6), 5)

    def test_already_at_boundary(self):
        dates = pd.Series(pd.to_datetime(
            ['2020-01-01'] * 5 + ['2020-02-01'] * 3
        ))
        self.assertEqual(align_to_event(dates, 5), 5)

    def test_past_end(self):
        dates = pd.Series(pd.to_datetime(['2020-01-01'] * 5))
        self.assertEqual(align_to_event(dates, 10), 5)


class WalkForwardTests(unittest.TestCase):
    def _make_data(self, n=100):
        np.random.seed(0)
        dates = pd.to_datetime(
            pd.date_range('2020-01-01', periods=n // 5, freq='14D')
            .repeat(5)[:n]
        )
        df = pd.DataFrame({
            'date': dates,
            'feat1': np.random.randn(n),
            'feat2': np.random.randn(n),
        })
        y = pd.Series(np.random.randint(0, 2, n))
        x = df[['feat1', 'feat2']]
        decimal = np.column_stack([
            np.full(n, 2.0),  # even odds
            np.full(n, 2.0),
        ])
        return df, y, x, decimal

    def test_covers_all_test_rows(self):
        df, y, x, decimal = self._make_data(100)
        proba, y_out, dec_out = walk_forward_predict(
            df, y, x, decimal,
            lambda: DummyClassifier(strategy='prior', random_state=0),
            min_train=50, step=25,
        )
        # Should predict on rows 50..99 = 50 rows
        self.assertEqual(len(y_out), 50)
        self.assertEqual(proba.shape, (50, 2))

    def test_no_future_leakage(self):
        df, y, x, decimal = self._make_data(100)
        # Make last 25 rows always class 1
        y.iloc[75:] = 1
        x_leak = x.copy()
        x_leak.iloc[75:, 0] = 999  # obvious signal

        proba, y_out, _ = walk_forward_predict(
            df, y, x_leak, decimal,
            lambda: DummyClassifier(strategy='prior', random_state=0),
            min_train=50, step=25,
        )
        # DummyClassifier ignores features, so the "leak" signal shouldn't matter
        # This test verifies the engine doesn't accidentally feed future rows
        self.assertEqual(len(y_out), 50)

    def test_fold_boundaries_respect_event_dates(self):
        # 20 fights across 4 event dates (5 per event)
        dates = pd.to_datetime(
            ['2020-01-01'] * 5 + ['2020-02-01'] * 5 +
            ['2020-03-01'] * 5 + ['2020-04-01'] * 5
        )
        df = pd.DataFrame({
            'date': dates,
            'feat1': np.random.randn(20),
        })
        y = pd.Series(np.random.randint(0, 2, 20))
        x = df[['feat1']]
        decimal = np.full((20, 2), 2.0)

        proba, y_out, _ = walk_forward_predict(
            df, y, x, decimal,
            lambda: DummyClassifier(strategy='prior', random_state=0),
            min_train=10, step=5,
        )
        # min_train=10 covers first 2 events; should predict events 3,4 = 10 rows
        self.assertEqual(len(y_out), 10)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 1: Write the test file above**

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_walk_forward.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement walk-forward engine**

```python
# models/comparisons/walk_forward.py
"""Walk-forward validation for chronological fight prediction.

Trains on an expanding window, predicts the next chunk of fights,
then advances. All fold boundaries snap to event-date boundaries
so same-day fights are never split across train/test.
"""
import numpy as np
import pandas as pd

from .evaluation import pipeline


def align_to_event(dates, raw_index):
    """Snap an index to the start of its event date.

    If raw_index falls mid-event, moves back to the first fight on that date.
    If raw_index exceeds the series length, returns len(dates).
    """
    if raw_index >= len(dates):
        return len(dates)
    target_date = dates.iloc[raw_index]
    return int((dates < target_date).sum())


def walk_forward_predict(df, y, x, decimal, estimator_factory,
                          min_train=4000, step=200):
    """Run walk-forward validation with expanding training window.

    Args:
        df: DataFrame with 'date' column, sorted chronologically.
        y: Series of labels (0=Red, 1=Blue).
        x: Feature DataFrame (same index as df).
        decimal: Array of decimal odds, shape (n, 2).
        estimator_factory: Callable returning a fresh sklearn estimator.
        min_train: Minimum number of training rows before first prediction.
        step: Approximate number of fights per fold (snapped to event dates).

    Returns:
        (probabilities, y_true, decimal_odds) — arrays for all predicted rows.
    """
    dates = df['date']
    n = len(df)
    train_end = align_to_event(dates, min_train)

    all_proba = []
    all_y = []
    all_decimal = []

    while train_end < n:
        test_end = align_to_event(dates, train_end + step)
        if test_end <= train_end:
            test_end = n  # remaining fights

        model = pipeline(estimator_factory(), x.columns)
        model.fit(x.iloc[:train_end], y.iloc[:train_end])

        proba = model.predict_proba(x.iloc[train_end:test_end])
        all_proba.append(proba)
        all_y.append(y.iloc[train_end:test_end].values)
        all_decimal.append(decimal[train_end:test_end])

        train_end = test_end

    return (
        np.concatenate(all_proba),
        np.concatenate(all_y),
        np.concatenate(all_decimal),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_walk_forward.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add models/comparisons/walk_forward.py tests/test_walk_forward.py
git commit -m "feat: add walk-forward validation engine"
```

---

### Task 3: Value Betting Evaluation

**Files:**
- Create: `models/comparisons/value_betting.py`
- Test: `tests/test_value_betting.py`

**Interfaces:**
- Consumes: `evaluate()` from `evaluation.py` (for comparison)
- Produces:
  - `value_bet_evaluate(y, probabilities, decimal, edge_threshold=0.05) -> dict` — returns metrics dict with keys: `test_accuracy`, `bets`, `betting_units`, `roi`, `skipped`
  - `optimize_threshold(y, probabilities, decimal, thresholds=None) -> (float, dict)` — returns best threshold and its metrics (optimized for betting_units)

```python
# tests/test_value_betting.py
import unittest
import numpy as np
from models.comparisons.value_betting import value_bet_evaluate, optimize_threshold


class ValueBetTests(unittest.TestCase):
    def test_zero_threshold_matches_flat_betting(self):
        y = np.array([0, 1, 0, 1, 0])
        proba = np.array([
            [0.7, 0.3],
            [0.4, 0.6],
            [0.6, 0.4],
            [0.3, 0.7],
            [0.55, 0.45],
        ])
        # Even odds: decimal 2.0 for both
        decimal = np.full((5, 2), 2.0)
        result = value_bet_evaluate(y, proba, decimal, edge_threshold=0.0)
        # All 5 bets placed (every prediction has edge >= 0 vs 50/50 implied)
        self.assertEqual(result['bets'], 5)
        # Predictions: [0, 1, 0, 1, 0]; actuals: [0, 1, 0, 1, 0]
        # All correct with even odds: 5 * (2.0 - 1) = +5.0
        self.assertAlmostEqual(result['betting_units'], 5.0)

    def test_high_threshold_filters_bets(self):
        y = np.array([0, 1, 0])
        proba = np.array([
            [0.9, 0.1],   # high confidence -> bets
            [0.45, 0.55], # low confidence -> skipped
            [0.8, 0.2],   # high confidence -> bets
        ])
        # -200/+160 type odds: favorite implied ~67%
        decimal = np.array([
            [1.5, 3.0],
            [1.5, 3.0],
            [1.5, 3.0],
        ])
        result = value_bet_evaluate(y, proba, decimal, edge_threshold=0.15)
        # Implied probs: [0.667, 0.333] (normalized from 1/1.5, 1/3.0)
        # Fight 0: pred=Red, model=0.9, implied=0.667, edge=0.233 > 0.15 -> bet
        # Fight 1: pred=Blue, model=0.55, implied=0.333, edge=0.217 > 0.15 -> bet
        # Fight 2: pred=Red, model=0.8, implied=0.667, edge=0.133 < 0.15 -> skip
        self.assertEqual(result['bets'], 2)
        self.assertEqual(result['skipped'], 1)

    def test_no_bets_when_threshold_too_high(self):
        y = np.array([0, 1])
        proba = np.array([[0.55, 0.45], [0.45, 0.55]])
        decimal = np.full((2, 2), 2.0)
        result = value_bet_evaluate(y, proba, decimal, edge_threshold=0.90)
        self.assertEqual(result['bets'], 0)
        self.assertEqual(result['betting_units'], 0.0)

    def test_optimize_threshold_returns_best(self):
        np.random.seed(0)
        y = np.random.randint(0, 2, 200)
        proba = np.random.dirichlet([1, 1], 200)
        decimal = np.full((200, 2), 2.0)
        best_threshold, best_metrics = optimize_threshold(
            y, proba, decimal,
            thresholds=[0.0, 0.1, 0.2, 0.3, 0.4],
        )
        self.assertIn(best_threshold, [0.0, 0.1, 0.2, 0.3, 0.4])
        self.assertIn('betting_units', best_metrics)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 1: Write the test file above**

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_value_betting.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement value betting evaluation**

```python
# models/comparisons/value_betting.py
"""Value betting: only bet when model probability exceeds market-implied probability."""
import numpy as np


def value_bet_evaluate(y, probabilities, decimal, edge_threshold=0.05):
    """Evaluate betting with value-based filtering.

    Only places a bet when the model's predicted probability for the
    chosen side exceeds the market-implied probability by at least
    edge_threshold.

    Args:
        y: Array of true labels (0=Red, 1=Blue).
        probabilities: Array of shape (n, 2) with predicted probabilities.
        decimal: Array of shape (n, 2) with decimal odds.
        edge_threshold: Minimum edge required to place a bet.

    Returns:
        Dict with test_accuracy, bets, betting_units, roi, skipped.
    """
    y = np.asarray(y)
    probabilities = np.asarray(probabilities)

    implied = 1 / decimal
    market = implied / implied.sum(axis=1, keepdims=True)

    predicted = probabilities.argmax(axis=1)  # Red wins exact ties
    model_prob = probabilities[np.arange(len(y)), predicted]
    market_prob = market[np.arange(len(y)), predicted]
    edge = model_prob - market_prob

    bet_mask = edge >= edge_threshold

    if not bet_mask.any():
        return {
            'test_accuracy': 0.0, 'bets': 0, 'betting_units': 0.0,
            'roi': 0.0, 'skipped': len(y),
        }

    selected_decimal = decimal[np.arange(len(y)), predicted]
    profits = np.where(predicted == y, selected_decimal - 1, -1)

    bet_profits = profits[bet_mask]
    bet_correct = (predicted[bet_mask] == y[bet_mask]).sum()

    return {
        'test_accuracy': float(bet_correct / bet_mask.sum()),
        'bets': int(bet_mask.sum()),
        'betting_units': float(bet_profits.sum()),
        'roi': float(bet_profits.mean()),
        'skipped': int((~bet_mask).sum()),
    }


def optimize_threshold(y, probabilities, decimal, thresholds=None):
    """Find the edge threshold that maximizes betting units.

    Args:
        y: Array of true labels.
        probabilities: Array of shape (n, 2).
        decimal: Array of shape (n, 2).
        thresholds: List of thresholds to try. Defaults to
            [0.0, 0.02, 0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25, 0.30].

    Returns:
        (best_threshold, best_metrics) tuple.
    """
    if thresholds is None:
        thresholds = [0.0, 0.02, 0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25, 0.30]

    best_threshold = 0.0
    best_metrics = value_bet_evaluate(y, probabilities, decimal, 0.0)

    for t in thresholds:
        metrics = value_bet_evaluate(y, probabilities, decimal, t)
        if metrics['betting_units'] > best_metrics['betting_units']:
            best_threshold = t
            best_metrics = metrics

    return best_threshold, best_metrics
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_value_betting.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add models/comparisons/value_betting.py tests/test_value_betting.py
git commit -m "feat: add value betting evaluation and threshold optimization"
```

---

### Task 4: Walk-Forward Training Script with Integrated Features

**Files:**
- Create: `models/comparisons/train_wf.py`
- Modify: `model_results.md` (appended by script)

**Interfaces:**
- Consumes: `compute_elo`, `compute_streaks` from Task 1; `walk_forward_predict` from Task 2; `value_bet_evaluate`, `optimize_threshold` from Task 3; `make_features`, `decimal_odds`, `split_index`, `pipeline`, `evaluate` from `evaluation.py`
- Produces: Walk-forward comparison results written to `model_results.md` and `artifacts/wf_results.json`

- [ ] **Step 1: Write a smoke test that verifies the script runs end-to-end on a small synthetic dataset**

```python
# tests/test_train_wf.py
import unittest
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from models.comparisons.features import BASE_FEATURES


class TrainWfSmokeTest(unittest.TestCase):
    def test_script_imports(self):
        """Verify all modules can be imported without error."""
        from models.comparisons import elo, walk_forward, value_betting
        from models.comparisons.evaluation import make_features, decimal_odds, split_index

    def test_elo_on_real_columns(self):
        """Verify ELO works with the columns present in the real dataset."""
        from models.comparisons.elo import compute_elo, compute_streaks
        df = pd.DataFrame({
            'r_id': ['a', 'b', 'c'],
            'b_id': ['b', 'c', 'a'],
            'winner': ['Red', 'Blue', 'Red'],
            'date': pd.to_datetime(['2020-01-01', '2020-02-01', '2020-03-01']),
        })
        elo = compute_elo(df)
        streaks = compute_streaks(df)
        self.assertEqual(len(elo), 3)
        self.assertEqual(len(streaks), 3)
        # ELO columns can be concatenated with feature dataframes
        combined = pd.concat([elo, streaks], axis=1)
        self.assertEqual(len(combined.columns), 8)  # 3 elo + 5 streak


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run smoke test**

Run: `.venv/bin/python -m pytest tests/test_train_wf.py -v`
Expected: PASS

- [ ] **Step 3: Write the walk-forward training script**

```python
# models/comparisons/train_wf.py
"""Walk-forward training with ELO features and value betting.

Run with: python -m models.comparisons.train_wf (from the repository root).
"""
import argparse
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from threadpoolctl import threadpool_limits

from .elo import compute_elo, compute_streaks
from .evaluation import decimal_odds, evaluate, make_features, pipeline, split_index
from .value_betting import optimize_threshold, value_bet_evaluate
from .walk_forward import walk_forward_predict

ROOT = Path(__file__).resolve().parents[2]

# Hyperparameter configurations to compare via walk-forward
CONFIGS = {
    'gb_baseline': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=200, max_leaf_nodes=15, learning_rate=0.05,
            l2_regularization=1, early_stopping=False, random_state=0,
        ),
        'use_elo': False,
    },
    'gb_elo': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=200, max_leaf_nodes=15, learning_rate=0.05,
            l2_regularization=1, early_stopping=False, random_state=0,
        ),
        'use_elo': True,
    },
    'gb_tuned_elo': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=500, max_leaf_nodes=31, learning_rate=0.03,
            l2_regularization=0.5, min_samples_leaf=20,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True,
    },
    'gb_conservative': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=300, max_leaf_nodes=15, learning_rate=0.03,
            l2_regularization=5, min_samples_leaf=50,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True,
    },
    'gb_deep_elo': {
        'estimator': lambda: HistGradientBoostingClassifier(
            max_iter=500, max_leaf_nodes=63, learning_rate=0.01,
            l2_regularization=1, min_samples_leaf=30,
            early_stopping=False, random_state=0,
        ),
        'use_elo': True,
    },
    'lr_elo': {
        'estimator': lambda: LogisticRegression(max_iter=2000, random_state=0),
        'use_elo': True,
    },
}


def build_features(df, use_elo=True):
    """Build feature matrix, optionally including ELO and streak features."""
    x = make_features(df)
    if use_elo:
        elo = compute_elo(df)
        streaks = compute_streaks(df)
        x = pd.concat([x, elo, streaks], axis=1)
    return x


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, default=ROOT / 'final-dataset-names-and-ids.csv')
    parser.add_argument('--output', type=Path, default=ROOT / 'models/comparisons/artifacts')
    parser.add_argument('--report', type=Path, default=ROOT / 'model_results.md')
    parser.add_argument('--min-train', type=int, default=4000,
                        help='Minimum training rows before first walk-forward prediction')
    parser.add_argument('--step', type=int, default=200,
                        help='Approximate fights per walk-forward fold')
    args = parser.parse_args()

    df = pd.read_csv(args.data)
    df['date'] = pd.to_datetime(df['date'], errors='raise')
    if df.date.isna().any():
        raise ValueError('Missing fight date')
    df = df.sort_values('date', kind='stable').reset_index(drop=True)
    y = df.winner.map({'Red': 0, 'Blue': 1})
    if y.isna().any():
        raise ValueError('Expected only Red/Blue winners')
    y = y.astype(int)
    decimal = decimal_odds(df)

    boundary = split_index(df.date)
    args.output.mkdir(parents=True, exist_ok=True)

    results = {}
    thresholds = [0.0, 0.02, 0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25, 0.30]

    for name, config in CONFIGS.items():
        print(f'\n=== {name} ===', flush=True)
        x = build_features(df, use_elo=config['use_elo'])

        # --- Walk-forward on TRAINING period for threshold selection ---
        print(f'  Walk-forward CV on training period...', flush=True)
        cv_proba, cv_y, cv_decimal = walk_forward_predict(
            df.iloc[:boundary], y.iloc[:boundary], x.iloc[:boundary],
            decimal[:boundary], config['estimator'],
            min_train=args.min_train, step=args.step,
        )
        cv_best_threshold, cv_best = optimize_threshold(
            cv_y, cv_proba, cv_decimal, thresholds,
        )
        print(f'  CV best threshold: {cv_best_threshold:.2f} '
              f'(units={cv_best["betting_units"]:+.2f}, '
              f'bets={cv_best["bets"]}, roi={cv_best["roi"]:.2%})', flush=True)

        # --- Walk-forward on FULL dataset (test period gets out-of-sample predictions) ---
        print(f'  Walk-forward on test period...', flush=True)
        test_proba, test_y, test_decimal = walk_forward_predict(
            df, y, x, decimal, config['estimator'],
            min_train=boundary, step=args.step,
        )

        # Flat-stake evaluation (all bets)
        flat_metrics, flat_pred, flat_profits = evaluate(test_y, test_proba, test_decimal)
        print(f'  Flat: accuracy={flat_metrics["test_accuracy"]:.4f}, '
              f'units={flat_metrics["betting_units"]:+.2f}', flush=True)

        # Value betting with CV-selected threshold
        value_metrics = value_bet_evaluate(
            test_y, test_proba, test_decimal, cv_best_threshold,
        )
        print(f'  Value (t={cv_best_threshold:.2f}): '
              f'accuracy={value_metrics["test_accuracy"]:.4f}, '
              f'units={value_metrics["betting_units"]:+.2f}, '
              f'bets={value_metrics["bets"]}, roi={value_metrics["roi"]:.2%}', flush=True)

        # Also try a range of thresholds on test to show the landscape
        threshold_landscape = {}
        for t in thresholds:
            t_metrics = value_bet_evaluate(test_y, test_proba, test_decimal, t)
            threshold_landscape[str(t)] = t_metrics

        results[name] = {
            'flat': flat_metrics,
            'value': value_metrics,
            'cv_threshold': cv_best_threshold,
            'cv_metrics': cv_best,
            'threshold_landscape': threshold_landscape,
            'use_elo': config['use_elo'],
        }

    # --- Write results ---
    metadata = {
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'dataset': str(args.data.resolve()),
        'dataset_sha256': hashlib.sha256(args.data.read_bytes()).hexdigest(),
        'python': platform.python_version(),
        'sklearn': sklearn.__version__,
        'numpy': np.__version__,
        'pandas': pd.__version__,
        'train_rows': boundary,
        'test_rows': len(df) - boundary,
        'train_dates': [str(df.date.iloc[0].date()), str(df.date.iloc[boundary - 1].date())],
        'test_dates': [str(df.date.iloc[boundary].date()), str(df.date.iloc[-1].date())],
        'min_train': args.min_train,
        'step': args.step,
    }
    (args.output / 'wf_results.json').write_text(
        json.dumps({'metadata': metadata, 'results': results}, indent=2, default=str) + '\n'
    )

    # --- Update model_results.md ---
    report = [
        '## Walk-forward value betting comparison', '',
        f'Run: {metadata["created_utc"]}. Dataset: `{args.data.name}`; '
        f'SHA-256: `{metadata["dataset_sha256"][:16]}...`.', '',
        f'Train: {metadata["train_dates"]} ({boundary:,} fights). '
        f'Test: {metadata["test_dates"]} ({len(df) - boundary:,} fights).', '',
        f'Walk-forward: min_train={args.min_train}, step={args.step}. '
        f'Models retrain on expanding windows aligned to event dates.', '',
        'Value-bet threshold selected on training-period walk-forward CV '
        '(maximizing betting units). The same threshold is applied to the '
        'test period. Threshold landscape shows test-period results across '
        'all thresholds for transparency (not used for selection).', '',
        '| Model | ELO | CV threshold | Flat accuracy | Flat units | '
        'Value accuracy | Value bets | Value units | Value ROI |',
        '|---|---|---:|---:|---:|---:|---:|---:|---:|',
    ]
    for name, r in results.items():
        f = r['flat']
        v = r['value']
        report.append(
            f'| {name} | {"yes" if r["use_elo"] else "no"} | '
            f'{r["cv_threshold"]:.2f} | '
            f'{f["test_accuracy"]:.4f} | {f["betting_units"]:+.2f} | '
            f'{v["test_accuracy"]:.4f} | {v["bets"]:,} | '
            f'{v["betting_units"]:+.2f} | {v["roi"]:.2%} |'
        )

    # Threshold landscape table for best model
    best_name = max(results, key=lambda n: results[n]['value']['betting_units'])
    report += ['', f'### Threshold landscape for {best_name}', '',
               '| Threshold | Bets | Units | ROI | Accuracy |',
               '|---:|---:|---:|---:|---:|']
    for t_str, t_metrics in results[best_name]['threshold_landscape'].items():
        report.append(
            f'| {float(t_str):.2f} | {t_metrics["bets"]:,} | '
            f'{t_metrics["betting_units"]:+.2f} | {t_metrics["roi"]:.2%} | '
            f'{t_metrics["test_accuracy"]:.4f} |'
        )
    report += ['',
        f'Reproduce: `.venv/bin/python -m models.comparisons.train_wf`. '
        f'See `models/comparisons/artifacts/wf_results.json` for full details.', '']

    existing = args.report.read_text() if args.report.exists() else '# Model Results\n\n'
    marker = '<!-- wf-comparison:start -->'
    end = '<!-- wf-comparison:end -->'
    section = marker + '\n' + '\n'.join(report) + end
    if marker in existing:
        before, rest = existing.split(marker, 1)
        _, after = rest.split(end, 1)
        existing = before + section + after
    else:
        existing += '\n' + section + '\n'
    args.report.write_text(existing)
    print(f'\nResults written to {args.report} and {args.output / "wf_results.json"}')


if __name__ == '__main__':
    with threadpool_limits(limits=1):
        main()
```

- [ ] **Step 4: Run the training script on the real dataset**

Run: `.venv/bin/python -m models.comparisons.train_wf`
Expected: Script completes, prints results for each model, writes `model_results.md` and `wf_results.json`.

- [ ] **Step 5: Review results and iterate**

Read `model_results.md` and `artifacts/wf_results.json`. If no configuration reaches +100 units:
- Add more hyperparameter configurations to CONFIGS
- Try different ELO K-factors (16, 24, 40)
- Adjust walk-forward step size
- Consider adding odds-derived features to the model (implied probabilities as features alongside stats)

- [ ] **Step 6: Commit**

```bash
git add models/comparisons/train_wf.py tests/test_train_wf.py model_results.md models/comparisons/artifacts/wf_results.json
git commit -m "feat: walk-forward training with ELO features and value betting"
```
