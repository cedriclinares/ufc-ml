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
        # Even odds: decimal 2.0 for both sides
        decimal = np.full((5, 2), 2.0)
        result = value_bet_evaluate(y, proba, decimal, edge_threshold=0.0)
        # All 5 bets placed (every prediction has edge >= 0 vs 50/50 implied)
        self.assertEqual(result['bets'], 5)
        # Predictions: [0, 1, 0, 1, 0]; actuals: [0, 1, 0, 1, 0]
        # All correct: 5 * (2.0 - 1) = +5.0
        self.assertAlmostEqual(result['betting_units'], 5.0)

    def test_high_threshold_filters_bets(self):
        y = np.array([0, 1, 0])
        proba = np.array([
            [0.9, 0.1],   # high confidence Red
            [0.45, 0.55], # low confidence Blue
            [0.8, 0.2],   # high confidence Red
        ])
        # Implied probs (normalized): Red=0.667, Blue=0.333
        decimal = np.array([
            [1.5, 3.0],
            [1.5, 3.0],
            [1.5, 3.0],
        ])
        result = value_bet_evaluate(y, proba, decimal, edge_threshold=0.15)
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
