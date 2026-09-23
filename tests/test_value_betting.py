import unittest
import numpy as np
from models.comparisons.value_betting import (
    kelly_filtered_evaluate, optimize_threshold, value_bet_evaluate,
)


class ValueBetTests(unittest.TestCase):
    def test_zero_threshold_bets_on_positive_ev_sides(self):
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
        result = value_bet_evaluate(y, proba, decimal, ev_threshold=0.0)
        # EV = prob * 2.0 - 1. For fight 0: EV_red=0.4, EV_blue=-0.4 -> bet Red
        # All predictions match argmax, all correct
        self.assertEqual(result['bets'], 5)
        self.assertAlmostEqual(result['betting_units'], 5.0)

    def test_ev_selects_underdog_when_value_exists(self):
        y = np.array([1])  # Blue wins
        # Model slightly favors Red, but Blue has much better odds
        proba = np.array([[0.52, 0.48]])
        decimal = np.array([[1.5, 3.0]])  # Red favorite, Blue underdog
        # EV on Red: 0.52 * 1.5 - 1 = -0.22
        # EV on Blue: 0.48 * 3.0 - 1 = +0.44
        result = value_bet_evaluate(y, proba, decimal, ev_threshold=0.0)
        self.assertEqual(result['bets'], 1)
        # Should bet on Blue (higher EV), and Blue wins -> profit = 3.0 - 1 = 2.0
        self.assertAlmostEqual(result['betting_units'], 2.0)

    def test_high_threshold_filters_bets(self):
        y = np.array([0, 1, 0])
        proba = np.array([
            [0.9, 0.1],
            [0.45, 0.55],
            [0.6, 0.4],
        ])
        decimal = np.array([
            [1.5, 3.0],
            [1.5, 3.0],
            [2.0, 2.0],
        ])
        # Fight 0: EV_red=0.9*1.5-1=0.35, EV_blue=0.1*3-1=-0.7 -> bet Red, EV=0.35
        # Fight 1: EV_red=0.45*1.5-1=-0.325, EV_blue=0.55*3-1=0.65 -> bet Blue, EV=0.65
        # Fight 2: EV_red=0.6*2-1=0.2, EV_blue=0.4*2-1=-0.2 -> bet Red, EV=0.2
        result = value_bet_evaluate(y, proba, decimal, ev_threshold=0.30)
        # Only fights with EV >= 0.30 pass: fight 0 (0.35) and fight 1 (0.65)
        self.assertEqual(result['bets'], 2)
        self.assertEqual(result['skipped'], 1)

    def test_no_bets_when_threshold_too_high(self):
        y = np.array([0, 1])
        proba = np.array([[0.55, 0.45], [0.45, 0.55]])
        decimal = np.full((2, 2), 2.0)
        result = value_bet_evaluate(y, proba, decimal, ev_threshold=0.90)
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


    def test_kelly_filtered_sizes_by_edge(self):
        y = np.array([1])  # Blue wins
        proba = np.array([[0.3, 0.7]])
        decimal = np.array([[2.0, 2.0]])
        # EV on Blue: 0.7*2.0-1 = +0.4 (best side), EV on Red: 0.3*2-1=-0.4
        # Kelly frac: (0.7*2-1)/(2-1) = 0.4
        # Bet size with fraction=1.0: 1.0 * 0.4 = 0.4
        # Profit: (2.0-1) * 0.4 = 0.4 (Blue wins)
        result = kelly_filtered_evaluate(y, proba, decimal, ev_threshold=0.0, fraction=1.0)
        self.assertEqual(result['bets'], 1)
        self.assertAlmostEqual(result['betting_units'], 0.4)
        self.assertAlmostEqual(result['total_wagered'], 0.4)

    def test_kelly_filtered_threshold_filters(self):
        y = np.array([0, 1])
        proba = np.array([[0.55, 0.45], [0.45, 0.55]])
        decimal = np.full((2, 2), 2.0)
        # EV: 0.55*2-1=0.1 for both best sides
        result = kelly_filtered_evaluate(y, proba, decimal, ev_threshold=0.5, fraction=1.0)
        self.assertEqual(result['bets'], 0)


if __name__ == '__main__':
    unittest.main()
