"""Check settlement and the separation of winner picks from underdog value bets."""

import unittest
import numpy as np
import pandas as pd

from scripts.evaluate_underdog_bets import analyze, settle


class UnderdogBetTests(unittest.TestCase):
    def test_settlement_scales_wins_and_losses_by_stake(self):
        prices = np.array([[3.0, 1.5], [2.5, 1.6]])
        profits = settle([0, 1], [0, 0], prices, [0.5, 2.0])
        np.testing.assert_allclose(profits, [1.0, -2.0])

    def test_loss_baseline_and_matched_favorite_stakes(self):
        rows = pd.DataFrame(
            dict(
                date=["2024-01-01", "2024-01-08", "2024-01-15"],
                winner_side=[0, 0, 1],
                p_red=[0.6, 0.7, 0.2],
                red_decimal=[2.5, 1.5, 4.0],
                blue_decimal=[1.5, 3.0, 1.3],
            )
        )
        result, detail = analyze(rows)
        self.assertAlmostEqual(result["always_loss_accuracy"], 2 / 3)
        self.assertEqual(result["gbt_accuracy"], 1)
        strategy = result["strategies"]["underdog_ev_20_kelly3"]
        self.assertEqual(strategy["bets"], 1)
        self.assertAlmostEqual(strategy["wagered"], 1)
        self.assertAlmostEqual(strategy["units"], 1.5)
        opposite = result["strategies"]["favorite_same_underdog_ev_20_kelly3"]
        self.assertEqual(opposite["wagered"], strategy["wagered"])
        self.assertAlmostEqual(opposite["units"], -1)

    def test_value_underdog_can_be_predicted_to_lose_and_equal_odds_are_excluded(self):
        rows = pd.DataFrame(
            dict(
                date=["2024-01-01", "2024-01-08"],
                winner_side=[0, 1],
                p_red=[0.4, 0.7],
                red_decimal=[4.0, 2.0],
                blue_decimal=[1.3, 2.0],
            )
        )
        result, detail = analyze(rows)
        self.assertEqual(result["fights"], 1)
        self.assertEqual(result["excluded"], 1)
        self.assertEqual(result["predicted_underdog_wins"], 0)
        self.assertEqual(result["strategies"]["underdog_ev_40_kelly3"]["bets"], 1)
        self.assertAlmostEqual(
            result["strategies"]["underdog_ev_40_kelly3"]["units"], 1.8
        )
        self.assertFalse(detail.predicted_underdog_win.iloc[0])


if __name__ == "__main__":
    unittest.main()
