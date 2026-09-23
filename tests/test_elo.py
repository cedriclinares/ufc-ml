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
        # Fight 2: a vs c. a won fight 0, c lost fight 1 (b won as Blue)
        self.assertGreater(elo.loc[2, 'r_elo'], 1500)  # a won once
        self.assertLess(elo.loc[2, 'b_elo'], 1500)     # c lost once

    def test_elo_diff_sign(self):
        elo = compute_elo(self.df, k=32, initial=1500)
        for i in range(len(self.df)):
            self.assertAlmostEqual(
                elo.loc[i, 'elo_diff'],
                elo.loc[i, 'r_elo'] - elo.loc[i, 'b_elo'],
            )

    def test_elo_custom_k(self):
        elo_low = compute_elo(self.df, k=16, initial=1500)
        elo_high = compute_elo(self.df, k=64, initial=1500)
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
        self.assertEqual(streaks.loc[0, 'r_win_streak'], 0)
        self.assertEqual(streaks.loc[0, 'r_loss_streak'], 0)
        self.assertEqual(streaks.loc[0, 'b_win_streak'], 0)
        self.assertEqual(streaks.loc[0, 'b_loss_streak'], 0)

    def test_win_streak_builds(self):
        streaks = compute_streaks(self.df)
        self.assertEqual(streaks.loc[1, 'r_win_streak'], 1)
        self.assertEqual(streaks.loc[2, 'r_win_streak'], 2)

    def test_streak_resets_on_loss(self):
        streaks = compute_streaks(self.df)
        # Fight 3: a is in blue corner, lost fight 2
        self.assertEqual(streaks.loc[3, 'b_win_streak'], 0)
        self.assertEqual(streaks.loc[3, 'b_loss_streak'], 1)

    def test_streak_tracks_across_corners(self):
        streaks = compute_streaks(self.df)
        # Fighter 'a' appears as red in fights 0-2, blue in fight 3
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
