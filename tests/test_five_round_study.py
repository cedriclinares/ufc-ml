"""Paired uncertainty and missing-target checks for the five-round study report."""

import unittest

import numpy as np
import pandas as pd

from scripts.report_five_round_study import paired_interval, per_fight_scores
from scripts.calibrate_study_baselines import logit_probabilities
from models.transformers.network import probabilities


class FiveRoundStudyTests(unittest.TestCase):
    def test_tree_temperature_changes_confidence_without_changing_decisions(self):
        distribution = np.zeros((2, 11))
        distribution[:, 2] = 0.2
        distribution[:, 10] = 0.8
        values = dict(
            winner=np.array([0.8, 0.2]),
            distance=np.array([1.0, 0.0]),
            rounds=distribution,
        )
        calibrated = probabilities(
            logit_probabilities(values), dict(winner=2, rounds=2, distance=2)
        )
        np.testing.assert_allclose(calibrated["winner"].numpy(), [2 / 3, 1 / 3])
        np.testing.assert_array_equal(
            calibrated["rounds"].argmax(axis=1).numpy(), [10, 10]
        )
        np.testing.assert_allclose(calibrated["rounds"].sum(axis=1).numpy(), 1)
        self.assertEqual(calibrated["rounds"][0, 0], 0)

    def test_constant_paired_difference_and_missing_targets(self):
        interval = paired_interval([0.2, np.nan, 0.2, 0.2], ["a", "a", "b", "b"], 100)
        self.assertAlmostEqual(interval["difference"], 0.2)
        self.assertAlmostEqual(interval["low"], 0.2)
        self.assertAlmostEqual(interval["high"], 0.2)
        self.assertEqual(interval["fights"], 3)
        self.assertEqual(interval["event_dates"], 2)

    def test_bootstrap_keeps_event_clusters_and_unequal_sizes(self):
        interval = paired_interval([0, 0, 0, 1], ["a", "a", "a", "b"], 1000)
        self.assertAlmostEqual(interval["difference"], 0.25)
        self.assertEqual(interval["low"], 0)
        self.assertEqual(interval["high"], 1)
        self.assertEqual(
            interval, paired_interval([0, 0, 0, 1], ["a", "a", "a", "b"], 1000)
        )

    def test_probability_scores_and_masking(self):
        rows = pd.DataFrame(
            dict(
                winner_target=[1.0, np.nan],
                p_red=[0.8, 0.4],
                rounds_target=[5.0, np.nan],
                prediction_rounds=[5.0, 1.0],
                distance_target=[1.0, np.nan],
                p_distance=[0.7, 0.4],
            )
        )
        for i in range(11):
            rows[f"p_{i / 2:.1f}"] = 0.8 if i == 10 else 0.2 if i == 2 else 0.0
        scores = per_fight_scores(rows)
        self.assertAlmostEqual(scores["winner_log_loss"][0], -np.log(0.8))
        self.assertAlmostEqual(scores["winner_brier"][0], 0.04)
        self.assertAlmostEqual(scores["duration_log_loss"][0], -np.log(0.8))
        self.assertEqual(scores["duration_mae"][0], 0)
        self.assertAlmostEqual(scores["over_4.5_brier"][0], 0.04)
        self.assertAlmostEqual(scores["distance_brier"][0], 0.09)
        for values in scores.values():
            self.assertTrue(np.isnan(values[1]))


if __name__ == "__main__":
    unittest.main()
