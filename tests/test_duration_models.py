import tempfile
import unittest
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from models.rounds.duration import (duration_features, duration_metrics,
                                   HalfRoundRegressor, HalfRoundSurvivalClassifier)


class DurationTests(unittest.TestCase):
    def test_history_cross_corner_same_date_and_future_exclusion(self):
        raw = pd.DataFrame([
            dict(date='2020-01-01', r_id=1, b_id=2, r_name='A', b_name='B', winner='A',
                 number_of_rounds=3, finish_round=1, finish_time='1:00', win_method='KO/TKO'),
            dict(date='2020-02-01', r_id=3, b_id=1, r_name='C', b_name='A', winner='C',
                 number_of_rounds=3, finish_round=3, finish_time='5:00', win_method='Decision'),
            dict(date='2020-03-01', r_id=1, b_id=2, r_name='A', b_name='B', winner='B',
                 number_of_rounds=3, finish_round=2, finish_time='2:30', win_method='Submission'),
        ])
        targets = pd.DataFrame(dict(date=['2020-01-01', '2020-02-01', '2020-02-01', '2020-04-01'],
                                    r_id=[1, 1, 2, 1], b_id=[2, 3, 1, 2]))
        features = duration_features(targets, raw)
        self.assertEqual(features.loc[0, 'r_duration_history_bouts'], 0)
        self.assertTrue(np.isnan(features.loc[0, 'r_duration_rounds_mean']))
        self.assertAlmostEqual(features.loc[1, 'r_duration_rounds_mean'], .2)
        self.assertEqual(features.loc[1, 'r_duration_finish_win_mean'], 1)
        self.assertEqual(features.loc[2, 'b_duration_history_bouts'], 1)
        self.assertEqual(features.loc[3, 'r_duration_history_bouts'], 3)
        self.assertAlmostEqual(features.loc[3, 'r_duration_submission_loss_mean'], 1/3)
        changed = raw.copy()
        changed.loc[1:, 'finish_time'] = '0:01'
        changed.loc[1:, 'winner'] = 'A'
        # Same-event and later outcomes cannot alter earlier inputs.
        pd.testing.assert_frame_equal(features.iloc[:3], duration_features(targets, changed).iloc[:3])

    def test_recent_five_and_opponent_adjustment(self):
        raw = pd.DataFrame([
            dict(date=f'2020-01-{i+1:02}', r_id=1, b_id=2, r_name='A', b_name='B', winner='A',
                 number_of_rounds=3, finish_round=1, finish_time=f'{i}:00', win_method='KO/TKO')
            for i in range(6)
        ])
        features = duration_features(pd.DataFrame(dict(date=['2020-02-01'], r_id=[1], b_id=[2])), raw)
        self.assertAlmostEqual(features.loc[0, 'r_duration_rounds_mean'], .5)
        self.assertAlmostEqual(features.loc[0, 'r_duration_recent5_rounds_mean'], .6)
        self.assertGreater(features.loc[0, 'r_duration_opponent_residual_mean'], 0)

    def test_survival_coherent_constant_boundaries_and_serialization(self):
        x = np.arange(16).reshape(-1, 1)
        y = np.array([.5, 1., 1.5, 2., 2.5, 3., 3., 3.] * 2)
        with threadpool_limits(limits=1):
            model = HalfRoundSurvivalClassifier(3, max_iter=2).fit(x, y)
            survival = model.predict_survival(x)
            self.assertTrue((np.diff(survival, axis=1) <= 0).all())
            probabilities = model.predict_proba(x)
            self.assertTrue((probabilities >= 0).all())
            np.testing.assert_allclose(probabilities.sum(axis=1), 1)
            self.assertTrue(np.isclose(model.predict(x).astype(float)*2,
                                       np.round(model.predict(x).astype(float)*2)).all())
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / 'model.joblib'
                joblib.dump(model, path)
                np.testing.assert_allclose(joblib.load(path).predict_proba(x), probabilities)

    def test_regression_ceiling_and_clipping(self):
        class FixedPredictor:
            def predict(self, x):
                return np.array([-.2, 0, .1, .5, .51, 2.6, 3.2])
        model = HalfRoundRegressor(3)
        model.model_ = FixedPredictor()
        np.testing.assert_array_equal(model.predict(np.zeros((7, 1))), [0, 0, .5, .5, 1, 3, 3])

    def test_conditional_survival_products(self):
        model = HalfRoundSurvivalClassifier(3)
        model.models_ = [1., .8, .5, .5, .5, .5]
        survival = model.predict_survival(np.zeros((1, 1)))
        np.testing.assert_allclose(survival, [[1., .8, .4, .2, .1, .05]])
        np.testing.assert_allclose(model.predict_proba(np.zeros((1, 1))),
                                   [[0., .2, .4, .2, .1, .05, .05]])

    def test_error_metrics_and_perfect_calibration(self):
        result = duration_metrics([0, .5, 1], [0, 1, 0], classes=[0, .5, 1])
        self.assertAlmostEqual(result['accuracy'], 1/3)
        self.assertAlmostEqual(result['within_half_round_accuracy'], 2/3)
        self.assertAlmostEqual(result['mean_absolute_error_rounds'], .5)
        self.assertIsNone(result['confidence_ece'])
        perfect = duration_metrics([0, .5, 1], [0, .5, 1], np.eye(3), [0, .5, 1])
        self.assertEqual(perfect['multiclass_brier'], 0)
        self.assertEqual(perfect['confidence_ece'], 0)
        self.assertEqual(perfect['survival_brier'], 0)
        self.assertEqual(sum(b['count'] for b in perfect['calibration_bins']), 3)


if __name__ == '__main__':
    unittest.main()
