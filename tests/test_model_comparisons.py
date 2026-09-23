import unittest

import numpy as np
import pandas as pd

from models.comparisons.evaluation import decimal_odds, evaluate, make_features, split_index
from models.comparisons.features import BASE_FEATURES


class ComparisonTests(unittest.TestCase):
    def test_event_date_stays_together(self):
        dates = pd.Series(pd.to_datetime(['2020-01-01'] * 7 + ['2020-02-01'] * 3))
        self.assertEqual(split_index(dates), 7)

    def test_settlement_and_ties(self):
        frame = pd.DataFrame({'r_fighter_odds': [150, -200, 100], 'b_fighter_odds': [-180, 170, -110]})
        result, predictions, profits = evaluate(pd.Series([0, 0, 1]), np.array([[0.8, 0.2], [0.5, 0.5], [0.8, 0.2]]), decimal_odds(frame))
        np.testing.assert_array_equal(predictions, [0, 0, 0])
        np.testing.assert_allclose(profits, [1.5, 0.5, -1])
        self.assertEqual(result['betting_units'], 1)
        self.assertEqual(result['confusion_matrix'], [[2, 0], [1, 0]])
        self.assertEqual(sum(b['count'] for b in result['calibration_bins']), 3)

    def test_invalid_odds(self):
        with self.assertRaises(ValueError):
            decimal_odds(pd.DataFrame({'r_fighter_odds': [0], 'b_fighter_odds': [100]}))

    def test_history_across_corners_and_same_day(self):
        df = pd.DataFrame(0.0, index=range(3), columns=BASE_FEATURES)
        df['date'] = pd.to_datetime(['2020-01-01', '2020-01-11', '2020-01-11'])
        df['r_id'] = ['a', 'c', 'a']
        df['b_id'] = ['b', 'a', 'd']
        x = make_features(df)
        self.assertEqual(x.loc[1, 'b_days_since_last_fight'], 10)
        self.assertEqual(x.loc[2, 'r_days_since_last_fight'], 10)
        self.assertEqual(x.loc[1, 'b_is_debut'], 0)
        self.assertEqual(x.loc[1, 'r_is_debut'], 1)
        self.assertNotIn('r_id', x.columns)
        self.assertNotIn('r_fighter_odds', x.columns)


if __name__ == '__main__':
    unittest.main()
