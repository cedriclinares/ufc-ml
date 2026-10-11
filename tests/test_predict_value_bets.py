import unittest

import numpy as np
import pandas as pd

from scripts.predict_value_bets import (
    american_to_decimal, format_table, upcoming_row, value_result, weight_in_pounds,
)


class ValuePredictionTests(unittest.TestCase):
    def test_underdog_value_and_kelly_filter(self):
        fight = dict(red_name='Figueiredo', blue_name='Talbott', red_odds=475, blue_odds=-650)
        result = value_result(fight, [.2552340976243666, .7447659023756334], .4, 3)
        self.assertEqual(result['value_pick'], 'Figueiredo')
        self.assertAlmostEqual(result['expected_roi'], .4675960613401079)
        self.assertAlmostEqual(result['calculated_units'], .295323828214805)
        self.assertEqual(result['filtered_units'], result['calculated_units'])
        self.assertTrue(result['qualifies'])

    def test_lower_threshold_changes_filter_not_calculated_size(self):
        fight = dict(red_name='Gautier', blue_name='Kopylov', red_odds=-240, blue_odds=195)
        high = value_result(fight, [.5782083936104853, .4217916063895147], .4, 3)
        low = value_result(fight, [.5782083936104853, .4217916063895147], .2, 3)
        self.assertEqual(high['value_pick'], 'Kopylov')
        self.assertAlmostEqual(high['calculated_units'], .3758234443831825)
        self.assertFalse(high['qualifies'])
        self.assertEqual(high['filtered_units'], 0)
        self.assertTrue(low['qualifies'])
        self.assertEqual(high['calculated_units'], low['calculated_units'])

    def test_negative_ev_never_allocates_units(self):
        fight = dict(red_name='A', blue_name='B', red_odds=-110, blue_odds=-110)
        result = value_result(fight, [.5, .5], 0, 3)
        self.assertLess(result['expected_roi'], 0)
        self.assertEqual(result['calculated_units'], 0)
        self.assertFalse(result['qualifies'])

    def test_odds_and_weight_validation(self):
        self.assertAlmostEqual(american_to_decimal('\u2212135'), 1 + 100 / 135)
        self.assertEqual(weight_in_pounds("Women's Flyweight"), 125)
        self.assertEqual(weight_in_pounds('Light Heavyweight'), 205)
        self.assertEqual(weight_in_pounds('155 lbs'), 155)
        for odds in (None, 0, 99, float('inf'), 150.5):
            with self.assertRaises((ValueError, TypeError)):
                american_to_decimal(odds)
        with self.assertRaises(ValueError):
            weight_in_pounds('Unknown')

    def test_snapshot_features_and_exact_identity(self):
        fighters = pd.DataFrame([
            dict(id=1, name='A', _name_key='a', date_of_birth='2000-01-01',
                 height=70, reach=72, stance='Orthodox', gender='male'),
            dict(id=2, name='B', _name_key='b', date_of_birth='2000-01-01',
                 height=68, reach=70, stance='Southpaw', gender='male'),
        ])
        snapshots = pd.DataFrame([
            dict(fighter_id=1, date=pd.Timestamp('2025-01-01'), wins=3),
            dict(fighter_id=1, date=pd.Timestamp('2025-02-01'), wins=4),
            dict(fighter_id=2, date=pd.Timestamp('2025-02-01'), wins=2),
        ])
        fight = dict(red_name='A', blue_name='B', weight_class=155)
        row = upcoming_row(fight, pd.Timestamp('2025-03-01'), fighters, snapshots, {1, 2})
        self.assertEqual(row['r_wins'], 4)
        self.assertEqual(row['b_wins'], 2)
        self.assertEqual(row['r_age'], (pd.Timestamp('2025-03-01') - pd.Timestamp('2000-01-01')).days)
        self.assertTrue(np.isnan(row['r_sig_str_landed']))
        with self.assertRaisesRegex(ValueError, 'Missing fighter: Unknown A'):
            upcoming_row(dict(fight, red_name='Unknown A'), pd.Timestamp('2025-03-01'),
                         fighters, snapshots, {1, 2})

    def test_table_retains_missing_bouts(self):
        missing = pd.DataFrame([dict(fight='A vs Newcomer', status='Missing history: Newcomer',
                                    qualifies=False, calculated_units=0., filtered_units=0.)])
        table = format_table(missing)
        self.assertIn('A vs Newcomer', table)
        self.assertIn('Unavailable', table)
        self.assertIn('Missing history: Newcomer', table)
        self.assertIn('0 qualifying bets', table)


if __name__ == '__main__':
    unittest.main()
