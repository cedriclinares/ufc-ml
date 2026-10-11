import tempfile
import unittest
from pathlib import Path

import pandas as pd

from models.comparisons.features import BASE_FEATURES
from scripts.train_rounds_model import ending_labels, elapsed_seconds, load_dataset


class RoundModelTests(unittest.TestCase):
    def test_distance_rule_and_last_round_stoppages(self):
        fights = pd.DataFrame({
            'number_of_rounds': [3, 3, 3, 5, 5, 3, 3, 3],
            'finish_round': [3, 3, 2, 5, 5, 3, 4, 3],
            'finish_time': ['05:00:00', '04:59:00', '05:00:00', '5:00',
                            '2:30', None, '01:00:00', '05:01:00'],
        })
        labels = ending_labels(fights)
        self.assertEqual(labels.iloc[:5].tolist(),
                         [3.0, 3.0, 2.0, 5.0, 4.5])
        self.assertTrue(labels.iloc[5:].isna().all())

    def test_time_formats(self):
        self.assertEqual(elapsed_seconds('05:00:00'), 300)
        self.assertEqual(elapsed_seconds('5:00'), 300)
        self.assertEqual(elapsed_seconds('01:30:00'), 90)
        self.assertTrue(pd.isna(elapsed_seconds('01:30:12')))
        self.assertTrue(pd.isna(elapsed_seconds('bad')))

    def test_join_outcomes_without_leaking_them_into_features(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            data = pd.DataFrame(0., index=range(2), columns=BASE_FEATURES)
            data['date'] = ['2020-01-01', '2020-01-11']
            data['r_id'], data['b_id'] = [1, 2], [2, 1]
            data.to_csv(directory / 'data.csv', index=False)
            fights = pd.DataFrame({
                'id': [10, 11], 'date': ['2020-01-01', '2020-01-11'],
                'r_fighter_id': [1, 2], 'b_fighter_id': [2, 1],
                'number_of_rounds': [3, 3], 'finish_round': [1, 3],
                'finish_time': ['01:30:00', '05:00:00'],
            })
            fights.to_csv(directory / 'fights.csv', index=False)
            df, features, usable, coverage = load_dataset(directory / 'data.csv', directory / 'fights.csv')
            self.assertEqual(df.rounds_last.tolist(), [0.5, 3.0])
            self.assertEqual(df.gone_distance.tolist(), [False, True])
            self.assertTrue(usable.all())
            self.assertEqual(coverage['matched_outcomes'], 2)
            self.assertEqual(features.loc[1, 'r_days_since_last_fight'], 10)
            for field in ('finish_round', 'finish_time', 'rounds_last', 'gone_distance', 'id', 'number_of_rounds'):
                self.assertNotIn(field, features.columns)
            fights = pd.concat([fights, fights.iloc[[0]]])
            fights.to_csv(directory / 'fights.csv', index=False)
            with self.assertRaisesRegex(ValueError, 'Ambiguous'):
                load_dataset(directory / 'data.csv', directory / 'fights.csv')

    def test_half_round_boundaries(self):
        fights = pd.DataFrame({
            'number_of_rounds': [5] * 8,
            'finish_round': [1, 1, 1, 1, 2, 4, 5, 5],
            'finish_time': ['0:01', '1:14', '1:15', '3:45', '2:30', '2:30', '2:30', '5:00'],
        })
        self.assertEqual(ending_labels(fights).tolist(), [.5, .5, .5, 1., 1.5, 3.5, 4.5, 5.])

    def test_ceiling_preserves_exact_halves_and_rounds_up_other_times(self):
        fights = pd.DataFrame({
            'number_of_rounds': [3] * 6,
            'finish_round': [1, 1, 1, 1, 2, 3],
            'finish_time': ['0:00', '2:30', '2:31', '3:00', '0:01', '4:59'],
        })
        self.assertEqual(ending_labels(fights).tolist(), [0., .5, 1., 1., 1.5, 3.])


if __name__ == '__main__':
    unittest.main()
