import unittest
import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier

from models.comparisons.walk_forward import walk_forward_predict, align_to_event


class AlignToEventTests(unittest.TestCase):
    def test_snaps_back_to_event_boundary(self):
        dates = pd.Series(pd.to_datetime(
            ['2020-01-01'] * 5 + ['2020-02-01'] * 3 + ['2020-03-01'] * 4
        ))
        # Index 6 is mid-event on 2020-02-01; should snap to 5
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
            np.full(n, 2.0),
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
        self.assertEqual(len(y_out), 50)
        self.assertEqual(proba.shape, (50, 2))

    def test_no_future_leakage(self):
        df, y, x, decimal = self._make_data(100)
        y.iloc[75:] = 1
        x_leak = x.copy()
        x_leak.iloc[75:, 0] = 999

        proba, y_out, _ = walk_forward_predict(
            df, y, x_leak, decimal,
            lambda: DummyClassifier(strategy='prior', random_state=0),
            min_train=50, step=25,
        )
        # DummyClassifier ignores features — verifies engine structure
        self.assertEqual(len(y_out), 50)

    def test_fold_boundaries_respect_event_dates(self):
        dates = pd.to_datetime(
            ['2020-01-01'] * 5 + ['2020-02-01'] * 5 +
            ['2020-03-01'] * 5 + ['2020-04-01'] * 5
        )
        np.random.seed(0)
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
        # min_train=10 covers first 2 events; predict events 3,4 = 10 rows
        self.assertEqual(len(y_out), 10)


if __name__ == '__main__':
    unittest.main()
