import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import joblib
import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from threadpoolctl import threadpool_limits

from models.rounds.duration import HalfRoundRegressor
from scripts.evaluate_rounds_walk_forward import evaluate, walk_forward_windows


class WalkForwardTests(unittest.TestCase):
    def test_event_alignment_coverage_and_small_steps(self):
        dates = pd.Series(pd.to_datetime(['2020-01-01']*3 + ['2020-02-01']*4 +
                                        ['2020-03-01']*2 + ['2020-04-01']*3))
        for step in (1, 3, 100):
            folds = walk_forward_windows(dates, 5, step)
            covered = []
            for start, end in folds:
                self.assertLess(dates.iloc[start-1], dates.iloc[start])
                if end < len(dates):
                    self.assertLess(dates.iloc[end-1], dates.iloc[end])
                covered.extend(range(start,end))
            self.assertEqual(covered, list(range(3,len(dates))))

    def test_invalid_windows(self):
        for dates, initial, step in [(['2020-01-01']*3,1,1),
                                      (['2020-02-01','2020-01-01'],1,1),
                                      (['2020-01-01','2020-02-01'],1,0)]:
            with self.assertRaises(ValueError):
                walk_forward_windows(dates,initial,step)

    def test_evaluation_refits_preprocessing_and_baselines_without_future_rows(self):
        dates = pd.date_range('2020-01-01',periods=12).repeat(2)
        target = np.array([.5]*8 + [3.]*16)
        df = pd.DataFrame(dict(id=np.arange(24),date=dates,r_name='A',b_name='B',
                               number_of_rounds=3,finish_round=np.where(target==.5,1,3),
                               finish_time=np.where(target==.5,'2:30','5:00'),
                               rounds_last=target,gone_distance=target==3))
        features = pd.DataFrame(dict(value=np.arange(24,dtype=float),r_duration_rounds_mean=target))
        usable = pd.Series(True,index=df.index)
        def estimator(name,scheduled,iterations):
            return HalfRoundRegressor(scheduled,2) if name=='regression_duration' else DummyClassifier(strategy='prior')
        with tempfile.TemporaryDirectory() as tmp, threadpool_limits(limits=1):
            root = Path(tmp)
            for name in ('data.csv','fights.csv'):
                (root/name).write_text('fixture\n')
            with patch('scripts.evaluate_rounds_walk_forward.load_dataset',
                       return_value=(df,features,usable,{})), \
                 patch('scripts.evaluate_rounds_walk_forward.estimator_for',side_effect=estimator), \
                 contextlib.redirect_stdout(io.StringIO()):
                results = evaluate(root/'data.csv',root/'fights.csv',root/'output',.5,3,3,2)
            rows = pd.read_csv(root/'output/predictions.csv')
            for name, group in rows.groupby('model'):
                self.assertEqual(len(group),12)
                self.assertEqual(group.id.nunique(),12)
                self.assertTrue((pd.to_datetime(group.train_through)<pd.to_datetime(group.date)).all())
            majority = rows[rows.model=='majority_baseline']
            for fold in results['3']['folds']:
                expected = float(pd.Series(target[:fold['train_rows']]).value_counts().idxmax())
                self.assertTrue((majority[majority.fold==fold['fold']].prediction==expected).all())
            model = joblib.load(root/'output/rounds_3_classifier_duration_last_fold.joblib')
            scaler = model.named_steps['columntransformer'].named_transformers_['numeric'].named_steps['standardscaler']
            last_start = results['3']['folds'][-1]['train_rows']
            np.testing.assert_allclose(scaler.mean_,features.iloc[:last_start].mean().to_numpy())
            self.assertTrue((root/'output/fold_metrics.csv').exists())
            self.assertTrue((root/'output/report.md').exists())


if __name__ == '__main__':
    unittest.main()
