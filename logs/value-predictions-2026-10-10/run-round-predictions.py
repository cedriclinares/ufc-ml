"""Reproduce October 10 round predictions using the prior live duration GBT."""
from pathlib import Path
import sys
import json
import hashlib
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from threadpoolctl import threadpool_limits

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.predict_value_bets import upcoming_row, american_to_decimal
from scripts.combine_tables import normalize_fighter_name
from scripts.train_rounds_model import load_dataset, build_round_features
from models.comparisons.evaluation import pipeline

OUT = Path(__file__).resolve().parent
card = json.loads((OUT / 'card.json').read_text())
day = pd.Timestamp(card['date'])
# BetUS totals retrieved October 10, 2026; order matches the saved card.
markets = [(3.5, -125, 105), (1.5, -170, 142), (2.5, -300, 246),
           (2.5, -180, 150), (2.5, -110, -110), (1.5, 100, -120),
           (1.5, -195, 162), (1.5, 190, -230), (1.5, 126, -150),
           (1.5, 100, -120), (2.5, -200, 166), (2.5, -345, 276)]
params = dict(max_iter=200, max_leaf_nodes=15, learning_rate=.05,
              l2_regularization=2, early_stopping=False, random_state=0)
df, x, usable, coverage = load_dataset(ROOT / 'final-dataset-names-and-ids.csv', ROOT / 'fights.csv')
history = df[df.date < day]
raw = pd.read_csv(ROOT / 'fights.csv')
raw['date'] = pd.to_datetime(raw.date)
raw = raw[raw.date < day]
fighters = pd.read_csv(ROOT / 'fighters.csv')
fighters['_name_key'] = fighters.name.map(lambda n: normalize_fighter_name(n).casefold())
snapshots = pd.read_csv(ROOT / 'total_fight_stats.csv').merge(
    raw[['id', 'date']], left_on='fight_id', right_on='id',
    suffixes=('', '_bout'), validate='many_to_one')
snapshots = snapshots[snapshots.snapshot_type.eq('post') & (snapshots.date < day)]
snapshots = snapshots.sort_values(['date', 'fight_id'], kind='stable')
known = set(history.r_id) | set(history.b_id)
upcoming = pd.DataFrame([upcoming_row(f, day, fighters, snapshots, known) for f in card['fights']])
assert len(upcoming) == len(markets) == 12
assert (pd.to_datetime(upcoming.r_snapshot_date) < day).all()
assert (pd.to_datetime(upcoming.b_snapshot_date) < day).all()
upcoming.to_csv(OUT / 'round-inputs.csv', index=False)
combined = pd.concat([history, upcoming], ignore_index=True)
future_x = build_round_features(combined, raw).iloc[-len(upcoming):]
models, train_counts = {}, {}
with threadpool_limits(limits=1):
    for scheduled in (3, 5):
        mask = usable & (df.date < day) & df.number_of_rounds.eq(scheduled)
        train_counts[scheduled] = int(mask.sum())
        print(f'Training {scheduled}-round classifier_duration on {mask.sum():,} fights', flush=True)
        model = pipeline(HistGradientBoostingClassifier(**params), x.columns)
        model.fit(x.loc[mask], df.loc[mask].rounds_last.map(lambda v: f'{v:.1f}'))
        models[scheduled] = model
        joblib.dump(model, OUT / f'rounds_{scheduled}_live.joblib')
    forecasts, bets, distributions = [], [], []
    for i, (fight, market) in enumerate(zip(card['fights'], markets)):
        scheduled = fight['number_of_rounds']
        line, over_odds, under_odds = market
        model = models[scheduled]
        p = model.predict_proba(future_x.iloc[[i]])[0]
        classes = np.asarray(model.classes_, dtype=float)
        assert np.isclose(p.sum(), 1) and (p >= 0).all()
        assert (classes <= scheduled).all() and np.allclose(classes * 2, np.round(classes * 2))
        name = f"{fight['red_name']} vs {fight['blue_name']}"
        under, over = float(p[classes <= line].sum()), float(p[classes > line].sum())
        assert np.isclose(under + over, 1)
        forecasts.append(dict(fight=name, predicted_rounds=float(classes[p.argmax()]),
                              line=line, over_probability=over, under_probability=under))
        distributions.append(dict(fight=name, **{f'p_{c:.1f}': float(v) for c, v in zip(classes, p)}))
        for side, prob, odds in [('Over', over, over_odds), ('Under', under, under_odds)]:
            decimal = american_to_decimal(odds)
            ev = prob * decimal - 1
            units = max(0., 3 * ev / (decimal - 1))
            bets.append(dict(fight=name, side=side, line=line, odds=odds, probability=prob,
                             expected_roi=ev, calculated_units=units,
                             units_20=units if ev >= .2 else 0., units_40=units if ev >= .4 else 0.))

forecasts = pd.DataFrame(forecasts)
bets = pd.DataFrame(bets)
forecasts.to_csv(OUT / 'round-predictions.csv', index=False)
bets.to_csv(OUT / 'round-bets.csv', index=False)
pd.DataFrame(distributions).to_csv(OUT / 'round-distributions.csv', index=False)
metadata = dict(generated_at=datetime.now(timezone.utc).isoformat(), model='classifier_duration',
                params=params, training_counts=train_counts, training_through=str(history.date.max().date()),
                odds_source='https://www.betus.com.pa/sportsbook/ufc/fight-night/',
                unit_multiplier=3, target='Elapsed duration ceiling to half rounds',
                cutoff_rule='Under: class <= line; Over: class > line. Equality assigned to Under.',
                threshold_note='20% and 40% thresholds carried over from moneylines; not validated for totals.',
                prediction_rule='Most probable half-round category; not expected duration or decision probability',
                coverage=coverage, input_sha256={n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest()
                    for n in ['final-dataset-names-and-ids.csv', 'fights.csv', 'fighters.csv', 'total_fight_stats.csv']})
(OUT / 'round-metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
print(forecasts.to_string(index=False))
print(bets.to_string(index=False))
print('20% total:', bets.units_20.sum(), '40% total:', bets.units_40.sum())
