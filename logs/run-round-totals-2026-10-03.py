"""Evaluate the round-total prices supplied in the October 3 screenshot."""
from pathlib import Path
import sys,json
from datetime import datetime,timezone
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from threadpoolctl import threadpool_limits
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts.train_rounds_model import load_dataset,build_round_features
from models.comparisons.evaluation import pipeline
from scripts.predict_value_bets import american_to_decimal

card=[('King Green','Esteban Ribovics',3,1.5,-202,171),
      ('Deiveson Figueiredo','Payton Talbott',3,2.5,-110,-106),
      ('Ateba Gautier','Roman Kopylov',3,1.5,-190,161),
      ('Natalia Silva','Wang Cong',5,4.5,-250,208),
      ('Imanol Rodriguez','Alden Coria',3,2.5,-140,120),
      ('Roberto Soldic','Khaos Williams',3,1.5,-115,-101)]
day=pd.Timestamp('2026-10-03');threshold=.4;multiplier=3.
df,x,usable,_=load_dataset(ROOT/'final-dataset-names-and-ids.csv',ROOT/'fights.csv')
raw=pd.read_csv(ROOT/'fights.csv');raw['date']=pd.to_datetime(raw.date);raw=raw[raw.date<day]
upcoming=pd.read_csv(ROOT/'logs/prediction-inputs-2026-10-03.csv');upcoming['date']=pd.to_datetime(upcoming.date)
assert (upcoming.date==day).all()
models={};output=[];missing=[];params=dict(max_iter=200,max_leaf_nodes=15,learning_rate=.05,l2_regularization=2,early_stopping=False,random_state=0)
with threadpool_limits(limits=1):
 for scheduled in (3,5):
  mask=usable & (df.date<day) & df.number_of_rounds.eq(scheduled)
  model=pipeline(HistGradientBoostingClassifier(**params),x.columns)
  model.fit(x.loc[mask],df.loc[mask].rounds_last.map(lambda v:f'{v:.1f}'))
  models[scheduled]=model
  print(f'Trained {scheduled}-round duration classifier on {mask.sum()} fights',flush=True)
 for rn,bn,scheduled,line,over_odds,under_odds in card:
  found=upcoming[(upcoming.r_name==rn)&(upcoming.b_name==bn)]
  if len(found)!=1:
   missing.append(dict(fight=f'{rn} vs {bn}',line=line,over_odds=over_odds,under_odds=under_odds,reason='Missing Roberto Soldic fighter history'))
   continue
  row=found.iloc[0]
  assert pd.Timestamp(row.r_snapshot_date)<day and pd.Timestamp(row.b_snapshot_date)<day
  combined=pd.concat([df[df.date<day],pd.DataFrame([row])],ignore_index=True)
  features=build_round_features(combined,raw).iloc[[-1]]
  model=models[scheduled];prob=model.predict_proba(features)[0]
  assert np.isclose(prob.sum(),1)
  classes=np.asarray(model.classes_,dtype=float)
  under=float(prob[classes<=line].sum());over=float(prob[classes>line].sum())
  for side,probability,odds in [('Over',over,over_odds),('Under',under,under_odds)]:
   decimal=american_to_decimal(odds);ev=probability*decimal-1;size=max(0,multiplier*ev/(decimal-1))
   output.append(dict(fight=f'{rn} vs {bn}',scheduled_rounds=scheduled,line=line,side=side,odds=odds,
                      probability=probability,break_even=1/decimal,expected_roi=ev,
                      calculated_units=size,filtered_units=size if ev>=threshold else 0,
                      qualifies=bool(ev>=threshold and size>0)))
result=pd.DataFrame(output);result.to_csv(ROOT/'logs/round-totals-2026-10-03.csv',index=False)
metadata=dict(run_utc=datetime.now(timezone.utc).isoformat(),model='classifier_duration',params=params,
              odds_source='User screenshot Screenshot 2026-10-03 at 4.38.12 PM',event_date=str(day.date()),
              training_through=str(df.date.max().date()),ev_threshold=threshold,kelly_multiplier=multiplier,
              cutoff_rule='Under: ceiling-rounded duration <= line; Over: duration > line',
              missing=missing,sizing='Project unit convention: 3 * EV / (decimal odds - 1); not bankroll percentages',
              threshold_note='The 40% threshold is carried over from the moneyline strategy, not optimized for totals.')
(ROOT/'logs/round-totals-2026-10-03-metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
print(result.to_string(index=False));print('Skipped:',json.dumps(missing));print('Total filtered units:',result.filtered_units.sum())
