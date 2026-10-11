"""Run branch gb_med_multielo_odds with current post-fight snapshots, both-side EV."""
from pathlib import Path
import hashlib
import importlib.util
import json
import sys
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from threadpoolctl import threadpool_limits
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from models.comparisons.evaluation import make_features, decimal_odds, pipeline
spec=importlib.util.spec_from_file_location('branch_elo',ROOT/'logs/value-betting-runtime/elo.py')
elo=importlib.util.module_from_spec(spec);spec.loader.exec_module(elo)
def build_features(df,decimal):
 x=pd.concat([make_features(df),elo.compute_multi_elo(df,k_values=(16,32,48)),elo.compute_streaks(df),elo.compute_experience(df)],axis=1)
 market=1/decimal;market/=market.sum(axis=1,keepdims=True)
 return pd.concat([x,pd.DataFrame({'r_implied_prob':market[:,0],'b_implied_prob':market[:,1]},index=x.index)],axis=1)
day=pd.Timestamp('2026-10-03');threshold=.4;kelly_multiplier=3.
history=pd.read_csv(ROOT/'final-dataset-names-and-ids.csv');history['date']=pd.to_datetime(history.date);history=history[history.date<day].sort_values('date',kind='stable').reset_index(drop=True)
upcoming=pd.read_csv(ROOT/'logs/prediction-inputs-2026-10-03.csv');upcoming['date']=pd.to_datetime(upcoming.date);upcoming['winner']='Red'
assert (upcoming.date==day).all()
assert (pd.to_datetime(upcoming.r_snapshot_date)<day).all() and (pd.to_datetime(upcoming.b_snapshot_date)<day).all()
assert pd.concat([upcoming.r_id,upcoming.b_id]).is_unique
train_decimal=decimal_odds(history);new_decimal=decimal_odds(upcoming)
params=dict(max_iter=300,max_leaf_nodes=31,learning_rate=.05,l2_regularization=2,min_samples_leaf=20,early_stopping=False,random_state=0)
with threadpool_limits(limits=1):
 print(f'Training gb_med_multielo_odds on {len(history)} fights through {history.date.max().date()}',flush=True)
 train_x=build_features(history,train_decimal);model=pipeline(HistGradientBoostingClassifier(**params),train_x.columns);model.fit(train_x,history.winner.map({'Red':0,'Blue':1}).astype(int))
 result=[]
 for i,row in upcoming.iterrows():
  # Each fight is appended independently: placeholder outcomes never affect other predictions.
  temp=pd.concat([history,pd.DataFrame([row])],ignore_index=True)
  test_x=build_features(temp,np.vstack([train_decimal,new_decimal[i]] )).iloc[[-1]]
  assert list(test_x.columns)==list(train_x.columns)
  prob=model.predict_proba(test_x)[0];assert np.isclose(prob.sum(),1)
  ev=prob*new_decimal[i]-1;side=0 if ev[0]>ev[1] else 1;qualifies=bool(ev[side]>=threshold)
  kelly=max(0,kelly_multiplier*ev[side]/(new_decimal[i,side]-1))
  result.append(dict(fight=f'{row.r_name} vs {row.b_name}',red_probability=float(prob[0]),blue_probability=float(prob[1]),red_odds=int(row.r_fighter_odds),blue_odds=int(row.b_fighter_odds),red_ev=float(ev[0]),blue_ev=float(ev[1]),value_pick=row.r_name if side==0 else row.b_name,pick_probability=float(prob[side]),pick_odds=int(row.r_fighter_odds if side==0 else row.b_fighter_odds),expected_roi=float(ev[side]),qualifies_40pct=qualifies,qualifies_20pct=bool(ev[side]>=.2),branch_size_units=kelly if qualifies else 0,minimum_decimal_for_40pct=1.4/float(prob[side])))
out=pd.DataFrame(result);out.to_csv(ROOT/'logs/value-predictions-2026-10-03.csv',index=False)
prior=json.loads((ROOT/'logs/predictions-2026-10-03-metadata.json').read_text())
metadata=dict(run_utc=datetime.now(timezone.utc).isoformat(),branch_commit='0f60c75e29cd76ac0f176ce694d71cbbfbafb2a3',model='gb_med_multielo_odds',params=params,training_fights=len(history),history_through=str(history.date.max().date()),ev_threshold=threshold,kelly_multiplier=kelly_multiplier,odds_source=prior['source'],skipped=prior['skipped'],input_method='Latest repaired post-fight CSV snapshots, event-date age, verified IDs; branch Elo/streak/experience uses training dataset history.',dataset_sha256=hashlib.sha256((ROOT/'final-dataset-names-and-ids.csv').read_bytes()).hexdigest(),sizing_note='Branch linear Kelly multiplier outputs units; no bankroll compounding or bankroll allocation calculated.')
(ROOT/'logs/value-predictions-2026-10-03-metadata.json').write_text(json.dumps(metadata,indent=2))
print(out.to_string(index=False));print('Qualifying bets:',int(out.qualifies_40pct.sum()))
