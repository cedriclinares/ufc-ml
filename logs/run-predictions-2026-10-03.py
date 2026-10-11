from pathlib import Path
import json
import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits
from models.comparisons.features import BASE_FEATURES
from models.comparisons.evaluation import make_features, decimal_odds
root=Path.cwd(); day=pd.Timestamp('2026-10-03')
card=[('Natalia Silva','Wang Cong',125,-205,170),('Deiveson Figueiredo','Payton Talbott',135,475,-650),('King Green','Esteban Ribovics',155,185,-225),('Roberto Soldic','Khaos Williams',170,-270,220),('Ateba Gautier','Roman Kopylov',185,-240,195),('Imanol Rodriguez','Alden Coria',125,-160,135),('Damian Pinas','Andrey Pulyaev',185,-550,400),('Marcus McGhee','Anthony Romero',145,-500,375),('Anthony Wint','Lucas Armand',265,-600,450),('Johnny Walker','Mick Parkin',265,-135,115),('Jacobe Smith','Bruce Whitehead',170,-1300,800),('Rafael Dos Anjos','Alexander Hernandez',155,220,-270),('Marvin Vettori','Ismail Naurdiev',185,115,-135),('Court McGee','Eric Nolan',170,185,-225)]
f=pd.read_csv('fighters.csv');h=pd.read_csv('fights.csv');h['date']=pd.to_datetime(h.date);h=h[h.date<day];s=pd.read_csv('total_fight_stats.csv').merge(h[['id','date']],left_on='fight_id',right_on='id',suffixes=('','_fight')).sort_values('date');s=s[s.snapshot_type.eq('post')];rows=[]; skipped=[]
for rn,bn,w,ro,bo in card:
 row=dict(date=day,r_name=rn,b_name=bn,weight=w,r_fighter_odds=ro,b_fighter_odds=bo);missing=[]
 for corner,name in [('r',rn),('b',bn)]:
  found=f[f.name.eq(name)]
  if found.empty: missing.append(name);continue
  fighter=found.iloc[0];fid=fighter.id;stats=s[s.fighter_id.eq(fid)]
  if stats.empty:missing.append(name);continue
  snap=stats.iloc[-1];row[f'{corner}_id']=fid
  for feature in BASE_FEATURES:
   if feature.startswith(corner+'_'):
    field=feature[2:];row[feature]=fighter[field] if field in ('height','reach','stance') else ((day-pd.Timestamp(fighter.date_of_birth)).days if field=='age' else snap.get(field,np.nan))
  row[f'{corner}_snapshot_date']=snap.date.strftime('%Y-%m-%d')
 row['gender']='female' if rn=='Natalia Silva' else 'male'
 if missing:skipped.append({'fight':rn+' vs '+bn,'missing_history':missing})
 else:rows.append(row)
new=pd.DataFrame(rows)
hist=h.rename(columns={'r_fighter_id':'r_id','b_fighter_id':'b_id'}).reindex(columns=list(dict.fromkeys(['date','r_id','b_id']+BASE_FEATURES)))
combined=pd.concat([hist,new],ignore_index=True).sort_values('date',kind='stable').reset_index(drop=True)
x=make_features(combined).tail(len(new));dec=decimal_odds(new);market=1/dec;market/=market.sum(axis=1,keepdims=True);out=[]
with threadpool_limits(limits=1):
 for path in sorted((root/'models/comparisons/artifacts').glob('*.joblib')):
  model=joblib.load(path); inp=pd.DataFrame(market,columns=['red_implied','blue_implied']) if path.stem in ('odds_only','majority_class') else x
  probs=model.predict_proba(inp)
  assert list(model.classes_)==[0,1]
  for i,p in enumerate(probs):
   choice=int(p.argmax());r=new.iloc[i];prob=float(p[choice]);odds=int(r[['r_fighter_odds','b_fighter_odds']].iloc[choice])
   out.append(dict(model=path.stem,fight=r.r_name+' vs '+r.b_name,pick=r[['r_name','b_name']].iloc[choice],probability=prob,odds=odds,break_even=1/dec[i,choice],expected_roi=prob*dec[i,choice]-1,r_snapshot_date=r.r_snapshot_date,b_snapshot_date=r.b_snapshot_date))
result=pd.DataFrame(out);result.to_csv('logs/predictions-2026-10-03.csv',index=False);new.to_csv('logs/prediction-inputs-2026-10-03.csv',index=False)
Path('logs/predictions-2026-10-03-metadata.json').write_text(json.dumps(dict(date=str(day.date()),history_through=str(h.date.max().date()),source='https://www.ufc.com/event/ufc-332',skipped=skipped,threshold=None,models='Existing saved chronological comparison models, not retrained'),indent=2))
print('Skipped:',json.dumps(skipped));print(result[result.model.isin(['gradient_boosting','logistic_regression','mlp'])].to_string(index=False))
