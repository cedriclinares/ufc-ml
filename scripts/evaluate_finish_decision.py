"""Walk-forward finish-versus-distance accuracy and half-round cutoff accuracy."""
import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score, balanced_accuracy_score, confusion_matrix, log_loss, precision_score, recall_score
from threadpoolctl import threadpool_limits

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts.train_rounds_model import load_dataset,elapsed_seconds
from scripts.evaluate_rounds_walk_forward import walk_forward_windows
from models.comparisons.evaluation import pipeline,split_index


def binary_metrics(actual,probability):
    actual=np.asarray(actual,dtype=int);probability=np.asarray(probability,dtype=float)
    predicted=(probability>=.5).astype(int)
    return dict(accuracy=float(accuracy_score(actual,predicted)),
                balanced_accuracy=float(balanced_accuracy_score(actual,predicted)),
                distance_precision=float(precision_score(actual,predicted,zero_division=0)),
                distance_recall=float(recall_score(actual,predicted,zero_division=0)),
                finish_recall=float(recall_score(1-actual,1-predicted,zero_division=0)),
                brier=float(np.mean((probability-actual)**2)),
                log_loss=float(log_loss(actual,np.column_stack([1-probability,probability]),labels=[0,1])),
                confusion_matrix_finish_distance=confusion_matrix(actual,predicted,labels=[0,1]).tolist(),
                fights=len(actual))


def evaluate(output):
    df,x,usable,coverage=load_dataset(ROOT/'final-dataset-names-and-ids.csv',ROOT/'fights.csv')
    output.mkdir(parents=True,exist_ok=True)
    results={};all_rows=[]
    for scheduled in (3,5):
        mask=usable & df.number_of_rounds.eq(scheduled)
        subset=df.loc[mask].reset_index(drop=True);features=x.loc[mask].reset_index(drop=True)
        boundary=split_index(subset.date,.8);windows=walk_forward_windows(subset.date,boundary,100 if scheduled==3 else 40)
        y=subset.gone_distance.astype(int)
        chunks=[]
        for fold,(start,end) in enumerate(windows,1):
            print(f'{scheduled}-round finish/distance: fold {fold}/{len(windows)}',flush=True)
            model=pipeline(HistGradientBoostingClassifier(max_iter=200,max_leaf_nodes=15,learning_rate=.05,
                                                         l2_regularization=2,early_stopping=False,random_state=0),features.columns)
            model.fit(features.iloc[:start],y.iloc[:start])
            probability=model.predict_proba(features.iloc[start:end])[:,list(model.classes_).index(1)]
            rows=subset.iloc[start:end][['id','date','number_of_rounds','gone_distance']].copy()
            rows['fold']=fold;rows['train_through']=str(subset.date.iloc[start-1].date())
            rows['distance_probability']=probability;rows['predicted_distance']=probability>=.5
            prior=float(y.iloc[:start].mean());rows['baseline_distance_probability']=prior
            chunks.append(rows)
        pooled=pd.concat(chunks,ignore_index=True);all_rows.append(pooled)
        results[str(scheduled)]=dict(model=binary_metrics(pooled.gone_distance,pooled.distance_probability),
                                    baseline=binary_metrics(pooled.gone_distance,pooled.baseline_distance_probability),
                                    folds=len(windows))
    predictions=pd.concat(all_rows,ignore_index=True);predictions.to_csv(output/'finish_distance_predictions.csv',index=False)
    duration=pd.read_csv(ROOT/'models/rounds/walk_forward/predictions.csv')
    duration['continuous_rounds']=duration.finish_round-1+duration.finish_time.map(elapsed_seconds)/300
    cutoffs=[]
    for (scheduled,name),group in duration.groupby(['number_of_rounds','model']):
        if name.endswith('baseline') or name=='regression_duration':continue
        for cutoff in np.arange(.5,scheduled,.5):
            columns=[f'p_{t/2:.1f}' for t in range(int(cutoff*2)+1)]
            probability=group[columns].sum(axis=1).to_numpy()
            actual=(group.continuous_rounds<=cutoff).to_numpy()
            predicted=probability>=.5
            cutoffs.append(dict(number_of_rounds=int(scheduled),model=name,finish_by_rounds=float(cutoff),
                                accuracy=float(np.mean(predicted==actual)),
                                majority_accuracy=float(max(actual.mean(),1-actual.mean())),
                                brier=float(np.mean((probability-actual)**2)),fights=len(group)))
    pd.DataFrame(cutoffs).to_csv(output/'cutoff_accuracy.csv',index=False)
    metadata=dict(distance_rule='finish_round == number_of_rounds and elapsed finish_time == 300 seconds',
                  evaluation='Same expanding-window folds and configurations as round-duration evaluation',
                  probability_threshold=.5,coverage=coverage,
                  note='Distance is defined by elapsed time, not win method. Final duration bin is not used as a decision label.')
    (output/'results.json').write_text(json.dumps(dict(metadata=metadata,results=results),indent=2)+'\n')
    lines=['# Finish versus distance walk-forward accuracy','',
           'Distance uses the exact full-final-round rule. Positive class is distance; probability cutoff is 50%.','',
           '| Rounds | Model | Accuracy | Balanced accuracy | Finish recall | Distance recall | Distance precision | Brier |',
           '|---:|---|---:|---:|---:|---:|---:|---:|']
    for scheduled,values in results.items():
        for name in ('model','baseline'):
            v=values[name]
            lines.append(f'| {scheduled} | {name} | {v["accuracy"]:.1%} | {v["balanced_accuracy"]:.1%} | '
                         f'{v["finish_recall"]:.1%} | {v["distance_recall"]:.1%} | '
                         f'{v["distance_precision"]:.1%} | {v["brier"]:.4f} |')
    lines+=['','See cutoff_accuracy.csv for finish-by-0.5/1.0/etc. accuracy from the saved duration models.',
            'The majority_accuracy column in that cutoff table is descriptive evaluation-period prevalence, not a fitted baseline.','']
    (output/'report.md').write_text('\n'.join(lines));print('\n'.join(lines),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=ROOT/'models/rounds/finish_decision')
    args=parser.parse_args()
    with threadpool_limits(limits=1):evaluate(args.output)


if __name__=='__main__':main()
