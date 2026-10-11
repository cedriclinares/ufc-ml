"""Pre-fight duration histories and ordered half-round estimators."""
from collections import defaultdict

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin, RegressorMixin
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor


def elapsed_seconds(value):
    if pd.isna(value):
        return np.nan
    parts = str(value).strip().split(':')
    try:
        numbers = [int(p) for p in parts]
    except ValueError:
        return np.nan
    if len(numbers) not in (2, 3) or (len(numbers) == 3 and numbers[2] != 0):
        return np.nan
    minutes, seconds = numbers[:2]
    return minutes * 60 + seconds if minutes >= 0 and 0 <= seconds < 60 else np.nan


def duration_features(targets, fights):
    """Emit features before applying any outcomes on the same event date.

    History comes from raw bouts, including bouts absent from the feature export.
    Recent form uses the last five bouts. Opponent adjustment is the mean residual
    against the opponent's average duration available before that historical bout.
    """
    history = defaultdict(list)
    fights = fights.copy()
    fights['date'] = pd.to_datetime(fights.date)
    targets = targets.copy()
    targets['date'] = pd.to_datetime(targets.date)
    output = {}

    def summarize(fid):
        records = history.get(fid, [])
        stats = {'duration_history_bouts': len(records)}
        fields = ('rounds', 'early_finish', 'early_win', 'early_loss', 'finish_win', 'finished', 'distance', 'ko_win',
                  'ko_loss', 'submission_win', 'submission_loss', 'opponent_residual')
        for field in fields:
            stats['duration_' + field + '_mean'] = np.mean([r[field] for r in records]) if records else np.nan
            stats['duration_recent5_' + field + '_mean'] = np.mean([r[field] for r in records[-5:]]) if records else np.nan
        return stats

    raw_groups = {date: group for date, group in fights.groupby('date')}
    target_groups = {date: group for date, group in targets.groupby('date')}
    for date in sorted(set(raw_groups) | set(target_groups)):
        if date in target_groups:
            for index, row in target_groups[date].iterrows():
                red, blue = summarize(row.r_id), summarize(row.b_id)
                features = {f'{corner}_{key}': value for corner, summary in [('r', red), ('b', blue)]
                            for key, value in summary.items()}
                for key in red:
                    features[f'matchup_{key}_mean'] = (red[key] + blue[key]) / 2
                    features[f'matchup_{key}_diff'] = red[key] - blue[key]
                output[index] = features
        pending = []
        if date in raw_groups:
            for _, fight in raw_groups[date].iterrows():
                elapsed = elapsed_seconds(fight.finish_time)
                scheduled, finish = fight.number_of_rounds, fight.finish_round
                if pd.isna(scheduled) or pd.isna(finish) or scheduled not in (3, 5) or \
                        not 1 <= finish <= scheduled or finish != int(finish) or not 0 <= elapsed <= 300:
                    continue
                rounds = finish - 1 + elapsed / 300
                distance = finish == scheduled and elapsed == 300
                method = str(fight.get('win_method', '')).casefold()
                ko = 'ko' in method
                submission = 'submission' in method
                winner = fight.get('winner')
                for corner, other in [('r', 'b'), ('b', 'r')]:
                    fid, opponent = fight.get(corner + '_id'), fight.get(other + '_id')
                    if pd.isna(fid):
                        continue
                    opponent_records = history.get(opponent, [])
                    opponent_mean = np.mean([r['rounds'] for r in opponent_records]) if opponent_records else rounds
                    won = winner in (('Red' if corner == 'r' else 'Blue'), fight.get(corner + '_name'))
                    lost = winner in (('Blue' if corner == 'r' else 'Red'), fight.get(other + '_name'))
                    pending.append((fid, dict(rounds=rounds, early_finish=float(rounds <= 1 and not distance),
                                              early_win=float(won and rounds <= 1 and (ko or submission)),
                                              early_loss=float(lost and rounds <= 1 and (ko or submission)),
                                              finish_win=float(won and (ko or submission)),
                                              finished=float(lost and (ko or submission)), distance=float(distance),
                                              ko_win=float(won and ko), ko_loss=float(lost and ko),
                                              submission_win=float(won and submission), submission_loss=float(lost and submission),
                                              opponent_residual=rounds-opponent_mean)))
        for fid, record in pending:
            history[fid].append(record)
    return pd.DataFrame.from_dict(output, orient='index').reindex(targets.index)


class HalfRoundRegressor(RegressorMixin, BaseEstimator):
    """Regress continuous elapsed rounds, then ceiling to a legal half-round."""
    def __init__(self, scheduled_rounds=3, max_iter=200):
        self.scheduled_rounds = scheduled_rounds
        self.max_iter = max_iter

    def fit(self, x, y):
        self.model_ = HistGradientBoostingRegressor(max_iter=self.max_iter, max_leaf_nodes=15,
                                                   learning_rate=.05, l2_regularization=2,
                                                   early_stopping=False, random_state=0)
        self.model_.fit(x, np.asarray(y, dtype=float))
        return self

    def predict(self, x):
        return np.ceil(np.clip(self.model_.predict(x), 0, self.scheduled_rounds) * 2) / 2


class HalfRoundSurvivalClassifier(ClassifierMixin, BaseEstimator):
    """Predict conditional survival through each boundary for fights still at risk."""
    def __init__(self, scheduled_rounds=3, max_iter=200):
        self.scheduled_rounds = scheduled_rounds
        self.max_iter = max_iter

    def fit(self, x, y):
        self.classes_ = np.array([f'{t / 2:.1f}' for t in range(self.scheduled_rounds * 2 + 1)])
        duration = np.asarray(y, dtype=float)
        self.models_ = []
        for boundary in np.arange(0, self.scheduled_rounds, .5):
            at_risk = np.ones(len(duration), dtype=bool) if boundary == 0 else duration > boundary-.5
            binary = (duration[at_risk] > boundary).astype(int)
            if len(binary) == 0:
                self.models_.append(0.)
                continue
            if binary.min() == binary.max():
                self.models_.append(float(binary[0]))
            else:
                model = HistGradientBoostingClassifier(max_iter=self.max_iter, max_leaf_nodes=15,
                                                       learning_rate=.05, l2_regularization=2,
                                                       early_stopping=False, random_state=0)
                model.fit(x[at_risk], binary)
                self.models_.append(model)
        return self

    def predict_survival(self, x):
        values = np.column_stack([np.full(len(x), model) if isinstance(model, float)
                                  else model.predict_proba(x)[:, 1] for model in self.models_])
        # Multiplying conditional survival gives P(T > t), decreasing naturally.
        return np.cumprod(np.clip(values, 0, 1), axis=1)

    def predict_proba(self, x):
        survival = self.predict_survival(x)
        edges = np.column_stack([np.ones(len(x)), survival, np.zeros(len(x))])
        return edges[:, :-1] - edges[:, 1:]

    def predict(self, x):
        return self.classes_[self.predict_proba(x).argmax(axis=1)]


def duration_metrics(actual, predicted, probabilities=None, classes=None):
    """Exact/near accuracy, ordinal error, and probability calibration on holdout rows."""
    from sklearn.metrics import classification_report, confusion_matrix, log_loss
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    classes = np.asarray(classes, dtype=float)
    errors = np.abs(actual-predicted)
    metrics = dict(accuracy=float(np.mean(errors == 0)),
                   within_half_round_accuracy=float(np.mean(errors <= .5)),
                   mean_absolute_error_rounds=float(np.mean(errors)),
                   confusion_matrix=confusion_matrix((actual*2).astype(int), (predicted*2).astype(int),
                                                    labels=(classes*2).astype(int)).tolist(),
                   classification_report=classification_report(
                       (actual*2).astype(int), (predicted*2).astype(int), labels=(classes*2).astype(int),
                       target_names=[f'{c:.1f}' for c in classes], output_dict=True, zero_division=0))
    if probabilities is None:
        metrics.update(log_loss=None, multiclass_brier=None, confidence_ece=None,
                       survival_brier=None, calibration_bins=None, survival_calibration=None)
        return metrics
    probabilities = np.asarray(probabilities, dtype=float)
    if probabilities.shape != (len(actual), len(classes)) or not np.isfinite(probabilities).all() or \
            (probabilities < -1e-10).any() or not np.allclose(probabilities.sum(axis=1), 1):
        raise ValueError('Invalid duration probability distribution')
    target_indices = np.searchsorted(classes, actual)
    truth = np.eye(len(classes))[target_indices]
    # Integer half-round ticks avoid float targets being interpreted as regression.
    metrics['log_loss'] = float(log_loss((actual*2).astype(int), probabilities,
                                        labels=(classes*2).astype(int)))
    metrics['multiclass_brier'] = float(np.mean(np.sum((probabilities-truth)**2, axis=1)))

    def bins(probability, observed):
        result = []
        for i in range(10):
            mask = (probability >= i/10) & ((probability < (i+1)/10) if i < 9 else (probability <= 1))
            if mask.any():
                result.append(dict(lower=i/10, upper=(i+1)/10, count=int(mask.sum()),
                                   mean_probability=float(probability[mask].mean()),
                                   observed_frequency=float(observed[mask].mean())))
        return result

    confidence = probabilities.max(axis=1)
    calibration = bins(confidence, (classes[probabilities.argmax(axis=1)] == actual).astype(float))
    metrics['calibration_bins'] = calibration
    metrics['confidence_ece'] = float(sum(b['count']*abs(b['mean_probability']-b['observed_frequency'])
                                        for b in calibration)/len(actual))
    survival = np.cumsum(probabilities[:, ::-1], axis=1)[:, ::-1][:, 1:]
    observed = (actual[:, None] > classes[None, :-1]).astype(float)
    metrics['survival_brier'] = float(np.mean((survival-observed)**2))
    metrics['survival_calibration'] = {
        f'{boundary:.1f}': bins(survival[:, i], observed[:, i])
        for i, boundary in enumerate(classes[:-1])}
    return metrics
