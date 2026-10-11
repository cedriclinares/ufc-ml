"""Print an upcoming UFC card with both-side value selection and Kelly sizes."""
import argparse
from contextlib import redirect_stdout
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from threadpoolctl import threadpool_limits

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from models.comparisons.elo import compute_multi_elo, compute_streaks, compute_experience
from models.comparisons.evaluation import decimal_odds, make_features, pipeline
from models.comparisons.features import BASE_FEATURES
from scripts.combine_tables import normalize_fighter_name


def american_to_decimal(odds):
    value = float(str(odds).replace('\u2212', '-'))
    if not np.isfinite(value) or abs(value) < 100 or not value.is_integer():
        raise ValueError(f'Invalid American odds: {odds!r}')
    return 1 + (value / 100 if value > 0 else 100 / abs(value))


def weight_in_pounds(value):
    try:
        weight = float(value)
    except (TypeError, ValueError):
        text = str(value).lower().replace('-', ' ')
        classes = [('strawweight', 115), ('flyweight', 125), ('bantamweight', 135),
                   ('featherweight', 145), ('lightweight', 155), ('welterweight', 170),
                   ('middleweight', 185), ('light heavyweight', 205), ('heavyweight', 265)]
        for label, pounds in classes:
            if label in text:
                return pounds
        # Tapology may provide e.g. "155 lbs".
        import re
        match = re.fullmatch(r'\s*(\d+(?:\.\d+)?)\s*(?:lbs?\.?|pounds?)\s*', text)
        if not match:
            raise ValueError(f'Unknown weight class: {value!r}; supply pounds')
        weight = float(match[1])
    if not np.isfinite(weight) or weight <= 0:
        raise ValueError(f'Invalid weight: {value!r}')
    return weight


def build_features(history, decimal):
    features = pd.concat([make_features(history),
                          compute_multi_elo(history, k_values=(16, 32, 48)),
                          compute_streaks(history), compute_experience(history)], axis=1)
    implied = 1 / decimal
    market = implied / implied.sum(axis=1, keepdims=True)
    return pd.concat([features, pd.DataFrame({
        'r_implied_prob': market[:, 0], 'b_implied_prob': market[:, 1],
    }, index=features.index)], axis=1)


def value_result(fight, probabilities, threshold, multiplier):
    decimal = np.array([american_to_decimal(fight['red_odds']),
                        american_to_decimal(fight['blue_odds'])])
    probabilities = np.asarray(probabilities, dtype=float)
    if probabilities.shape != (2,) or not np.isfinite(probabilities).all() or \
            (probabilities < 0).any() or not np.isclose(probabilities.sum(), 1):
        raise ValueError('Expected Red/Blue probabilities summing to one')
    ev = probabilities * decimal - 1
    side = 0 if ev[0] > ev[1] else 1  # Match the branch predictor's tie rule.
    calculated = max(0., multiplier * ev[side] / (decimal[side] - 1))
    qualifies = bool(ev[side] >= threshold and calculated > 0)
    return {
        'fight': f"{fight['red_name']} vs {fight['blue_name']}",
        'value_pick': fight['red_name'] if side == 0 else fight['blue_name'],
        'pick_odds': int(float(str(fight['red_odds'] if side == 0 else fight['blue_odds']).replace('\u2212', '-'))),
        'pick_probability': float(probabilities[side]), 'expected_roi': float(ev[side]),
        'qualifies': qualifies, 'calculated_units': calculated,
        'filtered_units': calculated if qualifies else 0., 'status': 'OK',
        'red_probability': float(probabilities[0]), 'blue_probability': float(probabilities[1]),
        'red_ev': float(ev[0]), 'blue_ev': float(ev[1]),
    }


def upcoming_row(fight, day, fighters, snapshots, known_ids):
    row = {'date': day, 'winner': 'Red', 'weight': weight_in_pounds(fight['weight_class'])}
    missing = []
    for corner, name in [('r', fight['red_name']), ('b', fight['blue_name'])]:
        key = normalize_fighter_name(name).casefold()
        matches = fighters[fighters['_name_key'] == key]
        if len(matches) != 1:
            missing.append(f"{'Ambiguous fighter' if len(matches) > 1 else 'Missing fighter'}: {name}")
            continue
        fighter = matches.iloc[0]
        stats = snapshots[snapshots.fighter_id == fighter.id]
        if stats.empty:
            missing.append(f'Missing history: {name}')
            continue
        if fighter.id not in known_ids:
            missing.append(f'Missing model history: {name}')
            continue
        snapshot = stats.iloc[-1]
        row[f'{corner}_id'] = fighter.id
        row[f'{corner}_name'] = name
        for feature in BASE_FEATURES:
            if not feature.startswith(corner + '_'):
                continue
            field = feature[2:]
            if field in ('height', 'reach', 'stance'):
                row[feature] = fighter[field]
            elif field == 'age':
                dob = pd.to_datetime(fighter.date_of_birth, errors='coerce')
                row[feature] = (day - dob).days if pd.notna(dob) else np.nan
            else:
                row[feature] = snapshot.get(field, np.nan)
        if corner == 'r':
            row['gender'] = fighter.gender
        row[f'{corner}_snapshot_date'] = snapshot.date.strftime('%Y-%m-%d')
    # Matchup metadata can correct missing or incorrect exported gender.
    text = str(fight['weight_class']).lower()
    if 'women' in text or 'female' in text:
        row['gender'] = 'female'
    if missing:
        raise ValueError('; '.join(missing))
    return row


def predict_card(card, day, data, fighters_path, snapshots_path, fights_path,
                 threshold=.4, multiplier=3.):
    history = pd.read_csv(data)
    history['date'] = pd.to_datetime(history.date, errors='raise')
    history = history[history.date < day].sort_values('date', kind='stable').reset_index(drop=True)
    if history.empty or history.winner.map({'Red': 0, 'Blue': 1}).isna().any():
        raise ValueError('Training data must contain dated Red/Blue results before the event')
    fighters = pd.read_csv(fighters_path)
    fighters['_name_key'] = fighters.name.map(lambda name: normalize_fighter_name(name).casefold())
    bouts = pd.read_csv(fights_path)
    bouts['date'] = pd.to_datetime(bouts.date, errors='raise')
    if bouts.id.duplicated().any():
        raise ValueError('Duplicate historical fight IDs')
    snapshots = pd.read_csv(snapshots_path).merge(
        bouts[['id', 'date']], left_on='fight_id', right_on='id', suffixes=('', '_bout'),
        validate='many_to_one')
    snapshots = snapshots[(snapshots.snapshot_type == 'post') & (snapshots.date < day)]
    snapshots = snapshots.sort_values(['date', 'fight_id'], kind='stable')
    known_ids = set(history.r_id) | set(history.b_id)
    prepared, results, seen = [], [], set()
    for fight in card:
        if not fight.get('red_name') or not fight.get('blue_name'):
            raise ValueError('Each fight needs red_name and blue_name')
        key = frozenset(normalize_fighter_name(fight[c]).casefold()
                        for c in ('red_name', 'blue_name'))
        if len(key) != 2 or key in seen:
            raise ValueError(f'Duplicate fight or fighter: {fight}')
        seen.add(key)
        try:
            for side in ('red', 'blue'):
                american_to_decimal(fight.get(side + '_odds'))
            row = upcoming_row(fight, day, fighters, snapshots, known_ids)
        except (ValueError, TypeError) as error:
            results.append({'fight': f"{fight['red_name']} vs {fight['blue_name']}",
                            'status': str(error), 'qualifies': False,
                            'calculated_units': 0., 'filtered_units': 0.})
            continue
        prepared.append((len(results), fight, row))
        results.append(None)
    if not prepared:
        return pd.DataFrame(results), len(history), history.date.max()
    decimal = decimal_odds(history)
    with threadpool_limits(limits=1):
        print(f'Training gb_med_multielo_odds on {len(history):,} fights through '
              f'{history.date.max().date()}...', file=sys.stderr, flush=True)
        features = build_features(history, decimal)
        model = pipeline(HistGradientBoostingClassifier(
            max_iter=300, max_leaf_nodes=31, learning_rate=.05, l2_regularization=2,
            min_samples_leaf=20, early_stopping=False, random_state=0), features.columns)
        model.fit(features, history.winner.map({'Red': 0, 'Blue': 1}).astype(int))
        if list(model.classes_) != [0, 1]:
            raise ValueError('Model must have Red and Blue classes')
        for index, fight, row in prepared:
            # Predict separately so placeholder outcomes cannot affect other bouts.
            combined = pd.concat([history, pd.DataFrame([row])], ignore_index=True)
            odds = [[american_to_decimal(fight['red_odds']), american_to_decimal(fight['blue_odds'])]]
            x = build_features(combined, np.vstack([decimal, odds])).iloc[[-1]]
            result = value_result(fight, model.predict_proba(x)[0], threshold, multiplier)
            result.update({key: row[key] for key in ('r_snapshot_date', 'b_snapshot_date')})
            results[index] = result
    return pd.DataFrame(results), len(history), history.date.max()


def format_table(results, threshold=.4, multiplier=3.):
    headers = ['Fight', 'Value side', 'Odds', 'Win probability', 'Expected return',
               'Qualifies', f'Size ({multiplier:g}x Kelly)', f'Size after {threshold:.0%} EV filter']
    rows = []
    for _, result in results.iterrows():
        if result.status != 'OK':
            rows.append([result.fight, '—', '—', '—', '—', 'Unavailable', '—', '—'])
        else:
            rows.append([result.fight, result.value_pick, f'{int(result.pick_odds):+d}',
                         f'{result.pick_probability:.1%}', f'{result.expected_roi:+.1%}',
                         'Yes' if result.qualifies else 'No', f'{result.calculated_units:.2f}u',
                         f'{result.filtered_units:.2f}u'])
    def escape(value):
        return str(value).replace('|', '\\|').replace('\n', ' ')
    lines = ['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join(['---'] * len(headers)) + ' |']
    lines.extend('| ' + ' | '.join(escape(value) for value in row) + ' |' for row in rows)
    bet_count = int(results.qualifies.sum())
    lines.append(f'\nTotal calculated: {results.calculated_units.sum():.2f}u; '
                 f'after filter: {results.filtered_units.sum():.2f}u '
                 f'({bet_count} qualifying {"bet" if bet_count == 1 else "bets"}).')
    skipped = results[results.status != 'OK']
    if not skipped.empty:
        lines.append('\nSkipped:')
        lines.extend(f'- {row.fight}: {row.status}' for _, row in skipped.iterrows())
    lines.append('\nSizes follow the project’s multiplier convention in units, not bankroll percentages.')
    return '\n'.join(lines)


def load_card(path):
    text = sys.stdin.read() if str(path) == '-' else Path(path).read_text()
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        return [json.loads(line) for line in text.splitlines() if line.strip()], None, None
    if isinstance(document, dict) and 'fights' in document:
        return document['fights'], document.get('date'), document.get('event')
    if isinstance(document, list):
        return document, None, None
    if isinstance(document, dict) and 'red_name' in document:
        return [document], None, None
    raise ValueError('Expected a fight list, JSON lines, or an event object with fights')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('card', nargs='?', help='Fight-card JSON/JSONL file, or - for stdin')
    parser.add_argument('--url', help='Tapology event URL to fetch instead of a file')
    parser.add_argument('--headless', action='store_true', help='Hide the browser when fetching')
    parser.add_argument('--date', help='Event date YYYY-MM-DD; overrides JSON date')
    parser.add_argument('--ev-threshold', type=float, default=.4)
    parser.add_argument('--kelly-multiplier', '--kelly-fraction', type=float, default=3.)
    parser.add_argument('--data', type=Path, default=ROOT / 'final-dataset-names-and-ids.csv')
    parser.add_argument('--fighters', type=Path, default=ROOT / 'fighters.csv')
    parser.add_argument('--snapshots', type=Path, default=ROOT / 'total_fight_stats.csv')
    parser.add_argument('--history', type=Path, default=ROOT / 'fights.csv')
    parser.add_argument('--csv', type=Path, help='Also save all results to CSV')
    args = parser.parse_args()
    if bool(args.card) == bool(args.url):
        parser.error('Provide either a card file or --url')
    if not np.isfinite(args.ev_threshold) or args.ev_threshold < 0:
        parser.error('--ev-threshold must be finite and nonnegative')
    if not np.isfinite(args.kelly_multiplier) or args.kelly_multiplier <= 0:
        parser.error('--kelly-multiplier must be finite and positive')
    try:
        if args.url:
            if not args.date:
                parser.error('--url requires --date YYYY-MM-DD')
            from scripts.fetch_upcoming import fetch_upcoming
            with redirect_stdout(sys.stderr):
                card = fetch_upcoming(args.url, headless=args.headless)
            card_date, title = None, args.url
        else:
            card, card_date, title = load_card(args.card)
        if not isinstance(card, list) or not card or not all(isinstance(f, dict) for f in card):
            raise ValueError('The card must be a nonempty list of fights')
        date_text = args.date or card_date
        if not date_text:
            raise ValueError('Provide --date YYYY-MM-DD or a date in the card JSON')
        from datetime import date
        day = pd.Timestamp(date.fromisoformat(date_text))
        results, count, last_date = predict_card(
            card, day, args.data, args.fighters, args.snapshots, args.history,
            args.ev_threshold, args.kelly_multiplier)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    print(f'{title or "Upcoming UFC event"} — {day.date()}\n')
    print(f'Model: gb_med_multielo_odds | History: {count:,} fights through {last_date.date()}\n')
    print(format_table(results, args.ev_threshold, args.kelly_multiplier))
    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        results.to_csv(args.csv, index=False)
        print(f'\nSaved: {args.csv}')


if __name__ == '__main__':
    main()
