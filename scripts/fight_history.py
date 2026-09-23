"""Deterministic in-memory pre/post-fight features built from raw bouts.
Only post-fight snapshots are persisted; pre values validate historical lookups.

All bouts on the same date use the history available before that date. Missing
measurements remain unknown; rounds that were never fought contribute zero.
"""
from collections import defaultdict
from datetime import date
from itertools import groupby

try:
    from .combine_tables import normalize_fighter_name
except ImportError:
    from combine_tables import normalize_fighter_name

CATEGORIES = ('sig_str', 'head', 'body', 'legs', 'distance', 'clinch', 'ground',
              'total_str', 'td', *(f'round_{i}_sig_str' for i in range(1, 6)))
RATIO_FEATURES = (
    'sig_str_accuracy', 'total_str_accuracy', 'td_accuracy',
    'head_accuracy', 'body_accuracy', 'legs_accuracy',
    'distance_accuracy', 'clinch_accuracy', 'ground_accuracy',
)
DIFF_FEATURES = (
    'wins_diff', 'losses_diff', 'draws_diff', 'age_diff', 'height_diff',
    'reach_diff', 'sig_str_landed_diff', 'sig_str_attempted_diff',
    'total_str_landed_diff', 'total_str_attempted_diff',
    'td_landed_diff', 'td_attempted_diff', 'kd_landed_diff',
    'ctrl_time_diff', 'fight_time_diff',
)
TIME_FEATURES = ('days_since_last_fight', 'is_debut')
DERIVED_FEATURES = RATIO_FEATURES + DIFF_FEATURES + TIME_FEATURES
COUNT_FIELDS = ('wins', 'losses', 'draws', 'championship_fights', 'ctrl_time',
                'opponent_ctrl_time', 'kd_landed', 'kd_absorbed', 'subs_attempted',
                'subs_evaded', 'opponent_wins', 'opponent_loses', 'fight_time',
                'opponent_fight_time', 'reversals') + tuple(
                    f'{category}_{suffix}' for category in CATEGORIES
                    for suffix in ('landed', 'attempted', 'absorbed', 'evaded'))


def as_date(value):
    return value if isinstance(value, date) else date.fromisoformat(value)


def number(value):
    return None if value in (None, '', '--', '---') else int(value)


def add(left, right):
    return None if left is None or right is None else left + right


def seconds(value):
    if value in (None, '', '--', '---'):
        return None
    pieces = str(value).split(':')
    if len(pieces) == 1:
        return int(value)
    # Legacy text is MM:SS:00 because the old database stored a SQL TIME.
    if len(pieces) == 3 and int(pieces[2]) != 0:
        raise ValueError(f'Ambiguous legacy duration {value!r}')
    return int(pieces[0]) * 60 + int(pieces[1])


def duration(fight):
    if fight.get('total_fight_time') is not None:
        return int(fight['total_fight_time'])
    elapsed = seconds(fight['finish_time'])
    return None if elapsed is None else (int(fight['finish_round']) - 1) * 300 + elapsed


def raw_count(stats, category, suffix, fight):
    source = 'leg' if category == 'legs' else category
    if category.startswith('round_') and int(category.split('_')[1]) > int(fight['finish_round']):
        return 0
    return number(stats.get(f'{source}_{suffix}'))


def ratio(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def numeric(value):
    if value in (None, '', '--', '---'):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pre_matchup_features(own, opponent, own_fighter, opponent_fighter,
                         own_metadata, opponent_metadata):
    """Build features from history available before the current fight."""
    result = {}
    for category in ('sig_str', 'total_str', 'td', 'head', 'body', 'legs',
                     'distance', 'clinch', 'ground'):
        result[f'{category}_accuracy'] = ratio(
            own[f'{category}_landed'], own[f'{category}_attempted'])
    for field in ('wins', 'losses', 'draws', 'sig_str_landed',
                  'sig_str_attempted', 'total_str_landed',
                  'total_str_attempted', 'td_landed', 'td_attempted',
                  'kd_landed', 'ctrl_time', 'fight_time'):
        result[f'{field}_diff'] = (
            None if own[field] is None or opponent[field] is None
            else own[field] - opponent[field]
        )
    for field, key in (('age', 'age_diff'), ('height', 'height_diff'),
                       ('reach', 'reach_diff')):
        own_value = own_metadata.get(field) if field == 'age' else own_fighter.get(field)
        opponent_value = (opponent_metadata.get(field) if field == 'age'
                          else opponent_fighter.get(field))
        if field != 'age':
            own_value = numeric(own_value)
            opponent_value = numeric(opponent_value)
        result[key] = (None if own_value is None or opponent_value is None
                       else own_value - opponent_value)
    return result


def update_history(state, opponent_before, own_stats, other_stats, fight, result):
    for category in CATEGORIES:
        landed = raw_count(own_stats, category, 'landed', fight)
        attempted = raw_count(own_stats, category, 'attempted', fight)
        absorbed = raw_count(other_stats, category, 'landed', fight)
        other_attempted = raw_count(other_stats, category, 'attempted', fight)
        evaded = None if absorbed is None or other_attempted is None else other_attempted - absorbed
        for suffix, value in zip(('landed', 'attempted', 'absorbed', 'evaded'),
                                 (landed, attempted, absorbed, evaded)):
            key = f'{category}_{suffix}'
            state[key] = add(state[key], value)
    increments = {
        'ctrl_time': seconds(own_stats.get('ctrl_time')),
        'opponent_ctrl_time': seconds(other_stats.get('ctrl_time')),
        'kd_landed': number(own_stats.get('kd_landed')),
        'kd_absorbed': number(other_stats.get('kd_landed')),
        'subs_attempted': number(own_stats.get('subs_attempted')),
        'subs_evaded': number(other_stats.get('subs_attempted')),
        'reversals': number(own_stats.get('reversals')),
        'fight_time': duration(fight),
        # Preserve the existing definition: sum of opponents' prior experience.
        'opponent_fight_time': opponent_before['fight_time'],
        'opponent_wins': opponent_before['wins'],
        'opponent_loses': opponent_before['losses'],
        'championship_fights': int(bool(fight['championship_fight'])),
    }
    for key, value in increments.items():
        state[key] = add(state[key], value)
    if result:
        state[result] += 1


def outcome(fight):
    """Only decision draws increment draws; no contests do not change W/L/D."""
    method = (fight.get('win_method') or '').lower()
    if method in ('overturned', 'could not continue', 'other', 'nc', 'no contest'):
        return None, None
    winner = fight.get('winner')
    if not winner:
        return ('draws', 'draws') if method.startswith('decision') else (None, None)
    winner = normalize_fighter_name(winner)
    red, blue = (normalize_fighter_name(fight[f'{c}_name']) for c in ('r', 'b'))
    if winner == red:
        return 'wins', 'losses'
    if winner == blue:
        return 'losses', 'wins'
    raise ValueError(f"Fight {fight['id']}: winner does not match either fighter")


def build_snapshots(fights, fighters, stats):
    """Return snapshots keyed by (fight ID, corner, pre/post) and audit issues."""
    fighter_by_id = {row['id']: row for row in fighters}
    stats_by_id = {row['id']: row for row in stats}
    name_ids = defaultdict(set)
    for row in fighters:
        name_ids[normalize_fighter_name(row['name'])].add(row['id'])
    states = defaultdict(lambda: dict.fromkeys(COUNT_FIELDS, 0))
    output, issues = {}, []
    seen_bouts = set()
    last_dates = {}
    for day, group in groupby(sorted(fights, key=lambda f: (as_date(f['date']), f['id'])),
                              key=lambda f: as_date(f['date'])):
        pending = []
        for fight in group:
            sides = []
            for corner in ('r', 'b'):
                name = fight[f'{corner}_name']
                fighter_id = fight[f'{corner}_fighter_id']
                if fighter_id is not None and fighter_id not in fighter_by_id:
                    raise ValueError(f"Fight {fight['id']}: dangling fighter ID {fighter_id}")
                key = ('id', fighter_id)
                if fighter_id is None:
                    candidates = name_ids[normalize_fighter_name(name)]
                    if len(candidates) > 1:
                        raise ValueError(f"Fight {fight['id']}: ambiguous missing fighter identity: {name}")
                    if len(candidates) == 1:
                        fighter_id = next(iter(candidates))
                        key = ('id', fighter_id)
                    else:
                        key = ('name', normalize_fighter_name(name))
                    issues.append({'fight_id': fight['id'], 'corner': corner,
                                   'reason': 'missing_fighter_id', 'name': name,
                                   'resolved_fighter_id': fighter_id})
                if fight[f'{corner}_fight_stats_id'] not in stats_by_id:
                    raise ValueError(f"Fight {fight['id']}: missing raw stats for {corner}")
                fighter = fighter_by_id.get(fighter_id, {})
                dob = fighter.get('date_of_birth')
                before = states[key].copy()
                metadata = dict(name=name, age=(day - as_date(dob)).days if dob else None,
                                fighter_id=fighter_id, fight_id=fight['id'])
                output[(fight['id'], corner, 'pre')] = dict(before, **metadata)
                sides.append((key, before, metadata,
                              stats_by_id[fight[f'{corner}_fight_stats_id']], fighter))
            bout_key = (day, frozenset(side[0] for side in sides))
            if len(bout_key[1]) != 2 or bout_key in seen_bouts:
                raise ValueError(f"Fight {fight['id']}: duplicate/invalid matchup")
            seen_bouts.add(bout_key)
            derived = []
            for index in range(2):
                own = sides[index]
                opponent = sides[1 - index]
                matchup = pre_matchup_features(
                    own[1], opponent[1], own[4], opponent[4], own[2], opponent[2]
                )
                prior_date = last_dates.get(own[0])
                matchup['days_since_last_fight'] = (
                    None if prior_date is None else (day - prior_date).days
                )
                matchup['is_debut'] = 1 if prior_date is None else 0
                derived.append(matchup)
            pending.append((fight, sides, derived))
        # Freeze every pre-fight snapshot before incorporating this day's results.
        for fight, sides, derived in pending:
            for index, (corner, result) in enumerate(zip(('r', 'b'), outcome(fight))):
                key, before, metadata, own_stats, _ = sides[index]
                _, opponent_before, _, other_stats, _ = sides[1 - index]
                update_history(states[key], opponent_before, own_stats, other_stats, fight, result)
                output[(fight['id'], corner, 'post')] = dict(
                    states[key], **metadata, **derived[index]
                )
                last_dates[key] = day
    return output, issues


def latest_post_snapshot_id(fighter_id, before=None):
    """Find history by event date, not insertion ID; exclude the prediction date."""
    from contextlib import closing
    try:
        from .combine_tables import connect_database
    except ImportError:
        from combine_tables import connect_database
    with closing(connect_database()) as conn, conn, conn.cursor() as cursor:
        cursor.execute('''SELECT t.id FROM total_fight_stats AS t
            JOIN fights AS f ON f.id = t.fight_id
            WHERE t.fighter_id = %s AND t.snapshot_type = 'post' AND f.date < %s
            ORDER BY f.date DESC, f.id DESC LIMIT 1''', (fighter_id, before or date.today()))
        row = cursor.fetchone()
        return row[0] if row else None
