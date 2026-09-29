"""Predict upcoming fights using the walk-forward trained model.

Usage:
    python scripts/predict_upcoming.py fights.json
    python scripts/fetch_upcoming.py URL | python scripts/predict_upcoming.py -

Reads JSON lines (from fetch_upcoming.py) with red_name, blue_name,
red_decimal, blue_decimal. Trains gb_med_multielo_odds on all historical
data, then predicts each upcoming fight and recommends Kelly-filtered bets.
"""

import argparse
import json
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

# Add project root to path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from models.comparisons.elo import compute_multi_elo, compute_streaks, compute_experience
from models.comparisons.evaluation import decimal_odds, make_features, pipeline


def build_features(df, decimal):
    """Build feature matrix matching gb_med_multielo_odds config."""
    x = make_features(df)
    elo = compute_multi_elo(df, k_values=(16, 32, 48))
    streaks = compute_streaks(df)
    experience = compute_experience(df)
    x = pd.concat([x, elo, streaks, experience], axis=1)
    implied = 1 / decimal
    market = implied / implied.sum(axis=1, keepdims=True)
    odds_df = pd.DataFrame({
        'r_implied_prob': market[:, 0],
        'b_implied_prob': market[:, 1],
    }, index=x.index)
    x = pd.concat([x, odds_df], axis=1)
    return x


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def fuzzy_match_fighter(name, df):
    """Find fighter ID by matching name against r_name and b_name columns."""
    # Try exact match first
    r_match = df[df['r_name'] == name]
    if len(r_match) > 0:
        return r_match.iloc[-1]['r_id'], name
    b_match = df[df['b_name'] == name]
    if len(b_match) > 0:
        return b_match.iloc[-1]['b_id'], name

    # Try accent-stripped exact match
    name_stripped = strip_accents(name).lower()
    for col, id_col in [('r_name', 'r_id'), ('b_name', 'b_id')]:
        matches = df[df[col].apply(lambda x: strip_accents(str(x)).lower()) == name_stripped]
        if len(matches) > 0:
            return matches.iloc[-1][id_col], matches.iloc[-1][col]

    # Try case-insensitive contains
    name_lower = name.lower()
    for col, id_col in [('r_name', 'r_id'), ('b_name', 'b_id')]:
        matches = df[df[col].str.lower().str.contains(name_lower, na=False)]
        if len(matches) > 0:
            return matches.iloc[-1][id_col], matches.iloc[-1][col]

    # Try accent-stripped contains
    for col, id_col in [('r_name', 'r_id'), ('b_name', 'b_id')]:
        matches = df[df[col].apply(lambda x: strip_accents(str(x)).lower()).str.contains(name_stripped, na=False)]
        if len(matches) > 0:
            return matches.iloc[-1][id_col], matches.iloc[-1][col]

    # Try last name only (accent-stripped)
    last_name = strip_accents(name.split()[-1]).lower()
    for col, id_col in [('r_name', 'r_id'), ('b_name', 'b_id')]:
        matches = df[df[col].apply(lambda x: strip_accents(str(x)).lower()).str.contains(last_name, na=False)]
        if len(matches) == 1:
            return matches.iloc[-1][id_col], matches.iloc[-1][col]

    return None, name


def get_latest_stats(fighter_id, df):
    """Get the most recent row of stats for a fighter."""
    r_fights = df[df['r_id'] == fighter_id]
    b_fights = df[df['b_id'] == fighter_id]

    latest_r = r_fights.iloc[-1] if len(r_fights) > 0 else None
    latest_b = b_fights.iloc[-1] if len(b_fights) > 0 else None

    if latest_r is not None and latest_b is not None:
        if latest_r['date'] >= latest_b['date']:
            return latest_r, 'r'
        return latest_b, 'b'
    if latest_r is not None:
        return latest_r, 'r'
    if latest_b is not None:
        return latest_b, 'b'
    return None, None


def build_upcoming_row(red_id, blue_id, red_decimal, blue_decimal, weight, df):
    """Build a single-row DataFrame matching the training features."""
    # Get latest stats for each fighter
    r_row, r_corner = get_latest_stats(red_id, df)
    b_row, b_corner = get_latest_stats(blue_id, df)

    if r_row is None or b_row is None:
        return None

    row = {}
    row['weight'] = weight
    row['r_fighter_odds'] = None  # Not used for features directly
    row['b_fighter_odds'] = None
    row['date'] = pd.Timestamp.now()
    row['winner'] = 'Red'  # Placeholder, not used for prediction
    row['gender'] = r_row.get('gender', 'Male')
    row['r_id'] = red_id
    row['b_id'] = blue_id
    row['r_name'] = ''
    row['b_name'] = ''

    # Copy fighter stats from their latest appearance
    for prefix in ['r_']:
        src = r_row
        src_prefix = r_corner + '_'
        for col in df.columns:
            if col.startswith(prefix) and col not in ['r_id', 'r_name', 'r_fighter_odds']:
                src_col = col.replace('r_', src_prefix, 1)
                if src_col in src.index:
                    row[col] = src[src_col]

    for prefix in ['b_']:
        src = b_row
        src_prefix = b_corner + '_'
        for col in df.columns:
            if col.startswith(prefix) and col not in ['b_id', 'b_name', 'b_fighter_odds']:
                src_col = col.replace('b_', src_prefix, 1)
                if src_col in src.index:
                    row[col] = src[src_col]

    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("fights", help="JSON lines file from fetch_upcoming.py, or - for stdin")
    parser.add_argument("--data", type=Path, default=ROOT / "final-dataset-names-and-ids.csv")
    parser.add_argument("--ev-threshold", type=float, default=0.4,
                        help="EV threshold for betting (default: 0.4, from CV)")
    parser.add_argument("--kelly-fraction", type=float, default=3.0,
                        help="Kelly fraction multiplier (default: 3.0, moderate)")
    args = parser.parse_args()

    # Load upcoming fights
    if args.fights == "-":
        lines = sys.stdin.readlines()
    else:
        lines = Path(args.fights).read_text().strip().split("\n")
    upcoming = [json.loads(line) for line in lines if line.strip()]
    print(f"Loaded {len(upcoming)} upcoming fights\n")

    # Load historical data
    df = pd.read_csv(args.data)
    df['date'] = pd.to_datetime(df['date'], errors='raise')
    df = df.sort_values('date', kind='stable').reset_index(drop=True)
    y = df.winner.map({'Red': 0, 'Blue': 1}).astype(int)
    decimal = decimal_odds(df)

    # Build features on full historical data
    x = build_features(df, decimal)

    # Train model on ALL historical data
    estimator = HistGradientBoostingClassifier(
        max_iter=300, max_leaf_nodes=31, learning_rate=0.05,
        l2_regularization=2, min_samples_leaf=20,
        early_stopping=False, random_state=0,
    )
    model = pipeline(estimator, x.columns)
    print("Training model on all historical data...")
    model.fit(x, y)
    print(f"Trained on {len(x)} fights\n")

    # Match fighters and predict
    print(f"{'Fight':<45} {'Pred':>5} {'Red%':>6} {'Blue%':>6} │ {'EV_Red':>7} {'EV_Blue':>8} │ {'Bet':>5} {'Size':>6}")
    print("─" * 105)

    total_bets = 0
    total_wagered = 0.0

    for fight in upcoming:
        red_name = fight['red_name']
        blue_name = fight['blue_name']
        r_dec = fight.get('red_decimal')
        b_dec = fight.get('blue_decimal')

        if not r_dec or not b_dec:
            print(f"{red_name} vs {blue_name:<25} │ No odds available")
            continue

        # Find fighters in historical data
        r_id, r_matched = fuzzy_match_fighter(red_name, df)
        b_id, b_matched = fuzzy_match_fighter(blue_name, df)

        if r_id is None:
            print(f"{red_name} vs {blue_name:<25} │ Red fighter not found in data")
            continue
        if b_id is None:
            print(f"{red_name} vs {blue_name:<25} │ Blue fighter not found in data")
            continue

        # Build feature row for this fight
        row_data = build_upcoming_row(r_id, b_id, r_dec, b_dec, fight['weight_class'], df)
        if row_data is None:
            print(f"{red_name} vs {blue_name:<25} │ Insufficient data")
            continue

        # Create a temporary df to compute ELO etc.
        # Append the upcoming fight to the dataset so ELO/streaks carry forward
        temp_df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)
        temp_decimal = np.vstack([decimal, [[r_dec, b_dec]]])
        temp_x = build_features(temp_df, temp_decimal)

        # Predict using the last row only
        fight_x = temp_x.iloc[[-1]]
        proba = model.predict_proba(fight_x)[0]

        # EV calculation
        ev_red = proba[0] * r_dec - 1
        ev_blue = proba[1] * b_dec - 1
        best_side = 0 if ev_red > ev_blue else 1
        best_ev = max(ev_red, ev_blue)
        best_odds = r_dec if best_side == 0 else b_dec
        best_prob = proba[best_side]

        # Kelly sizing
        kelly_frac = (best_prob * best_odds - 1) / (best_odds - 1)
        bet_size = max(0, args.kelly_fraction * kelly_frac)

        # Prediction label
        pred_label = "Red" if proba[0] > proba[1] else "Blue"
        bet_label = "Red" if best_side == 0 else "Blue"

        fight_label = f"{red_name} vs {blue_name}"
        if len(fight_label) > 44:
            fight_label = fight_label[:41] + "..."

        if best_ev >= args.ev_threshold and bet_size > 0:
            print(f"{fight_label:<45} {pred_label:>5} {proba[0]:>5.1%} {proba[1]:>5.1%} │ "
                  f"{ev_red:>+6.3f} {ev_blue:>+7.3f} │ {bet_label:>5} {bet_size:>5.2f}u")
            total_bets += 1
            total_wagered += bet_size
        else:
            print(f"{fight_label:<45} {pred_label:>5} {proba[0]:>5.1%} {proba[1]:>5.1%} │ "
                  f"{ev_red:>+6.3f} {ev_blue:>+7.3f} │  skip")

    print("─" * 105)
    print(f"Bets: {total_bets} | Total wagered: {total_wagered:.2f}u | "
          f"EV threshold: {args.ev_threshold} | Kelly fraction: {args.kelly_fraction}x")


if __name__ == "__main__":
    main()
