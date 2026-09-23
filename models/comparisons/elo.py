"""Chronological ELO ratings and streak features for UFC fighters.

All features are pre-fight: each row's values use only fight outcomes
that occurred before that row's date. No leakage by construction.
"""
import pandas as pd


def compute_elo(df, k=32, initial=1500):
    """Compute pre-fight ELO ratings for both corners.

    Args:
        df: DataFrame sorted by date with columns r_id, b_id, winner.
        k: ELO K-factor (rating sensitivity).
        initial: Starting rating for new fighters.

    Returns:
        DataFrame with columns r_elo, b_elo, elo_diff (same index as df).
    """
    ratings = {}
    rows = []
    for _, fight in df.iterrows():
        r_id, b_id = fight['r_id'], fight['b_id']
        r_elo = ratings.get(r_id, initial)
        b_elo = ratings.get(b_id, initial)
        rows.append({'r_elo': r_elo, 'b_elo': b_elo, 'elo_diff': r_elo - b_elo})

        expected_r = 1 / (1 + 10 ** ((b_elo - r_elo) / 400))
        actual_r = 1.0 if fight['winner'] == 'Red' else 0.0
        ratings[r_id] = r_elo + k * (actual_r - expected_r)
        ratings[b_id] = b_elo + k * ((1 - actual_r) - (1 - expected_r))

    return pd.DataFrame(rows, index=df.index)


def compute_streaks(df):
    """Compute pre-fight win/loss streaks for both corners.

    Tracks streaks by fighter ID across both corners. A win resets the
    loss streak and increments the win streak, and vice versa.

    Returns:
        DataFrame with columns r_win_streak, r_loss_streak,
        b_win_streak, b_loss_streak, win_streak_diff (same index as df).
    """
    fighter_streaks = {}  # fighter_id -> (win_streak, loss_streak)
    rows = []
    for _, fight in df.iterrows():
        r_id, b_id = fight['r_id'], fight['b_id']
        r_w, r_l = fighter_streaks.get(r_id, (0, 0))
        b_w, b_l = fighter_streaks.get(b_id, (0, 0))
        rows.append({
            'r_win_streak': r_w, 'r_loss_streak': r_l,
            'b_win_streak': b_w, 'b_loss_streak': b_l,
            'win_streak_diff': r_w - b_w,
        })

        if fight['winner'] == 'Red':
            fighter_streaks[r_id] = (r_w + 1, 0)
            fighter_streaks[b_id] = (0, b_l + 1)
        else:
            fighter_streaks[r_id] = (0, r_l + 1)
            fighter_streaks[b_id] = (b_w + 1, 0)

    return pd.DataFrame(rows, index=df.index)
