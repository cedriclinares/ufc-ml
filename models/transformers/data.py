"""Date-frozen raw-data reconstruction shared by training and inference."""

from collections import defaultdict
from dataclasses import dataclass
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from models.comparisons.features import BASE_FEATURES, RATIOS
from models.rounds.duration import duration_features, elapsed_seconds
from scripts.fight_history import build_snapshots, outcome, seconds, raw_count
from scripts.combine_tables import normalize_fighter_name

ROOT = Path(__file__).resolve().parents[2]
SUMMARY_FIELDS = [
    c[2:] for c in BASE_FEATURES if c.startswith("r_") and c != "r_stance"
]
ELO_FIELDS = [
    "elo_16",
    "elo_32",
    "elo_48",
    "experience",
    "win_rate",
    "win_streak",
    "loss_streak",
]
CATEGORIES = [
    "sig_str",
    "head",
    "body",
    "legs",
    "distance",
    "clinch",
    "ground",
    "total_str",
    "td",
]
PER_FIGHT_FIELDS = [
    f"{c}_{s}"
    for c in CATEGORIES
    for s in ("landed", "attempted", "absorbed", "evaded")
]
PER_FIGHT_FIELDS += [
    "kd_landed",
    "kd_absorbed",
    "subs_attempted",
    "subs_evaded",
    "ctrl_time",
    "opponent_ctrl_time",
    "reversals",
]
HISTORY_FIELDS = PER_FIGHT_FIELDS + [f"{c}_per_minute" for c in PER_FIGHT_FIELDS]
HISTORY_FIELDS += [
    "elapsed_rounds",
    "scheduled_rounds",
    "opponent_elo_16",
    "opponent_elo_32",
    "opponent_elo_48",
    "won",
    "lost",
    "draw",
    "days_before_matchup",
]


def decimal_price(value):
    try:
        value = float(str(value).replace("−", "-"))
    except (TypeError, ValueError):
        return np.nan
    if not np.isfinite(value) or abs(value) < 100 or value != int(value):
        return np.nan
    return 1 + (value / 100 if value > 0 else 100 / abs(value))


def records(frame):
    """None, not float NaN, is the existing reconstruction API's missing value."""
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


def ending(fight):
    scheduled, finish = fight.get("number_of_rounds"), fight.get("finish_round")
    elapsed = elapsed_seconds(fight.get("finish_time"))
    if (
        scheduled not in (3, 5)
        or finish is None
        or not 1 <= finish <= scheduled
        or finish != int(finish)
        or not np.isfinite(elapsed)
        or not 0 <= elapsed <= 300
    ):
        return np.nan, np.nan
    return np.ceil(2 * (finish - 1 + elapsed / 300)) / 2, float(
        finish == scheduled and elapsed == 300
    )


def number(value):
    try:
        x = float(value)
        return x if np.isfinite(x) else np.nan
    except (ValueError, TypeError):
        return np.nan


@dataclass
class FightData:
    rows: pd.DataFrame
    summary: np.ndarray  # N, 2, features
    stance: np.ndarray  # N, 2; raw categories
    history: np.ndarray  # N, 2, last ten, features
    method: np.ndarray  # N, 2, last ten; raw categories
    padding: np.ndarray  # True means padding
    context: np.ndarray  # weight, scheduled rounds, championship
    gender: np.ndarray
    market: np.ndarray  # normalized implied probabilities
    summary_fields: list
    audit: dict

    def take(self, indices):
        indices = np.asarray(indices, dtype=int)
        return FightData(
            self.rows.iloc[indices].reset_index(drop=True),
            *[
                getattr(self, name)[indices]
                for name in (
                    "summary",
                    "stance",
                    "history",
                    "method",
                    "padding",
                    "context",
                    "gender",
                    "market",
                )
            ],
            self.summary_fields,
            self.audit,
        )

    def __len__(self):
        return len(self.rows)


def build_data(
    fights, fighters, stats, upcoming=None, history_length=10, upcoming_only=False
):
    """Reconstruct all pre-fight summaries and sequences without persisted snapshots.

    Upcoming records have fighter IDs and matchup metadata, but no results. The
    caller must restrict raw history to dates before its prediction date.
    """
    fights = fights.copy()
    fights["date"] = pd.to_datetime(fights.date, errors="raise")
    fights = fights.sort_values(["date", "id"], kind="stable")
    if (
        fights.id.duplicated().any()
        or stats.id.duplicated().any()
        or fighters.id.duplicated().any()
    ):
        raise ValueError("Duplicate fight, stats, or fighter IDs")
    raw = records(fights)
    for fight in raw:
        fight["date"] = fight["date"].date()
    fighter_rows = records(fighters)
    stat_rows = records(stats)
    snapshot_rows = []
    for fight in raw:
        safe = dict(fight)
        finish = number(fight.get("finish_round"))
        elapsed = elapsed_seconds(fight.get("finish_time"))
        valid_round = np.isfinite(finish) and 1 <= finish <= 5 and finish == int(finish)
        if not valid_round:
            # Unknown ending: preserve measured round counts rather than invent
            # zeros for rounds that might have been fought. Original targets stay invalid.
            safe["finish_round"] = 5
        total = number(fight.get("total_fight_time"))
        if not np.isfinite(total) or total < 0:
            total = (
                (finish - 1) * 300 + elapsed
                if valid_round and 0 <= elapsed <= 300
                else np.nan
            )
        safe["total_fight_time"] = int(total) if np.isfinite(total) else None
        if not np.isfinite(elapsed) or safe["total_fight_time"] is None:
            safe["finish_time"] = None
        snapshot_rows.append(safe)
    snapshots, issues = build_snapshots(snapshot_rows, fighter_rows, stat_rows)
    by_fighter = {r["id"]: r for r in fighter_rows}
    by_stats = {r["id"]: r for r in stat_rows}
    ownership_issues = []
    for fight in raw:
        for c in ("r", "b"):
            stat = by_stats[fight[f"{c}_fight_stats_id"]]
            expected = fight[f"{c}_fighter_id"]
            if (
                expected is not None
                and stat.get("fighter_id") is not None
                and expected != stat["fighter_id"]
            ):
                ownership_issues.append(
                    dict(
                        fight_id=int(fight["id"]),
                        corner=c,
                        stats_id=int(stat["id"]),
                        fight_fighter_id=int(expected),
                        stats_fighter_id=int(stat["fighter_id"]),
                    )
                )
    # Resolve IDs using the exact same reconstruction lookup, without mutating CSVs.
    for fight in raw:
        for c in ("r", "b"):
            fight[f"{c}_fighter_id"] = snapshots[(fight["id"], c, "pre")]["fighter_id"]
    outcomes = {fight["id"]: outcome(fight) for fight in raw}
    normalized = pd.DataFrame(raw, columns=fights.columns).rename(
        columns={"r_fighter_id": "r_id", "b_fighter_id": "b_id"}
    )
    normalized["date"] = pd.to_datetime(normalized.date)
    future = upcoming or []
    targets = (
        pd.DataFrame(future)
        if upcoming_only
        else pd.concat([normalized, pd.DataFrame(future)], ignore_index=True)
    )
    if targets.empty:
        raise ValueError("No matchups supplied for feature construction")
    targets["date"] = pd.to_datetime(targets.date)
    targets = targets.sort_values(["date", "id"], kind="stable").reset_index(drop=True)
    duration_x = duration_features(targets, normalized)
    duration_fields = [c[2:] for c in duration_x if c.startswith("r_")]
    fields = (
        SUMMARY_FIELDS
        + RATIOS
        + ["days_since_last_fight", "is_debut"]
        + ELO_FIELDS
        + duration_fields
    )
    raw_by_date = {day: group for day, group in normalized.groupby("date")}
    target_by_date = {day: group for day, group in targets.groupby("date")}
    elo = defaultdict(lambda: {k: 1500.0 for k in (16, 32, 48)})
    experience = defaultdict(
        lambda: [0, 0, 0, 0]
    )  # wins, bouts, win streak, loss streak
    history = defaultdict(list)
    last = {}
    latest = {}
    (
        result_rows,
        summaries,
        stances,
        sequences,
        methods,
        paddings,
        contexts,
        genders,
        markets,
    ) = ([] for _ in range(9))
    excluded = []
    for day in sorted(set(raw_by_date) | set(target_by_date)):
        group = target_by_date.get(day, targets.iloc[:0])
        for idx, row in group.iterrows():
            ids = [row.get("r_id"), row.get("b_id")]
            if any(pd.isna(fid) or fid not in by_fighter for fid in ids):
                excluded.append(
                    {"fight_id": int(row.id), "reason": "unresolved fighter identity"}
                )
                continue
            is_future = (
                bool(row.get("_upcoming", False))
                if pd.notna(row.get("_upcoming"))
                else False
            )
            if row.number_of_rounds not in (3, 5):
                excluded.append(
                    {"fight_id": int(row.id), "reason": "unsupported scheduled length"}
                )
                continue
            side_summary, side_stance, side_history, side_methods, side_padding = (
                [],
                [],
                [],
                [],
                [],
            )
            for c, fid in zip(("r", "b"), ids):
                info = by_fighter[fid]
                if is_future:
                    snapshot = latest.get(fid, dict.fromkeys(SUMMARY_FIELDS, 0))
                else:
                    snapshot = snapshots[(row.id, c, "pre")]
                dob = pd.to_datetime(info.get("date_of_birth"), errors="coerce")
                values = {f: number(snapshot.get(f)) for f in SUMMARY_FIELDS}
                values.update({f: number(info.get(f)) for f in ("height", "reach")})
                values["age"] = (day - dob).days if pd.notna(dob) else np.nan
                for f in RATIOS:
                    base = f.removesuffix("_accuracy")
                    den = values[f"{base}_attempted"]
                    values[f] = (
                        values[f"{base}_landed"] / den
                        if den and np.isfinite(den)
                        else np.nan
                    )
                values["days_since_last_fight"] = (
                    (day - last[fid]).days if fid in last else np.nan
                )
                values["is_debut"] = float(fid not in last)
                wins, bouts, ws, ls = experience[fid]
                values.update({f"elo_{k}": elo[fid][k] for k in (16, 32, 48)})
                values.update(
                    experience=bouts,
                    win_rate=wins / bouts if bouts else np.nan,
                    win_streak=ws,
                    loss_streak=ls,
                )
                values.update(
                    {f: duration_x.loc[idx, c + "_" + f] for f in duration_fields}
                )
                side_summary.append([values[f] for f in fields])
                side_stance.append(str(info.get("stance") or "Unknown"))
                seq = np.full((history_length, len(HISTORY_FIELDS)), np.nan)
                cat = np.full(history_length, "Unknown", dtype=object)
                pad = np.ones(history_length, dtype=bool)
                for j, (previous_day, token, method) in enumerate(
                    history[fid][-history_length:]
                ):
                    seq[j] = token
                    seq[j, -1] = (day - previous_day).days
                    cat[j], pad[j] = method, False
                side_history.append(seq)
                side_methods.append(cat)
                side_padding.append(pad)
            rounds, distance = ending(row)
            winner = np.nan
            if not is_future:
                result = outcomes[row.id]
                winner = (
                    1.0
                    if result[0] == "wins"
                    else 0.0 if result[1] == "wins" else np.nan
                )
            decimals = np.array(
                [decimal_price(row.get(f"{c}_fighter_odds")) for c in ("r", "b")]
            )
            market = 1 / decimals
            market = (
                market / market.sum()
                if np.isfinite(market).all()
                else np.full(2, np.nan)
            )
            result_rows.append(
                dict(
                    id=int(row.id),
                    date=day,
                    r_id=ids[0],
                    b_id=ids[1],
                    r_name=row.r_name,
                    b_name=row.b_name,
                    number_of_rounds=int(row.number_of_rounds),
                    winner_target=winner,
                    rounds_target=rounds,
                    distance_target=distance,
                    odds_available=bool(np.isfinite(market).all()),
                    upcoming=is_future,
                )
            )
            summaries.append(side_summary)
            stances.append(side_stance)
            sequences.append(side_history)
            methods.append(side_methods)
            paddings.append(side_padding)
            contexts.append(
                [
                    number(row.get("weight")),
                    row.number_of_rounds,
                    float(bool(row.get("championship_fight", False))),
                ]
            )
            genders.append(str(by_fighter[ids[0]].get("gender") or "Unknown"))
            markets.append(market)
        # All result-dependent values remain frozen until every target on this day was emitted.
        pending = []
        if day in raw_by_date:
            for fight in records(raw_by_date[day]):
                results = outcome(fight)
                ids = [fight.get("r_id"), fight.get("b_id")]
                if any(fid is None for fid in ids):
                    continue
                elapsed = number(fight.get("total_fight_time"))
                for i, c in enumerate(("r", "b")):
                    fid, opponent = ids[i], ids[1 - i]
                    own = by_stats[fight[f"{c}_fight_stats_id"]]
                    other_c = "b" if c == "r" else "r"
                    other = by_stats[fight[f"{other_c}_fight_stats_id"]]
                    measurements = {}
                    for cat in CATEGORIES:
                        landed, attempted = (
                            raw_count(own, cat, s, fight)
                            for s in ("landed", "attempted")
                        )
                        absorbed, other_attempted = (
                            raw_count(other, cat, s, fight)
                            for s in ("landed", "attempted")
                        )
                        evaded = (
                            other_attempted - absorbed
                            if other_attempted is not None and absorbed is not None
                            else None
                        )
                        measurements.update(
                            dict(
                                zip(
                                    [
                                        cat + "_" + s
                                        for s in (
                                            "landed",
                                            "attempted",
                                            "absorbed",
                                            "evaded",
                                        )
                                    ],
                                    (landed, attempted, absorbed, evaded),
                                )
                            )
                        )
                    measurements.update(
                        kd_landed=own.get("kd_landed"),
                        kd_absorbed=other.get("kd_landed"),
                        subs_attempted=own.get("subs_attempted"),
                        subs_evaded=other.get("subs_attempted"),
                        ctrl_time=seconds(own.get("ctrl_time")),
                        opponent_ctrl_time=seconds(other.get("ctrl_time")),
                        reversals=own.get("reversals"),
                    )
                    token = [number(measurements[f]) for f in PER_FIGHT_FIELDS]
                    rates = [
                        v / (elapsed / 60) if elapsed > 0 else np.nan for v in token
                    ]
                    token += rates + [elapsed / 300, fight["number_of_rounds"]]
                    token += [elo[opponent][k] for k in (16, 32, 48)]
                    token += [
                        float(results[i] == "wins"),
                        float(results[i] == "losses"),
                        float(results[i] == "draws"),
                        0.0,
                    ]
                    pending.append(
                        (
                            fid,
                            opponent,
                            token,
                            str(fight.get("win_method") or "Unknown"),
                            results[i],
                            fight,
                        )
                    )
        # Sum event-date Elo deltas against date-start ratings, even if a fighter appears twice.
        deltas = defaultdict(lambda: defaultdict(float))
        for fid, opponent, token, method, result, fight in pending:
            history[fid].append((day, token, method))
            last[fid] = day
            c = "r" if fight["r_id"] == fid else "b"
            latest[fid] = snapshots[(fight["id"], c, "post")]
            e = experience[fid]
            e[1] += 1
            if result == "wins":
                e[0] += 1
                e[2] += 1
                e[3] = 0
            elif result == "losses":
                e[3] += 1
                e[2] = 0
            if result is not None:
                score = 1.0 if result == "wins" else 0.0 if result == "losses" else 0.5
                for k in (16, 32, 48):
                    expected = 1 / (1 + 10 ** ((elo[opponent][k] - elo[fid][k]) / 400))
                    deltas[fid][k] += k * (score - expected)
        for fid, delta in deltas.items():
            for k, change in delta.items():
                elo[fid][k] += change
    if not result_rows:
        raise ValueError("No usable matchups after identity and schedule checks")
    return FightData(
        pd.DataFrame(result_rows),
        np.asarray(summaries, dtype=np.float32),
        np.asarray(stances),
        np.asarray(sequences, dtype=np.float32),
        np.asarray(methods),
        np.asarray(paddings),
        np.asarray(contexts, dtype=np.float32),
        np.asarray(genders),
        np.asarray(markets, dtype=np.float32),
        fields,
        dict(
            raw_bouts=len(fights),
            usable_rows=len(result_rows),
            exclusions=excluded,
            identity_issues=issues,
            history_length=history_length,
            stats_ownership_issues=ownership_issues,
            stats_join_note="Referenced stats IDs define bout sides; ownership discrepancies are audited, not used to reassign fighters.",
            metadata_note="Height/reach/stance/gender are current static records, not time-versioned.",
        ),
    )


def load_data(
    fights=ROOT / "fights.csv",
    fighters=ROOT / "fighters.csv",
    stats=ROOT / "fight_stats.csv",
    before=None,
):
    raw = pd.read_csv(fights)
    if before is not None:
        cutoff = pd.Timestamp(before).normalize()
        if pd.isna(cutoff):
            raise ValueError("Invalid exclusive prediction-date cutoff")
        raw = raw[pd.to_datetime(raw.date) < cutoff]
    if raw.empty:
        raise ValueError("No historical fights available before the requested cutoff")
    data = build_data(raw, pd.read_csv(fighters), pd.read_csv(stats))
    data.audit["fingerprints"] = {
        str(Path(p).name): hashlib.sha256(Path(p).read_bytes()).hexdigest()
        for p in (fights, fighters, stats)
    }
    return data
