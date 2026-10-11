"""Print winner, half-round, distance, and priced value predictions from a checkpoint."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from models.transformers.data import build_data, decimal_price
from models.transformers.training import load_checkpoint, predict, device_for
from scripts.combine_tables import normalize_fighter_name
from scripts.predict_value_bets import load_card, weight_in_pounds


def price_result(probability, odds):
    if not np.isfinite(probability) or not 0 <= probability <= 1:
        raise ValueError("Bet probability must be finite and between zero and one")
    decimal = decimal_price(odds)
    if not np.isfinite(decimal):
        return dict(odds=None, ev=None, units_20=None, units_40=None)
    ev = float(probability * decimal - 1)
    units = max(0.0, 3 * ev / (decimal - 1))
    return dict(
        odds=int(float(str(odds).replace("−", "-"))),
        ev=ev,
        units_20=units if ev >= 0.2 else 0.0,
        units_40=units if ev >= 0.4 else 0.0,
    )


def total_probabilities(distribution, line, scheduled):
    line = float(line)
    if (
        not np.isfinite(line)
        or line * 2 != int(line * 2)
        or not 0.5 <= line < scheduled
    ):
        raise ValueError(
            "total_line must be a half-round increment between 0.5 and the scheduled length"
        )
    p = np.asarray(distribution)
    if (
        p.shape != (11,)
        or not np.isfinite(p).all()
        or (p < 0).any()
        or not np.isclose(p.sum(), 1)
    ):
        raise ValueError("Invalid duration probabilities")
    if (p[np.arange(11) / 2 > scheduled] > 1e-8).any():
        raise ValueError("Duration probabilities exceed the scheduled length")
    return float(p[np.arange(11) / 2 > line].sum()), float(
        p[np.arange(11) / 2 <= line].sum()
    )


def predict_card(
    card,
    day,
    checkpoint,
    fights_path=ROOT / "fights.csv",
    fighters_path=ROOT / "fighters.csv",
    stats_path=ROOT / "fight_stats.csv",
    device="auto",
):
    day = pd.Timestamp(day).normalize()
    if pd.isna(day):
        raise ValueError("A valid event date is required")
    device = device_for(device)
    model, prep, temperatures, metadata = load_checkpoint(checkpoint, device)
    if pd.Timestamp(metadata["train_through"]) >= day:
        raise ValueError(
            "Checkpoint includes outcomes on/after this event date; train an earlier checkpoint"
        )
    fighters = pd.read_csv(fighters_path)
    fighters["_key"] = fighters.name.map(lambda n: normalize_fighter_name(n).casefold())
    history = pd.read_csv(fights_path)
    history = history[pd.to_datetime(history.date) < day]
    prepared, results, seen = [], [], set()
    for index, fight in enumerate(card):
        display = f"{fight.get('red_name','?')} vs {fight.get('blue_name','?')}"
        try:
            ids = []
            for side in ("red", "blue"):
                key = normalize_fighter_name(fight[side + "_name"]).casefold()
                matched = fighters[fighters._key.eq(key)]
                if len(matched) != 1:
                    raise ValueError(
                        f'Missing or ambiguous fighter: {fight[side+"_name"]}'
                    )
                ids.append(int(matched.iloc[0].id))
            key = frozenset(ids)
            if len(key) != 2 or key in seen:
                raise ValueError("Duplicate matchup or identical fighters")
            seen.add(key)
            scheduled = fight.get("number_of_rounds")
            if scheduled not in (3, 5):
                raise ValueError("number_of_rounds must be supplied as 3 or 5")
            if "championship_fight" in fight and not isinstance(
                fight["championship_fight"], bool
            ):
                raise ValueError("championship_fight must be a JSON boolean")
            if fight.get("total_line") is not None:
                total_probabilities(np.eye(11)[0], fight["total_line"], scheduled)
            if model.config.odds_mode == "market" and any(
                not np.isfinite(decimal_price(fight.get(s + "_odds")))
                for s in ("red", "blue")
            ):
                raise ValueError(
                    "Odds-assisted checkpoint requires both moneyline prices"
                )
            row = dict(
                id=-index - 1,
                date=day,
                r_id=ids[0],
                b_id=ids[1],
                r_name=fight["red_name"],
                b_name=fight["blue_name"],
                number_of_rounds=scheduled,
                weight=weight_in_pounds(fight["weight_class"]),
                championship_fight=bool(fight.get("championship_fight", False)),
                r_fighter_odds=fight.get("red_odds"),
                b_fighter_odds=fight.get("blue_odds"),
                _upcoming=True,
            )
            prepared.append(row)
            results.append(dict(fight=display, status="OK", id=row["id"]))
        except (ValueError, KeyError, TypeError) as error:
            results.append(dict(fight=display, status=str(error), id=-index - 1))
    if prepared:
        data = build_data(
            history,
            fighters.drop(columns="_key"),
            pd.read_csv(stats_path),
            prepared,
            upcoming_only=True,
        )
        data = data.take(np.flatnonzero(data.rows.upcoming))
        p = predict(model, prep, temperatures, data, device)
        lookup = {int(fid): i for i, fid in enumerate(data.rows.id)}
        for index, (fight, result) in enumerate(zip(card, results)):
            if result["status"] != "OK":
                continue
            i = lookup[result["id"]]
            red = float(p["winner"][i])
            duration = p["rounds"][i]
            result.update(
                red_probability=red,
                blue_probability=1 - red,
                winner=fight["red_name"] if red >= 0.5 else fight["blue_name"],
                predicted_rounds=float(duration.argmax() / 2),
                distance_probability=float(p["distance"][i]),
            )
            result.update(
                {f"p_{tick/2:.1f}": float(value) for tick, value in enumerate(duration)}
            )
            prices = [
                price_result(red, fight.get("red_odds")),
                price_result(1 - red, fight.get("blue_odds")),
            ]
            available = [(j, r) for j, r in enumerate(prices) if r["ev"] is not None]
            if available:
                side, value = max(available, key=lambda pair: pair[1]["ev"])
                result["moneyline_pick"] = (
                    fight["red_name"] if side == 0 else fight["blue_name"]
                )
                result.update({f"moneyline_{key}": v for key, v in value.items()})
            for side, value in zip(("red", "blue"), prices):
                result.update({f"{side}_{key}": v for key, v in value.items()})
            if fight.get("total_line") is not None:
                over, under = total_probabilities(
                    duration, fight["total_line"], fight["number_of_rounds"]
                )
                result.update(
                    total_line=float(fight["total_line"]),
                    over_probability=over,
                    under_probability=under,
                )
                for side, prob in [("over", over), ("under", under)]:
                    result.update(
                        {
                            f"{side}_{key}": v
                            for key, v in price_result(
                                prob, fight.get(side + "_odds")
                            ).items()
                        }
                    )
    return pd.DataFrame(results), metadata


def markdown(headers, rows):
    def clean(value):
        return str(value).replace("|", "\\|").replace("\n", " ")

    return "\n".join(
        [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]
        + ["| " + " | ".join(map(clean, row)) + " |" for row in rows]
    )


def format_tables(results):
    def value(row, key, kind="number"):
        x = row.get(key)
        if x is None or pd.isna(x):
            return "—"
        return (
            f"{x:.1%}"
            if kind == "percent"
            else f"{int(x):+d}" if kind == "odds" else f"{x:.2f}"
        )

    summaries, values, distributions = [], [], []
    for row in results.to_dict("records"):
        if row["status"] != "OK":
            summaries.append([row["fight"], row["status"], "—", "—", "—", "—"])
            continue
        summaries.append(
            [
                row["fight"],
                row["winner"],
                value(row, "red_probability", "percent"),
                value(row, "blue_probability", "percent"),
                value(row, "predicted_rounds"),
                value(row, "distance_probability", "percent"),
            ]
        )
        distributions.append(
            [row["fight"]]
            + [value(row, f"p_{tick/2:.1f}", "percent") for tick in range(11)]
        )
        for market, pick in [
            ("moneyline", row.get("moneyline_pick")),
            ("over", f"Over {row.get('total_line')}"),
            ("under", f"Under {row.get('total_line')}"),
        ]:
            if market in ("over", "under") and pd.isna(row.get("total_line", np.nan)):
                continue
            if pick is None:
                continue
            values.append(
                [
                    row["fight"],
                    pick,
                    value(row, market + "_odds", "odds"),
                    value(row, market + "_ev", "percent"),
                    value(row, market + "_units_20"),
                    value(row, market + "_units_40"),
                ]
            )
    sections = [
        markdown(
            [
                "Fight",
                "Predicted winner",
                "Red win",
                "Blue win",
                "Predicted rounds",
                "Distance",
            ],
            summaries,
        )
    ]
    if values:
        sections.append(
            markdown(
                ["Fight", "Bet", "Odds", "EV", "Units ≥20% EV", "Units ≥40% EV"], values
            )
        )
    if distributions:
        sections.append(
            markdown(
                ["Fight"] + [f"P({tick/2:.1f})" for tick in range(11)], distributions
            )
        )
    sections.append(
        "Sizes use the project’s 3× Kelly unit convention, not bankroll percentages. EV thresholds have not been optimized for these transformers."
    )
    return "\n\n".join(sections)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("card", type=Path)
    p.add_argument(
        "--checkpoint",
        type=Path,
        default=ROOT / "models/transformers/artifacts/production/history_stats.pt",
    )
    p.add_argument("--date")
    p.add_argument("--csv", type=Path)
    p.add_argument("--device", default="auto")
    p.add_argument("--fights", type=Path, default=ROOT / "fights.csv")
    p.add_argument("--fighters", type=Path, default=ROOT / "fighters.csv")
    p.add_argument("--stats", type=Path, default=ROOT / "fight_stats.csv")
    args = p.parse_args()
    try:
        card, day, event = load_card(args.card)
        day = args.date or day
        if not day:
            raise ValueError("An event date is required")
        results, metadata = predict_card(
            card,
            day,
            args.checkpoint,
            args.fights,
            args.fighters,
            args.stats,
            args.device,
        )
        print(
            f"{event or 'UFC predictions'} — {day}\nModel: {metadata['model_config']['architecture']}_{metadata['model_config']['odds_mode']}; history through {metadata['train_through']}\n"
        )
        print(format_tables(results))
        if args.csv:
            args.csv.parent.mkdir(parents=True, exist_ok=True)
            results.to_csv(args.csv, index=False)
            args.csv.with_suffix(".metadata.json").write_text(
                json.dumps(metadata, indent=2) + "\n"
            )
    except (ValueError, OSError) as error:
        p.error(str(error))


if __name__ == "__main__":
    main()
