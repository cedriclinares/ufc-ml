"""Compare out-of-sample GBT underdog classifications and betting returns."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from threadpoolctl import threadpool_limits

from models.comparisons.evaluation import decimal_odds, pipeline, split_index
from scripts.predict_value_bets import build_features
from scripts.report_five_round_study import paired_interval


def settle(winner_side, selected_side, prices, stakes):
    winner_side, selected_side, stakes = map(
        np.asarray, (winner_side, selected_side, stakes)
    )
    payout = prices[np.arange(len(prices)), selected_side]
    return stakes * np.where(winner_side == selected_side, payout - 1, -1)


def strategy_summary(winner, selected, prices, stakes):
    profits = settle(winner, selected, prices, stakes)
    bet = stakes > 0
    wagered = float(stakes.sum())
    return (
        dict(
            bets=int(bet.sum()),
            wins=int(((winner == selected) & bet).sum()),
            losses=int(((winner != selected) & bet).sum()),
            wagered=wagered,
            units=float(profits.sum()),
            roi=float(profits.sum() / wagered) if wagered else None,
        ),
        profits,
    )


def analyze(rows):
    prices = rows[["red_decimal", "blue_decimal"]].to_numpy(float)
    if not np.isfinite(prices).all() or (prices <= 1).any():
        raise ValueError("Invalid settlement prices")
    valid = rows.winner_side.notna().to_numpy() & (prices[:, 0] != prices[:, 1])
    dropped = int((~valid).sum())
    rows, prices = rows.loc[valid].reset_index(drop=True), prices[valid]
    if rows.empty:
        raise ValueError("No decisive fights with unequal odds")
    winner = rows.winner_side.to_numpy(int)
    p = rows.p_red.to_numpy(float)
    if not np.isfinite(p).all() or (p < 0).any() or (p > 1).any():
        raise ValueError("Invalid model probabilities")
    probability = np.column_stack([p, 1 - p])
    dog, favorite = prices.argmax(axis=1), prices.argmin(axis=1)
    predicted = probability.argmax(axis=1)
    actual_win, predicted_win = winner == dog, predicted == dog
    tp = int((actual_win & predicted_win).sum())
    fp = int((~actual_win & predicted_win).sum())
    fn = int((actual_win & ~predicted_win).sum())
    tn = int((~actual_win & ~predicted_win).sum())
    metrics = dict(
        fights=len(rows),
        excluded=dropped,
        underdog_wins=int(actual_win.sum()),
        underdog_losses=int((~actual_win).sum()),
        always_loss_accuracy=float((~actual_win).mean()),
        gbt_accuracy=float((actual_win == predicted_win).mean()),
        predicted_underdog_wins=int(predicted_win.sum()),
        upset_precision=float(tp / (tp + fp)) if tp + fp else None,
        upset_recall=float(tp / (tp + fn)) if tp + fn else None,
        confusion=dict(
            true_underdog_wins=tp,
            false_underdog_wins=fp,
            missed_underdog_wins=fn,
            correct_underdog_losses=tn,
        ),
    )
    metrics["accuracy_difference_interval"] = paired_interval(
        (actual_win == predicted_win).astype(float) - (~actual_win).astype(float),
        rows.date,
    )
    strategies, detail = {}, rows.copy()
    detail["underdog_side"] = dog
    detail["underdog_won"] = actual_win
    detail["p_underdog"] = probability[np.arange(len(rows)), dog]
    detail["underdog_decimal"] = prices[np.arange(len(rows)), dog]
    detail["predicted_underdog_win"] = predicted_win

    def add(name, selected, stakes):
        summary, profits = strategy_summary(
            winner, selected, prices, np.asarray(stakes, float)
        )
        summary["mean_profit_per_fight_interval"] = paired_interval(profits, rows.date)
        strategies[name] = summary
        detail[f"{name}_stake"] = stakes
        detail[f"{name}_profit"] = profits

    add("always_favorite_flat", favorite, np.ones(len(rows)))
    add("always_underdog_flat", dog, np.ones(len(rows)))
    add("gbt_winner_flat", predicted, np.ones(len(rows)))
    add("gbt_underdog_winner_flat", dog, predicted_win.astype(float))
    ev = probability * prices - 1
    # The live script selects Blue on an exact EV tie.
    value_side = np.where(ev[:, 0] > ev[:, 1], 0, 1)
    value_ev = ev[np.arange(len(rows)), value_side]
    selected_price = prices[np.arange(len(rows)), value_side]
    units = np.maximum(0, 3 * value_ev / (selected_price - 1))
    detail["underdog_ev"] = ev[np.arange(len(rows)), dog]
    detail["highest_ev_side"] = value_side
    for threshold in (0.0, 0.2, 0.4):
        selected = (value_side == dog) & (value_ev >= threshold) & (units > 0)
        key = f"underdog_ev_{int(threshold * 100)}"
        add(f"{key}_flat", dog, selected.astype(float))
        add(f"{key}_kelly3", dog, np.where(selected, units, 0))
        add(f"favorite_same_{key}_flat", favorite, selected.astype(float))
        add(f"favorite_same_{key}_kelly3", favorite, np.where(selected, units, 0))
        all_selected = (value_ev >= threshold) & (units > 0)
        add(
            f"all_sides_ev_{int(threshold * 100)}_kelly3",
            value_side,
            np.where(all_selected, units, 0),
        )
    metrics["strategies"] = strategies
    return metrics, detail


def comparison_rows(path, models, label):
    rows = pd.read_csv(path)
    if "seed" in rows:
        rows = rows[rows.seed.eq(0)]
    rows = rows[rows.model.isin(models)].copy()
    raw = pd.read_csv(ROOT / "fights.csv").set_index("id")
    prices = decimal_odds(raw.loc[rows.id])
    rows["red_decimal"], rows["blue_decimal"] = prices[:, 0], prices[:, 1]
    rows["winner_side"] = 1 - rows.winner_target
    rows["source"] = label
    return rows


def replay_live(output):
    data_path = ROOT / "final-dataset-names-and-ids.csv"
    df = pd.read_csv(data_path)
    df["date"] = pd.to_datetime(df.date)
    df = df.sort_values("date", kind="stable").reset_index(drop=True)
    if df.duplicated(["date", "r_id", "b_id"]).any():
        raise ValueError("Duplicate matchup/date")
    y = df.winner.map({"Red": 0, "Blue": 1})
    if y.isna().any():
        raise ValueError("Replay requires decisive outcomes")
    prices = decimal_odds(df)
    x = build_features(df, prices)
    boundary = split_index(df.date)
    config = dict(
        max_iter=300,
        max_leaf_nodes=31,
        learning_rate=0.05,
        l2_regularization=2,
        min_samples_leaf=20,
        early_stopping=False,
        random_state=0,
    )
    result, records = [], []
    start, fold = boundary, 0
    while start < len(df):
        raw_end = min(start + 100, len(df))
        end = (
            len(df)
            if raw_end == len(df)
            else int((df.date < df.date.iloc[raw_end]).sum())
        )
        if end <= start:
            raise ValueError("Walk-forward window failed to advance")
        if df.date.iloc[start - 1] >= df.date.iloc[start]:
            raise ValueError("Training and test event dates overlap")
        fold += 1
        print(
            f"Live GBT fold {fold}: {start} train, {end - start} test through {df.date.iloc[end-1].date()}",
            flush=True,
        )
        with threadpool_limits(limits=1):
            model = pipeline(HistGradientBoostingClassifier(**config), x.columns)
            model.fit(x.iloc[:start], y.iloc[:start].astype(int))
            probabilities = model.predict_proba(x.iloc[start:end])
        rows = df.iloc[start:end][["date", "r_id", "b_id", "r_name", "b_name"]].copy()
        rows["id"] = rows.index
        rows["p_red"] = probabilities[:, 0]
        rows["red_decimal"], rows["blue_decimal"] = (
            prices[start:end, 0],
            prices[start:end, 1],
        )
        rows["winner_side"] = y.iloc[start:end].to_numpy()
        rows["model"] = "gb_med_multielo_odds"
        rows["source"] = "live_gbt_replay"
        rows["fold"] = fold
        rows["train_through"] = str(df.date.iloc[start - 1].date())
        result.append(rows)
        records.append(
            dict(
                fold=fold,
                train_fights=start,
                test_fights=end - start,
                train_through=str(df.date.iloc[start - 1].date()),
                test_start=str(df.date.iloc[start].date()),
                test_end=str(df.date.iloc[end - 1].date()),
            )
        )
        pd.concat(result, ignore_index=True).to_csv(
            output / "live-gbt-predictions.csv", index=False
        )
        start = end
    metadata = dict(
        complete=True,
        model="gb_med_multielo_odds",
        config=config,
        folds=records,
        dataset_sha256=hashlib.sha256(data_path.read_bytes()).hexdigest(),
        features="Current live betting script build_features: exported pre-fight statistics, multi-Elo, streaks, experience and market probabilities",
        training_rows=boundary,
        test_rows=len(df) - boundary,
        created_utc=datetime.now(timezone.utc).isoformat(),
    )
    (output / "live-gbt-metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n"
    )
    return pd.concat(result, ignore_index=True)


def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    if args.resume_live and (args.output / "live-gbt-metadata.json").exists():
        meta = json.loads((args.output / "live-gbt-metadata.json").read_text())
        fingerprint = hashlib.sha256(
            (ROOT / "final-dataset-names-and-ids.csv").read_bytes()
        ).hexdigest()
        if not meta["complete"] or meta["dataset_sha256"] != fingerprint:
            raise ValueError("Incompatible live replay cache")
        live = pd.read_csv(args.output / "live-gbt-predictions.csv")
    else:
        live = replay_live(args.output)
    initial = comparison_rows(
        ROOT / "models/transformers/artifacts/walk-forward/predictions.csv",
        ["tree_stats", "tree_market"],
        "initial_40_iterations",
    )
    fuller = comparison_rows(
        ROOT / "logs/five-round-transformer-study/predictions.csv",
        [
            "tree_stats",
            "tree_market",
            "tree_calibrated_stats",
            "tree_calibrated_market",
        ],
        "fuller_200_iterations",
    )
    groups = {"live_gbt_all": live}
    for label, source in (("initial", initial), ("fuller", fuller)):
        for (model, scheduled), group in source.groupby(["model", "number_of_rounds"]):
            groups[f"{label}_{model}_{scheduled}r"] = group
    results, details = {}, []
    for name, rows in groups.items():
        if rows.duplicated(["id"]).any():
            raise ValueError("Duplicate test predictions")
        summary, detail = analyze(rows)
        summary["test_start"] = str(pd.to_datetime(rows.date).min().date())
        summary["test_end"] = str(pd.to_datetime(rows.date).max().date())
        results[name] = summary
        details.append(detail.assign(evaluation=name))
    pd.concat(details, ignore_index=True).to_csv(
        args.output / "bet-audit.csv", index=False
    )
    (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    write_report(args.output / "report.md", results)
    print(f"Saved underdog analysis to {args.output / 'report.md'}", flush=True)


def write_report(path, results):
    lines = [
        "# GBT underdog predictions and betting units",
        "",
        "Underdog means the fighter with larger decimal odds. Equal-price fights and missing outcomes are excluded. Some near-even underdogs still have negative American odds.",
        "Always predicting an underdog loss is the favorite-picking classifier. Its underdog-win recall is zero. Classification is separate from choosing the highest-EV betting side.",
        "Live replay uses gb_med_multielo_odds (300 iterations) over the original expanding, event-aligned test windows. Comparison trees use 40 iterations in the initial study and 200 in the fuller five-round study; they are distinct models and datasets.",
        "",
        "## Classification",
        "",
        "| Evaluation | Fights | Actual underdog wins | Always-loss accuracy | GBT accuracy | GBT underdog picks | Upset precision | Upset recall |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    def pct(value):
        return "—" if value is None else f"{value:.1%}"

    for name, r in results.items():
        lines.append(
            f"| {name} | {r['fights']} | {r['underdog_wins']} | {r['always_loss_accuracy']:.1%} | {r['gbt_accuracy']:.1%} | {r['predicted_underdog_wins']} | {pct(r['upset_precision'])} | {pct(r['upset_recall'])} |"
        )
    for name, r in results.items():
        lines.extend(
            [
                "",
                f"## Betting returns: {name}",
                "",
                f"Test dates: {r['test_start']} to {r['test_end']}. Excluded: {r['excluded']} fights.",
                "",
                "| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for strategy, m in r["strategies"].items():
            lines.append(
                f"| {strategy} | {m['bets']} | {m['wins']} | {m['losses']} | {m['wagered']:.2f} | {m['units']:+.2f} | {pct(m['roi'])} |"
            )
        ci = r["accuracy_difference_interval"]
        lines.extend(
            [
                "",
                f"GBT minus always-loss accuracy: {ci['difference'] * 100:+.2f} percentage points; paired event-date bootstrap 95% interval [{ci['low'] * 100:+.2f}, {ci['high'] * 100:+.2f}].",
            ]
        )
    lines.extend(
        [
            "",
            "## Settlement and limits",
            "",
            "Flat stakes are 1 unit per bet. Project Kelly units = max(0, 3 × EV / (decimal odds − 1)); EV = probability × decimal odds − 1. This is the existing unit convention, without bankroll compounding.",
            "Underdog EV bets require the underdog to be the live script's highest-EV side and meet the specified threshold. A model can place a value bet on an underdog without predicting that underdog is more likely than the favorite to win.",
            "favorite_same strategies bet the opposing favorite on exactly the same opportunities and, for Kelly, using exactly the same stake sizes. The no-underdog-bets baseline has zero wagers and zero profit; it is different from wagering on every favorite.",
            "20% and 40% thresholds are the existing project choices, not optimized in this analysis. Portfolios overlap; do not add their profits. Full-precision stakes settle at recorded historical prices with wins paying stake × (decimal − 1), losses costing the stake.",
            "Per-fight stakes and profits are in bet-audit.csv. results.json includes paired event-date bootstrap intervals for accuracy differences and mean profit per test fight, using 10,000 samples.",
            "The legacy live GBT replay assumes exported statistics are pre-fight; it does not independently rebuild them. Historical odds timestamps are unverified. These are retrospective returns on a previously examined test period, not future profitability guarantees.",
            "",
        ]
    )
    path.write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "logs/underdog-analysis")
    parser.add_argument("--resume-live", action="store_true")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
