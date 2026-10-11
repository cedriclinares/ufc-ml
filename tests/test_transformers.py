"""Leakage, masking, symmetry, calibration, and real checkpoint/card integration."""

from copy import deepcopy
from dataclasses import replace
import tempfile
from pathlib import Path
import unittest

import numpy as np
import pandas as pd
import torch

from models.transformers.data import (
    build_data,
    ending,
    HISTORY_FIELDS,
    decimal_price,
    load_data,
)
from models.transformers.network import (
    ModelConfig,
    Preprocessor,
    multitask_loss,
    targets,
    probabilities,
)
from models.transformers.training import (
    TrainingConfig,
    model_for,
    fit_model,
    predict,
    save_checkpoint,
    load_checkpoint,
    chronological_parts,
)
from models.transformers.evaluation import (
    metrics,
    prediction_rows,
    reference_folds,
    baselines,
)
from models.comparisons.features import BASE_FEATURES
from scripts.predict_transformers import (
    predict_card,
    price_result,
    total_probabilities,
    format_tables,
)


def fixture(n=40):
    fighters = pd.DataFrame(
        [
            dict(
                id=i,
                name=f"Fighter {i}",
                date_of_birth="1990-01-01",
                height=65 + i,
                reach=66 + i,
                stance="Orthodox" if i % 2 else "Southpaw",
                gender="male",
                weight=155,
            )
            for i in range(1, 6)
        ]
    )
    fights, stats = [], []
    for j in range(n):
        red, blue = [(1, 2), (3, 1), (2, 4), (4, 3)][j % 4]
        scheduled = 5 if j % 5 == 0 else 3
        decision = j % 3 == 0
        finish = scheduled if decision else 1 + j % 2
        time = "5:00" if decision else "2:31"
        seconds = (finish - 1) * 300 + (300 if decision else 151)
        day = pd.Timestamp("2020-01-01") + pd.Timedelta(days=14 * j)
        fights.append(
            dict(
                id=j + 1,
                date=str(day.date()),
                r_fighter_id=red,
                b_fighter_id=blue,
                r_name=f"Fighter {red}",
                b_name=f"Fighter {blue}",
                winner=f"Fighter {red if j%2 else blue}",
                win_method="Decision - Unanimous" if decision else "KO/TKO",
                number_of_rounds=scheduled,
                finish_round=finish,
                finish_time=time,
                total_fight_time=seconds,
                weight=155,
                championship_fight=scheduled == 5,
                r_fight_stats_id=2 * j + 1,
                b_fight_stats_id=2 * j + 2,
                r_fighter_odds=150,
                b_fighter_odds=-175,
            )
        )
        for k, fid in enumerate((red, blue)):
            row = dict(
                id=2 * j + k + 1,
                fighter_id=fid,
                name=f"Fighter {fid}",
                ctrl_time="60",
                kd_landed=j % 2,
                subs_attempted=1,
                reversals=0,
            )
            for category in [
                "sig_str",
                "head",
                "body",
                "leg",
                "distance",
                "clinch",
                "ground",
                "total_str",
                "td",
            ]:
                row[category + "_landed"] = j + k + 1
                row[category + "_attempted"] = (j + k + 1) * 2
            for rnd in range(1, 6):
                row[f"round_{rnd}_sig_str_landed"] = j + 1 if rnd <= finish else None
                row[f"round_{rnd}_sig_str_attempted"] = (
                    (j + 1) * 2 if rnd <= finish else None
                )
            stats.append(row)
    return pd.DataFrame(fights), fighters, pd.DataFrame(stats)


class TransformerDataTests(unittest.TestCase):
    def test_invalid_endings_keep_valid_winner_targets(self):
        f, p, s = fixture(12)
        f.loc[1, "finish_time"] = "bad"
        f.loc[2, "finish_round"] = None
        f.loc[2, "total_fight_time"] = None
        data = build_data(f, p, s)
        self.assertTrue(data.rows.loc[[1, 2], "winner_target"].notna().all())
        self.assertTrue(data.rows.loc[[1, 2], "rounds_target"].isna().all())
        self.assertTrue(data.rows.loc[[1, 2], "distance_target"].isna().all())

    def test_current_future_outcomes_and_stats_do_not_leak(self):
        f, p, s = fixture(12)
        old = build_data(f, p, s)
        changed = f.copy()
        changed.loc[6, "winner"] = changed.loc[6, "b_name"]
        changed.loc[7:, "win_method"] = "Submission"
        other = s.copy()
        other.loc[12:, "sig_str_landed"] = 999
        new = build_data(changed, p, other)
        np.testing.assert_equal(old.summary[:7], new.summary[:7])
        np.testing.assert_equal(old.history[:7], new.history[:7])
        self.assertFalse(
            np.array_equal(old.summary[8:], new.summary[8:], equal_nan=True)
        )

    def test_same_date_elo_and_history_are_frozen(self):
        f, p, s = fixture(8)
        f.loc[1, "date"] = f.loc[0, "date"]
        data = build_data(f, p, s)
        self.assertTrue(data.padding[:2].all())
        for col in ["elo_16", "elo_32", "elo_48"]:
            self.assertTrue(
                (data.summary[:2, :, data.summary_fields.index(col)] == 1500).all()
            )
        changed = f.copy()
        changed.loc[0, "winner"] = changed.loc[0, "r_name"]
        newer = build_data(changed, p, s)
        np.testing.assert_equal(data.summary[:2], newer.summary[:2])

    def test_history_order_truncation_and_corner_changes(self):
        f, p, s = fixture(48)
        data = build_data(f, p, s)
        i = 47
        fid = data.rows.iloc[i].b_id
        expected = f.iloc[:i][
            f.iloc[:i].r_fighter_id.eq(fid) | f.iloc[:i].b_fighter_id.eq(fid)
        ].tail(10)
        mask = ~data.padding[i, 1]
        self.assertEqual(mask.sum(), 10)
        np.testing.assert_equal(
            data.history[i, 1, mask, -1],
            (
                pd.Timestamp(f.iloc[i].date) - pd.to_datetime(expected.date)
            ).dt.days.to_numpy(),
        )

    def test_debut_missing_measurements_and_unknown_categories(self):
        f, p, s = fixture(12)
        s.loc[0, "ctrl_time"] = None
        data = build_data(f, p, s)
        self.assertTrue(data.padding[0].all())
        self.assertTrue(
            np.isnan(data.history[1, 1, 0, HISTORY_FIELDS.index("ctrl_time")])
        )
        prep = Preprocessor().fit(data.take(range(6)))
        future = data.take([7])
        future.stance[:] = "Never seen"
        result = prep.transform(future)
        self.assertEqual(result["stance"].sum().item(), 0)
        self.assertTrue(torch.isfinite(result["history"]).all())

    def test_duration_ceiling_distance_and_task_masks(self):
        f, p, s = fixture(12)
        f.loc[1, "finish_round"] = 3
        f.loc[1, "finish_time"] = "4:59"
        f.loc[2, "winner"] = None
        f.loc[2, "win_method"] = "Overturned"
        d = build_data(f, p, s)
        self.assertEqual(d.rows.iloc[1].rounds_target, 3.0)
        self.assertEqual(d.rows.iloc[1].distance_target, 0.0)
        self.assertTrue(np.isnan(d.rows.iloc[2].winner_target))
        self.assertEqual(
            ending(dict(number_of_rounds=3, finish_round=1, finish_time="2:30")),
            (0.5, 0.0),
        )
        self.assertEqual(
            ending(dict(number_of_rounds=3, finish_round=1, finish_time="2:31")),
            (1.0, 0.0),
        )
        self.assertEqual(
            ending(dict(number_of_rounds=3, finish_round=1, finish_time="0:00")),
            (0.0, 0.0),
        )
        self.assertTrue(
            np.isnan(
                ending(dict(number_of_rounds=3, finish_round=4, finish_time="1:00"))[0]
            )
        )

    def test_preprocessing_and_partitions_use_only_training(self):
        f, p, s = fixture()
        d = build_data(f, p, s)
        a, b, c = chronological_parts(d)
        self.assertLess(d.rows.iloc[a].date.max(), d.rows.iloc[b].date.min())
        self.assertLess(d.rows.iloc[b].date.max(), d.rows.iloc[c].date.min())
        prep = Preprocessor().fit(d.take(a))
        saved = deepcopy(prep.state)
        d.summary[c] = 1e9
        prep.transform(d.take(c))
        self.assertEqual(saved, prep.state)

    def test_ambiguous_and_duplicate_ids_fail(self):
        f, p, s = fixture(5)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            build_data(pd.concat([f, f.iloc[[0]]]), p, s)
        p.loc[1, "name"] = p.loc[0, "name"]
        f.loc[0, "r_fighter_id"] = np.nan
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            build_data(f, p, s)

    def test_reference_folds_cover_each_test_id_once_and_keep_dates_intact(self):
        f, p, s = fixture(40)
        export = pd.DataFrame(0.0, index=range(len(f)), columns=BASE_FEATURES)
        for col in ("date", "r_name", "b_name"):
            export[col] = f[col]
        export["r_id"] = f.r_fighter_id
        export["b_id"] = f.b_fighter_id
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            export.to_csv(root / "features.csv", index=False)
            f.to_csv(root / "fights.csv", index=False)
            folds = reference_folds(
                root / "features.csv",
                root / "fights.csv",
                fraction=0.5,
                step=5,
                five_step=2,
            )
        for scheduled in (3, 5):
            selected = [r for r in folds if r["scheduled"] == scheduled]
            ids = [fid for r in selected for fid in r["ids"]]
            self.assertEqual(len(ids), len(set(ids)))
            for a, b in zip(selected, selected[1:]):
                self.assertLess(a["test_end"], b["test_start"])

    def test_cutoff_with_time_still_excludes_the_entire_event_date(self):
        f, p, s = fixture(12)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, frame in [("fights", f), ("fighters", p), ("stats", s)]:
                frame.to_csv(root / f"{name}.csv", index=False)
            data = load_data(
                root / "fights.csv",
                root / "fighters.csv",
                root / "stats.csv",
                before=f.iloc[6].date + "T18:00:00",
            )
        self.assertEqual(len(data), 6)
        self.assertLess(data.rows.date.max(), pd.Timestamp(f.iloc[6].date))

    def test_empty_raw_history_can_predict_known_debutants(self):
        f, p, s = fixture(2)
        upcoming = [
            dict(
                id=-1,
                date="2023-01-01",
                r_id=1,
                b_id=5,
                r_name="Fighter 1",
                b_name="Fighter 5",
                number_of_rounds=3,
                weight=155,
                championship_fight=False,
                _upcoming=True,
            )
        ]
        data = build_data(f.head(0), p, s.head(0), upcoming)
        self.assertEqual(len(data), 1)
        self.assertTrue(data.padding.all())
        self.assertTrue(
            (data.summary[:, :, data.summary_fields.index("experience")] == 0).all()
        )

    def test_upcoming_only_path_matches_full_reconstruction(self):
        f, p, s = fixture(40)
        upcoming = [
            dict(
                id=-1,
                date="2023-01-01",
                r_id=1,
                b_id=5,
                r_name="Fighter 1",
                b_name="Fighter 5",
                number_of_rounds=3,
                weight=155,
                championship_fight=False,
                _upcoming=True,
            )
        ]
        full = build_data(f, p, s, upcoming)
        full = full.take(np.flatnonzero(full.rows.upcoming))
        fast = build_data(f, p, s, upcoming, upcoming_only=True)
        for name in (
            "summary",
            "stance",
            "history",
            "method",
            "padding",
            "context",
            "gender",
            "market",
        ):
            np.testing.assert_equal(getattr(full, name), getattr(fast, name))


class TransformerNetworkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(2)
        cls.data = build_data(*fixture())

    def test_both_architectures_swap_symmetry_and_probability_masks(self):
        d = self.data.take(range(8))
        prep = Preprocessor().fit(d)
        for architecture in ("features", "history"):
            for mode in ("stats", "market"):
                config = ModelConfig(
                    architecture=architecture,
                    odds_mode=mode,
                    width=16,
                    heads=4,
                    layers=1,
                    feedforward=32,
                    dropout=0.0,
                )
                model = model_for(config, prep, d).eval()
                x = prep.transform(d, mode)
                y = {k: v.clone() for k, v in x.items()}
                for key in (
                    "summary",
                    "summary_missing",
                    "history",
                    "history_missing",
                    "stance",
                    "method",
                    "padding",
                ):
                    y[key] = y[key].flip(1)
                if mode == "market":
                    y["context"][:, -2:] = y["context"][:, -2:].flip(1)
                with torch.no_grad():
                    p = probabilities(model(x))
                    q = probabilities(model(y))
                torch.testing.assert_close(p["winner"], 1 - q["winner"])
                torch.testing.assert_close(p["rounds"], q["rounds"])
                torch.testing.assert_close(p["distance"], q["distance"])
                torch.testing.assert_close(p["rounds"].sum(1), torch.ones(len(d)))
                self.assertEqual(p["rounds"][x["scheduled"] == 3, 7:].sum().item(), 0.0)

    def test_padding_does_not_affect_predictions(self):
        d = self.data.take(range(6))
        prep = Preprocessor().fit(d)
        model = model_for(
            ModelConfig(width=16, layers=1, feedforward=32, dropout=0.0), prep, d
        ).eval()
        x = prep.transform(d)
        y = {k: v.clone() for k, v in x.items()}
        y["history"][y["padding"]] = 999
        y["history_missing"][y["padding"]] = 0
        with torch.no_grad():
            p = model(x)
            q = model(y)
        for key in p:
            torch.testing.assert_close(p[key], q[key])

    def test_masked_multitask_loss_updates_parameters(self):
        d = self.data.take(range(8))
        prep = Preprocessor().fit(d)
        model = model_for(ModelConfig(width=16, layers=1, feedforward=32), prep, d)
        x = prep.transform(d)
        y = targets(d)
        y["winner"][:2] = float("nan")
        y["rounds"][2:4] = float("nan")
        before = model.winner.weight.detach().clone()
        optimizer = torch.optim.AdamW(model.parameters(), lr=0.01)
        loss = multitask_loss(model(x), y, x["scheduled"])
        self.assertTrue(torch.isfinite(loss))
        loss.backward()
        optimizer.step()
        self.assertFalse(torch.equal(before, model.winner.weight))

    def test_end_to_end_training_checkpoint_card_and_cutoffs(self):
        f, p, s = fixture()
        d = self.data
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = ModelConfig(width=16, layers=1, feedforward=32, dropout=0.0)
            model, prep, t, info = fit_model(
                d,
                config,
                TrainingConfig(epochs=2, batch_size=16, threads=2, device="cpu"),
            )
            self.assertTrue(all(np.isfinite(v) and v > 0 for v in t.values()))
            self.assertFalse(
                set(info["optimizer_train_ids"]) & set(info["calibration_ids"])
            )
            checkpoint = root / "test.pt"
            save_checkpoint(checkpoint, model, prep, t, info)
            restored, rprep, rt, rinfo = load_checkpoint(checkpoint)
            for key, value in predict(model, prep, t, d).items():
                np.testing.assert_allclose(
                    value, predict(restored, rprep, rt, d)[key], rtol=1e-6, atol=1e-6
                )
            for name, frame in [("fights", f), ("fighters", p), ("stats", s)]:
                frame.to_csv(root / f"{name}.csv", index=False)
            card = [
                dict(
                    red_name="Fighter 1",
                    blue_name="Fighter 5",
                    weight_class=155,
                    number_of_rounds=3,
                    red_odds=-110,
                    blue_odds=100,
                    total_line=1.5,
                    over_odds=-120,
                    under_odds=100,
                )
            ]
            day = "2023-01-01"
            table, _ = predict_card(
                card,
                day,
                checkpoint,
                root / "fights.csv",
                root / "fighters.csv",
                root / "stats.csv",
                "cpu",
            )
            self.assertEqual(table.iloc[0].status, "OK")
            self.assertAlmostEqual(
                table.iloc[0].over_probability + table.iloc[0].under_probability,
                1.0,
                places=6,
            )
            self.assertIn("Units ≥20%", format_tables(table))
            self.assertIn("P(0.5)", format_tables(table))
            unpriced = deepcopy(card)
            for key in ("red_odds", "blue_odds", "over_odds", "under_odds"):
                unpriced[0].pop(key)
            table, _ = predict_card(
                unpriced,
                day,
                checkpoint,
                root / "fights.csv",
                root / "fighters.csv",
                root / "stats.csv",
                "cpu",
            )
            self.assertEqual(table.iloc[0].status, "OK")
            self.assertIsNone(table.iloc[0].over_ev)
            market_config = replace(config, odds_mode="market")
            market_model = model_for(market_config, prep, d)
            market_checkpoint = root / "market.pt"
            save_checkpoint(market_checkpoint, market_model, prep, t, info)
            table, _ = predict_card(
                unpriced,
                day,
                market_checkpoint,
                root / "fights.csv",
                root / "fighters.csv",
                root / "stats.csv",
                "cpu",
            )
            self.assertIn("requires both moneyline", table.iloc[0].status)
            with self.assertRaisesRegex(ValueError, "on/after"):
                predict_card(
                    card,
                    "2020-01-01",
                    checkpoint,
                    root / "fights.csv",
                    root / "fighters.csv",
                    root / "stats.csv",
                    "cpu",
                )
            card[0].pop("number_of_rounds")
            table, _ = predict_card(
                card,
                day,
                checkpoint,
                root / "fights.csv",
                root / "fighters.csv",
                root / "stats.csv",
                "cpu",
            )
            self.assertIn("number_of_rounds", table.iloc[0].status)

    def test_metric_reporting_masks_missing_targets(self):
        d = self.data.take(range(8))
        prep = Preprocessor().fit(d)
        model = model_for(ModelConfig(width=16, layers=1, feedforward=32), prep, d)
        values = predict(model, prep, dict(winner=1.0, rounds=1.0, distance=1.0), d)
        rows = prediction_rows(d, values, "history_stats", 1, "2019-12-01")
        rows.loc[0, "winner_target"] = np.nan
        result = metrics(rows)
        self.assertIn("history_stats|3|odds_available", result)
        self.assertTrue(
            np.isfinite(result["history_stats|3|all"]["duration"]["log_loss"])
        )

    def test_duration_prior_is_conditioned_on_scheduled_length(self):
        train = self.data.take(range(30))
        test = self.data.take(range(30, 40))
        values = baselines(train, test, "stats", iterations=1)
        five = test.rows.number_of_rounds.eq(5).to_numpy()
        self.assertTrue((values["prior"]["rounds"][five, 6] < 1e-5).all())
        np.testing.assert_allclose(values["prior"]["rounds"].sum(1), 1)


class TransformerPricingTests(unittest.TestCase):
    def test_ev_and_units(self):
        r = price_result(0.6, 150)
        self.assertAlmostEqual(r["ev"], 0.5)
        self.assertAlmostEqual(r["units_20"], 1.0)
        self.assertAlmostEqual(r["units_40"], 1.0)
        self.assertEqual(price_result(0.45, 150)["units_40"], 0.0)
        self.assertIsNone(price_result(0.6, None)["ev"])
        self.assertTrue(np.isnan(decimal_price(0)))
        with self.assertRaises(ValueError):
            price_result(1.2, 150)

    def test_total_probabilities_preserve_exact_boundary(self):
        p = np.zeros(11)
        p[3] = 0.4
        p[4] = 0.6
        self.assertEqual(total_probabilities(p, 1.5, 3), (0.6, 0.4))
        with self.assertRaises(ValueError):
            total_probabilities(p, 1.25, 3)
        with self.assertRaises(ValueError):
            total_probabilities(p, 3.0, 3)
        p = np.eye(11)[10]
        with self.assertRaises(ValueError):
            total_probabilities(p, 1.5, 3)


if __name__ == "__main__":
    unittest.main()
