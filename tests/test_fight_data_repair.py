"""Tests use synthetic raw bouts; no database is contacted."""
from copy import deepcopy
from datetime import date
import unittest
from unittest.mock import MagicMock, patch

from scripts.fight_history import build_snapshots, latest_post_snapshot_id, outcome
from scripts.repair_fight_data import odds_plan, totals_plan


def fixture():
    fighters = [dict(id=i, name=name, date_of_birth='1990-01-01')
                for i, name in [(1, 'Red'), (2, 'Blue'), (3, 'Third')]]
    stats = []
    for i in range(1, 5):
        row = dict(id=i, fighter_id=None, ctrl_time='60', kd_landed=1,
                   subs_attempted=2, reversals=0)
        for cat in ('sig_str', 'head', 'body', 'leg', 'distance', 'clinch', 'ground', 'total_str', 'td'):
            row[f'{cat}_landed'] = i * 10
            row[f'{cat}_attempted'] = i * 20
        for rnd in range(1, 6):
            row[f'round_{rnd}_sig_str_landed'] = i * 10 if rnd == 1 else None
            row[f'round_{rnd}_sig_str_attempted'] = i * 20 if rnd == 1 else None
        stats.append(row)
    fights = []
    for i, day, red, blue, winner, rs, bs in [
        (1, '2020-01-01', 1, 2, 'Red', 1, 2),
        (2, '2020-02-01', 3, 1, 'Third', 3, 4),
    ]:
        fights.append(dict(id=i, date=day, r_fighter_id=red, b_fighter_id=blue,
                           r_name=fighters[red-1]['name'], b_name=fighters[blue-1]['name'],
                           winner=winner, win_method='KO/TKO', championship_fight=False,
                           finish_round=1, finish_time='2:00', total_fight_time=120,
                           r_fight_stats_id=rs, b_fight_stats_id=bs,
                           r_total_stats_id=None, b_total_stats_id=None,
                           fight_odds_id=None, r_fighter_odds=None, b_fighter_odds=None))
    return dict(fights=fights, fighters=fighters, fight_stats=stats,
                total_fight_stats=[], fight_odds=[])


def snapshots(data):
    return build_snapshots(data['fights'], data['fighters'], data['fight_stats'])[0]


class HistoryTests(unittest.TestCase):
    def test_first_fight_has_no_result_or_stat_leakage(self):
        values = snapshots(fixture())
        pre = values[(1, 'r', 'pre')]
        self.assertEqual(pre['wins'], 0)
        self.assertEqual(pre['sig_str_landed'], 0)
        self.assertEqual(pre['ctrl_time'], 0)
        self.assertEqual(pre['fight_time'], 0)
        self.assertEqual(pre['age'], (date(2020, 1, 1) - date(1990, 1, 1)).days)
        self.assertEqual(values[(1, 'r', 'post')]['wins'], 1)
        self.assertEqual(values[(1, 'r', 'post')]['sig_str_landed'], 10)

    def test_post_snapshot_contains_pre_fight_matchup_features(self):
        values = snapshots(fixture())
        first_red = values[(1, 'r', 'post')]
        self.assertIsNone(first_red['sig_str_accuracy'])
        self.assertEqual(first_red['wins_diff'], 0)
        self.assertEqual(first_red['is_debut'], 1)
        self.assertIsNone(first_red['days_since_last_fight'])
        later_blue = values[(2, 'b', 'post')]
        self.assertEqual(later_blue['sig_str_accuracy'], 0.5)
        self.assertEqual(later_blue['wins_diff'], 1)
        self.assertEqual(later_blue['age_diff'], 0)
        self.assertEqual(later_blue['days_since_last_fight'], 31)
        self.assertEqual(later_blue['is_debut'], 0)

    def test_prior_history_follows_fighter_across_corners(self):
        values = snapshots(fixture())
        later = values[(2, 'b', 'pre')]
        self.assertEqual(later['wins'], 1)
        self.assertEqual(later['losses'], 0)
        self.assertEqual(later['sig_str_landed'], 10)
        self.assertEqual(later['sig_str_absorbed'], 20)
        self.assertEqual(later['ctrl_time'], 60)
        self.assertEqual(later['fight_time'], 120)
        self.assertEqual(values[(2, 'b', 'post')]['losses'], 1)

    def test_changing_current_outcome_and_stats_does_not_change_its_features(self):
        data = fixture()
        old = snapshots(data)
        data['fights'][0]['winner'] = 'Blue'
        data['fight_stats'][0]['sig_str_landed'] = 999
        new = snapshots(data)
        for corner in ('r', 'b'):
            self.assertEqual(old[(1, corner, 'pre')], new[(1, corner, 'pre')])
        self.assertNotEqual(old[(2, 'b', 'pre')], new[(2, 'b', 'pre')])

    def test_input_order_and_future_results_do_not_change_earlier_features(self):
        data = fixture()
        old = snapshots(data)
        data['fights'].reverse()
        data['fights'][0]['winner'] = 'Red'
        new = snapshots(data)
        self.assertEqual(old[(1, 'r', 'pre')], new[(1, 'r', 'pre')])
        self.assertEqual(old[(2, 'b', 'pre')], new[(2, 'b', 'pre')])

    def test_same_date_bouts_share_date_start_history(self):
        data = fixture()
        data['fights'][1]['date'] = data['fights'][0]['date']
        values = snapshots(data)
        self.assertEqual(values[(2, 'b', 'pre')]['wins'], 0)
        self.assertEqual(values[(2, 'b', 'post')]['wins'], 1)
        self.assertEqual(values[(2, 'b', 'post')]['losses'], 1)

    def test_missing_measurements_and_unfought_rounds(self):
        data = fixture()
        data['fight_stats'][0]['ctrl_time'] = None
        values = snapshots(data)
        self.assertIsNone(values[(2, 'b', 'pre')]['ctrl_time'])
        self.assertEqual(values[(2, 'b', 'pre')]['round_5_sig_str_landed'], 0)

    def test_draws_and_no_contests_are_distinct(self):
        fight = fixture()['fights'][0]
        fight['winner'] = ''
        for method in ('Overturned', 'Could Not Continue', 'Other'):
            fight['win_method'] = method
            self.assertEqual(outcome(fight), (None, None))
        fight['win_method'] = 'Decision - Majority'
        self.assertEqual(outcome(fight), ('draws', 'draws'))

    def test_missing_fighter_metadata_does_not_erase_opponent_history(self):
        data = fixture()
        data['fights'][0]['b_fighter_id'] = None
        data['fighters'] = [f for f in data['fighters'] if f['id'] != 2]
        values, issues = build_snapshots(data['fights'], data['fighters'], data['fight_stats'])
        self.assertIsNone(values[(1, 'b', 'pre')]['age'])
        self.assertEqual(values[(2, 'b', 'pre')]['wins'], 1)
        self.assertEqual(len(issues), 1)

    def apply_plan(self, data, plan):
        removed = set(plan['deletes'])
        rows = {row['id']: row for row in data['total_fight_stats'] if row['id'] not in removed}
        rows.update({row['id']: row for row in plan['updates'] + plan['inserts']})
        data['total_fight_stats'] = list(rows.values())
        links = {row[0]: row[1:] for row in plan['links']}
        for fight in data['fights']:
            if fight['id'] in links:
                fight['r_total_stats_id'], fight['b_total_stats_id'] = links[fight['id']]

    def test_rebuild_is_idempotent_and_links_only_prior_post_snapshots(self):
        data = fixture()
        plan = totals_plan(data)
        self.apply_plan(data, plan)
        self.assertEqual(len(data['total_fight_stats']), 4)
        self.assertTrue(all(row['snapshot_type'] == 'post' for row in data['total_fight_stats']))
        self.assertIsNone(data['fights'][0]['r_total_stats_id'])
        self.assertIsNone(data['fights'][0]['b_total_stats_id'])
        self.assertIsNone(data['fights'][1]['r_total_stats_id'])
        source_id = data['fights'][1]['b_total_stats_id']
        source = next(row for row in data['total_fight_stats'] if row['id'] == source_id)
        self.assertEqual(source['fight_id'], 1)
        self.assertEqual(source['fighter_id'], 1)
        second = totals_plan(data)
        for field in ('inserts', 'updates', 'links', 'deletes'):
            self.assertFalse(second[field])

    def test_migration_deletes_pre_rows_preserves_post_ids_and_values(self):
        data = fixture()
        from scripts.repair_fight_data import stored_value
        for index, ((fid, corner, kind), snapshot) in enumerate(snapshots(data).items(), 100):
            row = {key: stored_value(key, value) for key, value in snapshot.items()}
            row.update(id=index, snapshot_type=kind)
            data['total_fight_stats'].append(row)
            if kind == 'pre':
                next(f for f in data['fights'] if f['id'] == fid)[corner+'_total_stats_id'] = index
        before = deepcopy(data['total_fight_stats'])
        plan = totals_plan(data)
        self.assertEqual(set(plan['deletes']), {r['id'] for r in before if r['snapshot_type'] == 'pre'})
        self.assertFalse(plan['inserts'])
        self.assertFalse(plan['updates'])
        self.apply_plan(data, plan)
        self.assertEqual(data['total_fight_stats'], [r for r in before if r['snapshot_type'] == 'post'])
        self.assertTrue(all(f[c+'_total_stats_id'] not in plan['deletes']
                            for f in data['fights'] for c in ('r', 'b')))

    def test_post_only_links_exclude_all_same_day_results(self):
        data = fixture()
        data['fights'][1]['date'] = data['fights'][0]['date']
        plan = totals_plan(data)
        self.apply_plan(data, plan)
        self.assertTrue(all(f[c+'_total_stats_id'] is None for f in data['fights'] for c in ('r', 'b')))

    def test_post_only_backfill_changes_later_source_link(self):
        data = fixture()
        self.apply_plan(data, totals_plan(data))
        original = data['fights'][1]['b_total_stats_id']
        extra = deepcopy(data['fights'][0])
        extra.update(id=99, date='2020-01-15', r_total_stats_id=None, b_total_stats_id=None)
        data['fights'].append(extra)
        plan = totals_plan(data)
        self.apply_plan(data, plan)
        new_source = data['fights'][1]['b_total_stats_id']
        self.assertNotEqual(new_source, original)
        self.assertEqual(next(row for row in data['total_fight_stats'] if row['id'] == new_source)['fight_id'], 99)

    def test_prior_unknown_measurement_is_not_replaced_by_zero(self):
        data = fixture()
        data['fight_stats'][0]['ctrl_time'] = None
        self.apply_plan(data, totals_plan(data))
        source = next(row for row in data['total_fight_stats']
                      if row['id'] == data['fights'][1]['b_total_stats_id'])
        self.assertIsNone(source['ctrl_time'])

    def test_latest_prediction_snapshot_is_post_and_date_bounded(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (7,)
        with patch('scripts.combine_tables.connect_database', return_value=conn):
            self.assertEqual(latest_post_snapshot_id(1, date(2020, 2, 1)), 7)
        query, params = cursor.execute.call_args.args
        self.assertIn("snapshot_type = 'post'", query)
        self.assertIn('f.date < %s', query)
        self.assertIn('ORDER BY f.date DESC', query)
        self.assertEqual(params, (1, date(2020, 2, 1)))


class OddsRepairTests(unittest.TestCase):
    def data(self):
        data = fixture()
        fight = data['fights'][0]
        fight.update(r_name='Rafa García', b_name='Opponent', fight_odds_id=1,
                     r_fighter_odds=-180, b_fighter_odds=150)
        data['fight_odds'] = [dict(id=1, fight_id=1, date=fight['date'],
                                  left_name='Rafa Garcia', right_name='Opponent',
                                  left_odds=150, right_odds=-180)]
        return data

    def test_repairs_existing_link_and_is_idempotent(self):
        data = self.data()
        changes, skipped = odds_plan(data)
        self.assertEqual(changes[0]['after'], (150, -180))
        self.assertFalse(skipped)
        data['fights'][0]['r_fighter_odds'], data['fights'][0]['b_fighter_odds'] = changes[0]['after']
        self.assertEqual(odds_plan(data), ([], []))
        self.assertEqual(data['fights'][0]['fight_odds_id'], 1)

    def test_bad_links_dates_and_names_are_not_guessed(self):
        for field, value in [('fight_id', 99), ('date', '2021-01-01'),
                             ('left_name', 'Unknown'), ('right_odds', None)]:
            data = self.data()
            data['fight_odds'][0][field] = value
            with self.subTest(field=field):
                changes, skipped = odds_plan(data)
                self.assertFalse(changes)
                self.assertEqual(len(skipped), 1)

    def test_reverse_source_order(self):
        data = self.data()
        data['fight_odds'][0].update(left_name='Opponent', right_name='Rafa Garcia',
                                    left_odds=-180, right_odds=150)
        self.assertEqual(odds_plan(data)[0][0]['after'], (150, -180))


if __name__ == '__main__':
    unittest.main()
