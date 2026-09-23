"""Regression checks for odds linking; database calls are always mocked."""
from contextlib import redirect_stdout
import importlib
import io
import unittest
from unittest.mock import MagicMock, patch

from scripts import combine_tables as combine


class CombineTablesTests(unittest.TestCase):
    def setUp(self):
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.addCleanup(self.output.__exit__, None, None, None)

    def test_original_and_alias_names_in_both_corner_orders(self):
        for original, canonical in [('Fighter', 'Fighter'), *combine.NAME_ALIASES.items()]:
            odds = (10, original, 'Opponent', '+150', '-180', 'Light Heavyweight', '2026-09-19')
            for reverse in (False, True):
                with self.subTest(name=original, reverse=reverse):
                    names = ('Opponent', canonical) if reverse else (canonical, 'Opponent')
                    result = combine.match_corner_to_fight_odds(odds, (20, *names))
                    self.assertEqual(result['r_fighter_odds'], '-180' if reverse else '+150')
                    self.assertEqual(result['b_fighter_odds'], '+150' if reverse else '-180')
                    self.assertEqual(result['fight_id'], 20)
                    self.assertEqual(result['fight_odds_id'], 10)

    def test_unicode_normalization(self):
        for name, expected in [
            ('Rafa García', 'Rafa Garcia'),
            ('Łukasz Żółć', 'Lukasz Zolc'),
            ('  Rafa\tGarci\u0301a  ', 'Rafa Garcia'),
            ('Kauê Fernandes', 'Kaue Fernandes'),
            ('  Joo Sang Yoo ', 'JooSang Yoo'),
        ]:
            with self.subTest(name=name):
                self.assertEqual(combine.normalize_fighter_name(name), expected)

    def test_accented_names_on_both_sides_and_both_orders(self):
        odds = (10, 'Rafa García', 'Opponent', '+150', '-180', 'Lightweight')
        for name in ('Rafa Garcia', 'Rafa García', 'Rafa Garci\u0301a'):
            for reverse in (False, True):
                names = ('Opponent', name) if reverse else (name, 'Opponent')
                with self.subTest(name=name, reverse=reverse):
                    result = combine.match_corner_to_fight_odds(odds, (20, *names))
                    self.assertEqual(result['r_fighter_odds'], '-180' if reverse else '+150')
                    self.assertEqual(result['b_fighter_odds'], '+150' if reverse else '-180')

    def test_alias_on_right(self):
        odds = (10, 'Opponent', 'Khalil Rountree', '-180', '+150', 'Light Heavyweight')
        result = combine.match_corner_to_fight_odds(odds, (20, 'Khalil Rountree Jr.', 'Opponent'))
        self.assertEqual(result['r_fighter_odds'], '+150')

    def test_lookup_and_assignment_share_normalized_names(self):
        odds = (10, 'Khalil Rountree', 'Opponent', '+150', '-180', 'Light Heavyweight', '2026-09-19')
        with patch.object(combine, 'get_all_fight_odds', return_value=[odds]), \
                patch.object(combine, 'get_fight_for_odds', return_value=(20, 'Khalil Rountree Jr.', 'Opponent')) as lookup, \
                patch.object(combine, 'save_fight_odds') as save:
            combine.get_fight_odds_ids()
            self.assertEqual(lookup.call_args.args[0]['left_name'], 'Khalil Rountree Jr.')
            self.assertEqual(save.call_args.args[0]['r_fighter_odds'], '+150')

    def test_unmatched_corners_are_not_saved(self):
        odds = (10, 'Fighter', 'Opponent', '+150', '-180', 'Light Heavyweight', '2026-09-19')
        with patch.object(combine, 'get_all_fight_odds', return_value=[odds]), \
                patch.object(combine, 'get_fight_for_odds', return_value=(20, 'Someone Else', 'Opponent')), \
                patch.object(combine, 'save_fight_odds') as save:
            combine.get_fight_odds_ids()
            save.assert_not_called()

    def test_database_settings(self):
        for env, expected in [({}, 'cedriclinares'), ({'PGDATABASE': 'test_ufc'}, 'test_ufc')]:
            with patch.dict(combine.os.environ, env, clear=True), patch.object(combine.psycopg2, 'connect') as connect:
                combine.connect_database()
                connect.assert_called_once_with(dbname=expected)

    def test_import_does_not_connect(self):
        with patch.object(combine.psycopg2, 'connect') as connect:
            importlib.reload(combine)
            connect.assert_not_called()

    def test_connection_failures_preserve_original_error(self):
        calls = [
            (combine.save_b_fighter_id, {}), (combine.save_r_fighter_id, {}),
            (combine.get_fighter_id, 'Fighter'), (combine.get_fight_for_odds, {}),
            (combine.save_fight_odds, {}), (combine.get_fights_without_fighter_ids,),
            (combine.get_fights_without_odds_ids,), (combine.get_all_fight_odds,),
        ]
        for function, *args in calls:
            with self.subTest(function=function.__name__):
                error = combine.psycopg2.OperationalError('test connection failure')
                with patch.object(combine, 'connect_database', side_effect=error):
                    with self.assertRaises(combine.psycopg2.OperationalError) as raised:
                        function(*args)
                    self.assertIs(raised.exception, error)

    def test_cursor_creation_failure_closes_connection(self):
        conn = MagicMock()
        conn.cursor.side_effect = RuntimeError('test cursor failure')
        with patch.object(combine, 'connect_database', return_value=conn):
            with self.assertRaisesRegex(RuntimeError, 'test cursor failure'):
                combine.get_all_fight_odds()
        conn.close.assert_called_once()

    def test_both_odds_updates_commit_together(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        with patch.object(combine, 'connect_database', return_value=conn):
            combine.save_fight_odds({})
        self.assertEqual(cursor.execute.call_count, 2)
        conn.commit.assert_called_once()
        cursor.close.assert_called_once()
        conn.close.assert_called_once()

    def test_failed_second_update_does_not_commit(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.execute.side_effect = [None, RuntimeError('test update failure')]
        with patch.object(combine, 'connect_database', return_value=conn):
            with self.assertRaisesRegex(RuntimeError, 'test update failure'):
                combine.save_fight_odds({})
        conn.commit.assert_not_called()
        cursor.close.assert_called_once()
        conn.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
