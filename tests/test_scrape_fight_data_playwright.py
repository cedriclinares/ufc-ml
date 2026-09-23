"""Offline checks for the legacy output contract and UFCStats table parsing."""
import csv
from datetime import date
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from scripts import scrape_fight_data_playwright as scraper

ROOT = Path(__file__).resolve().parents[1]


def table(headers, rows):
    return {'headers': headers.split(), 'rows': rows}


def tables():
    totals = [['Red', 'Blue'], ['1', '0'], ['20 of 40', '10 of 30'],
              ['50%', '33%'], ['30 of 55', '15 of 45'], ['1 of 2', '0 of 1'],
              ['50%', '0%'], ['2', '0'], ['0', '1'], ['1:12', '--']]
    sig = [['Red', 'Blue'], ['20 of 40', '10 of 30'], ['50%', '33%']]
    sig += [['3 of 8', '2 of 5']] * 6
    return [table('fighter kd sigstr sigstrpct totalstr td tdpct subatt rev ctrl', [totals]),
            table('fighter kd sigstr sigstrpct totalstr td tdpct subatt rev ctrl', [totals, totals]),
            table('fighter sigstr sigstrpct head body leg distance clinch ground', [sig])]


class StatsTests(unittest.TestCase):
    def parse(self, values):
        with patch.object(scraper, 'read_tables', return_value=values):
            return scraper.get_fight_stats(type('Page', (), {'url': 'fixture'})())

    def test_schema_values_and_unfought_rounds(self):
        red, blue = self.parse(tables())
        self.assertEqual(red['ctrl_time'], 72)
        self.assertIsNone(blue['ctrl_time'])
        self.assertEqual(red['TD_attempted'], '2')
        self.assertEqual(blue['round_2_sig_str_landed'], '10')
        self.assertIsNone(red['round_3_sig_str_landed'])
        with (ROOT / 'fight_stats.csv').open() as source:
            expected = set(next(csv.reader(source))) - {'id', 'name', 'fighter_id'}
        self.assertEqual({key.lower() for key in red}, expected)

    def test_reordered_columns(self):
        values = tables()
        for item in values:
            item['headers'].reverse()
            for row in item['rows']:
                row.reverse()
        self.assertEqual(self.parse(values)[0]['ctrl_time'], 72)

    def test_one_round_and_missing_control(self):
        values = tables()
        values[1]['rows'] = values[1]['rows'][:1]
        values[0]['headers'].pop()
        values[0]['rows'][0] = values[0]['rows'][0][:-1]
        red, _ = self.parse(values)
        self.assertIsNone(red['ctrl_time'])
        self.assertIsNone(red['round_2_sig_str_attempted'])

    def test_missing_aggregate_fails(self):
        with self.assertRaisesRegex(ValueError, 'aggregate'):
            self.parse([])

    def test_invalid_statistics_fail(self):
        with self.assertRaises(ValueError):
            scraper.pair('not a stat')
        self.assertEqual(scraper.pair('--'), (None, None))


class DateFilterTests(unittest.TestCase):
    def test_requested_range_excludes_2026_and_includes_both_endpoints(self):
        page = MagicMock()
        page.url = scraper.EVENTS_URL
        rows = []
        for index, day in enumerate(('September 19, 2026', 'October 05, 2025',
                                     'October 04, 2025', 'June 01, 2024',
                                     'December 09, 2023', 'December 08, 2023')):
            row = MagicMock()
            row.inner_text.return_value = f'UFC Event {day} Las Vegas, Nevada, USA'
            row.get_attribute.return_value = ''
            row.locator.return_value.first.count.return_value = 1
            row.locator.return_value.first.get_attribute.return_value = f'/event-details/{index}'
            row.locator.return_value.last.inner_text.return_value = 'Las Vegas, Nevada, USA'
            rows.append(row)
        page.locator.return_value.all.return_value = rows
        with patch.object(scraper, 'navigate'):
            cards = scraper.collect_cards(page, date(2023, 12, 9), date(2025, 10, 4))
        self.assertEqual([card['date'] for _, card in cards],
                         ['October 04, 2025', 'June 01, 2024', 'December 09, 2023'])

    def test_cli_forwards_equals_style_dates(self):
        argv = ['scrape_fight_data_playwright.py', '--headless',
                '--since=2023-12-09', '--until=2025-10-04', '--save']
        with patch('sys.argv', argv), patch.object(scraper, 'scrape_fight_data') as scrape, \
                patch.object(scraper.logging, 'basicConfig'):
            scraper.main()
        scrape.assert_called_once_with(since=date(2023, 12, 9), until=date(2025, 10, 4),
                                       limit=None, headless=True, save=True)


if __name__ == '__main__':
    unittest.main()
