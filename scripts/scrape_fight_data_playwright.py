"""Scrape UFCStats with Playwright, preserving the existing fights/fight_stats fields.

Setup:
    python3 -m pip install playwright
    python3 -m playwright install chromium
    python3 scripts/scrape_fight_data_playwright.py --headless
    python3 scripts/scrape_fight_data_playwright.py --since 2023-12-10 --save

By default, collect the latest seven completed cards and print JSON records with
fight, r_stats and b_stats dictionaries. --since selects all matching cards.
--save also inserts into PostgreSQL (pip install psycopg2-binary), using PG*
environment variables or .pgpass; PGDATABASE defaults to cedriclinares. Database IDs
are null in preview mode. Importing this module does not scrape or write data.
"""

import argparse
from contextlib import closing
from datetime import date, datetime
import json
import logging
import os
import re
import time
from urllib.parse import urljoin

LOG = logging.getLogger(__name__)
EVENTS_URL = 'http://ufcstats.com/statistics/events/completed?page=all'


def clean(value):
    return ' '.join(value.split())


def navigate(page, url, selector):
    from playwright.sync_api import Error

    for attempt in range(3):
        try:
            response = page.goto(url, wait_until='domcontentloaded', timeout=60000)
            if response is not None and response.status >= 400:
                raise RuntimeError(f'HTTP {response.status}: {url}')
            page.locator(selector).first.wait_for(state='attached', timeout=30000)
            return
        except (Error, RuntimeError):
            if attempt == 2:
                raise
            time.sleep(3)


def collect_cards(page, since=None, until=None, limit=None):
    navigate(page, EVENTS_URL, 'a[href*="/event-details/"]')
    cards = {}
    for row in page.locator('tr').all():
        link = row.locator('a[href*="/event-details/"]').first
        if not link.count():
            continue
        raw = clean(row.inner_text())
        match = re.search(r'[A-Z][a-z]+\s+\d{1,2},\s+\d{4}', raw)
        if not match:
            raise ValueError(f'Missing event date: {raw}')
        event_date = datetime.strptime(match.group(), '%B %d, %Y').date()
        if event_date > (until or date.today()) or (since and event_date < since):
            continue
        # The completed-events listing can include an upcoming event row.
        if 'upcoming' in (row.get_attribute('class') or '').lower():
            continue
        cells = row.locator('td')
        cards[urljoin(page.url, link.get_attribute('href'))] = {
            'date': event_date.strftime('%B %d, %Y'),
            'location': clean(cells.last.inner_text()),
        }
    ordered = sorted(cards.items(), key=lambda item: datetime.strptime(item[1]['date'], '%B %d, %Y'), reverse=True)
    return ordered[:limit] if limit is not None else ordered


def labeled_values(container):
    """Read labeled leaf items without depending on HTML line breaks."""
    return container.locator('i, li, p').evaluate_all('''nodes => {
        const result = {};
        for (const node of nodes) {
            const label = Array.from(node.children).find(child =>
                /:\\s*$/.test(child.textContent.trim()));
            if (!label) continue;
            const copy = node.cloneNode(true);
            copy.children[Array.from(node.children).indexOf(label)].remove();
            const key = label.textContent.replace(/:\\s*$/, '').trim().toLowerCase();
            result[key] = copy.textContent.replace(/\\s+/g, ' ').trim();
        }
        return result;
    }''')


def get_fight_finish_details(page):
    container = page.locator('.b-fight-details__fight')
    values = labeled_values(container)
    for key in ('method', 'round', 'time', 'time format'):
        if not values.get(key):
            raise ValueError(f'Missing fight detail {key}: {page.url}')
    # Details can be sibling text/judge spans outside the label's own item.
    for paragraph in container.locator('p').all():
        text = clean(paragraph.text_content())
        if re.match(r'^Details\s*:', text, re.I):
            values['details'] = re.sub(r'^Details\s*:\s*', '', text, flags=re.I)
    title = container.locator('.b-fight-details__fight-title')
    icons = title.locator('img').evaluate_all('nodes => nodes.map(n => n.src)')
    rounds = re.match(r'(\d+)\s+Rnd', values['time format'], re.I)
    number_of_rounds = rounds.group(1) if rounds else values['time format'].split()[0]
    return {
        'win_method': values['method'],
        'win_method_details': values.get('details', ''),
        'finish_round': values['round'],
        'finish_time': values['time'],
        'number_of_rounds': number_of_rounds,
        'referee': values.get('referee', ''),
        'championship_fight': any(re.search(r'/belt\.png(?:\?|$)', src) for src in icons),
        'total_fight_time': (int(values['round']) - 1) * 300 + seconds(values['time']),
    }


def seconds(value):
    if value in ('--', '---', ''):
        return None
    minutes, secs = value.split(':')
    return int(minutes) * 60 + int(secs)


def pair(value):
    if value in ('--', '---', ''):
        return None, None
    match = re.fullmatch(r'(\d+)\s+of\s+(\d+)', value)
    if not match:
        raise ValueError(f'Invalid landed/attempted statistic: {value!r}')
    return match.groups()


def read_tables(page):
    """Include hidden per-round rows; identify columns by headings, not offsets."""
    return page.locator('table').evaluate_all('''tables => tables.map(table => ({
        headers: Array.from(table.querySelectorAll('thead th')).map(n =>
            n.textContent.replace(/%/g, 'pct').replace(/[^a-z0-9]/gi, '').toLowerCase()),
        rows: Array.from(table.querySelectorAll('tbody tr')).map(row =>
            Array.from(row.querySelectorAll('td')).map(cell => {
                const parts = Array.from(cell.querySelectorAll('p'));
                return (parts.length ? parts : [cell]).map(n =>
                    n.textContent.replace(/\\s+/g, ' ').trim());
            })).filter(row => row.length > 1)
    }))''')


def get_fight_stats(page):
    tables = read_tables(page)
    totals = next((t for t in tables if 'kd' in t['headers'] and len(t['rows']) == 1), None)
    significant = next((t for t in tables if 'head' in t['headers'] and len(t['rows']) == 1), None)
    if totals is None or significant is None:
        raise ValueError(f'Missing aggregate statistics tables: {page.url}')
    stats = [{}, {}]
    for table, mapping in (
        (totals, {'kd': 'KD_landed', 'sigstr': 'sig_str', 'totalstr': 'total_str',
                  'td': 'TD', 'subatt': 'subs_attempted', 'rev': 'reversals', 'ctrl': 'ctrl_time'}),
        (significant, {key: key for key in ('head', 'body', 'leg', 'distance', 'clinch', 'ground')}),
    ):
        for heading, key in mapping.items():
            if heading not in table['headers']:
                if heading == 'ctrl':
                    for fighter in stats:
                        fighter[key] = None
                    continue
                raise ValueError(f'Missing statistics column {heading}: {page.url}')
            values = table['rows'][0][table['headers'].index(heading)]
            if len(values) != 2:
                raise ValueError(f'Expected two values for {heading}: {values}')
            for fighter, value in zip(stats, values):
                if key == 'ctrl_time':
                    fighter[key] = seconds(value)
                elif key in ('KD_landed', 'subs_attempted', 'reversals'):
                    fighter[key] = None if value in ('--', '---', '') else value
                else:
                    fighter[key + '_landed'], fighter[key + '_attempted'] = pair(value)
    # UFCStats places the per-round table after its aggregate table.
    per_round = next((t for t in tables if t is not totals and 'kd' in t['headers'] and t['rows']), None)
    if per_round is None:
        raise ValueError(f'Missing per-round statistics: {page.url}')
    column = per_round['headers'].index('sigstr')
    for number in range(1, 6):
        values = per_round['rows'][number - 1][column] if number <= len(per_round['rows']) else ['', '']
        if len(values) != 2:
            raise ValueError(f'Expected two fighters in round {number}')
        for fighter, value in zip(stats, values):
            fighter[f'round_{number}_sig_str_landed'], fighter[f'round_{number}_sig_str_attempted'] = pair(value)
    return stats


def collect_fight(page, card):
    people = page.locator('.b-fight-details__person')
    if people.count() != 2:
        raise ValueError(f'Expected two fighters: {page.url}')
    fight = dict(card, **get_fight_finish_details(page), winner='')
    stats = get_fight_stats(page)
    urls = []
    for index, corner in enumerate(('r', 'b')):
        person = people.nth(index)
        link = person.locator('a[href*="/fighter-details/"]').first
        name = clean(link.inner_text())
        urls.append(urljoin(page.url, link.get_attribute('href')))
        fight[f'{corner}_name'] = name
        fight[f'{corner}_fighter_id'] = None
        fight[f'{corner}_fight_stats_id'] = None
        if clean(person.locator('.b-fight-details__person-status').inner_text()) == 'W':
            fight['winner'] = name
        stats[index].update(name=name, fighter_id=None)
    title = clean(page.locator('.b-fight-details__fight-title').inner_text())
    return {'fight': fight, 'r_stats': stats[0], 'b_stats': stats[1]}, urls, ('female' if 'Women' in title else 'male')


def collect_fighter(page, url, name, gender):
    navigate(page, url, '.b-list__box-list')
    values = labeled_values(page.locator('body'))
    height = re.fullmatch(r'''(\d+)'\s*(\d+)"''', values.get('height', ''))
    inches = int(height[1]) * 12 + int(height[2]) if height else None
    weight = re.search(r'\d+', values.get('weight', ''))
    reach = re.search(r'\d+', values.get('reach', ''))
    dob = values.get('dob', '--')
    return dict(name=name, height=inches, weight=weight.group() if weight else None,
                reach=reach.group() if reach else inches,
                stance=values.get('stance') or None,
                date_of_birth=None if dob == '--' else dob, gender=gender)


def insert(cursor, table, data):
    # Table/column identifiers come exclusively from this module's fixed schema.
    columns = ', '.join(data)
    placeholders = ', '.join(['%s'] * len(data))
    cursor.execute(f'INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING id', tuple(data.values()))
    return cursor.fetchone()[0]


def save_fight_data(record, fighter_page, urls, gender):
    """Commit the fight, both statistics rows and new fighters atomically."""
    import psycopg2

    fight = record['fight']
    with closing(psycopg2.connect(dbname=os.environ.get('PGDATABASE', 'cedriclinares'))) as conn:
        with conn, conn.cursor() as cursor:
            cursor.execute('LOCK TABLE fights, fighters IN SHARE ROW EXCLUSIVE MODE')
            cursor.execute('''SELECT id FROM fights WHERE date = %s::date AND
                ((r_name = %s AND b_name = %s) OR (r_name = %s AND b_name = %s))''',
                (fight['date'], fight['r_name'], fight['b_name'], fight['b_name'], fight['r_name']))
            if cursor.fetchone():
                LOG.info('Already saved: %s vs %s (event_date=%s)',
                         fight['r_name'], fight['b_name'], fight['date'])
                return False
            for corner, url in zip(('r', 'b'), urls):
                name = fight[f'{corner}_name']
                cursor.execute('SELECT id FROM fighters WHERE name = %s', (name,))
                existing = cursor.fetchone()
                fighter_id = existing[0] if existing else insert(cursor, 'fighters', collect_fighter(fighter_page, url, name, gender))
                stats = record[f'{corner}_stats']
                stats['fighter_id'] = fighter_id
                fight[f'{corner}_fighter_id'] = fighter_id
                fight[f'{corner}_fight_stats_id'] = insert(cursor, 'fight_stats', stats)
            insert(cursor, 'fights', fight)
    return True


def scrape_fight_data(since=None, until=None, *, limit=None, headless=False, save=False):
    from playwright.sync_api import sync_playwright

    if since and until and since > until:
        raise ValueError('since must be on or before until')
    if limit is not None and limit < 1:
        raise ValueError('limit must be positive')
    if since is None and limit is None:
        limit = 7
    LOG.info('Event date filter: since=%s until=%s (inclusive); limit=%s',
             since or 'unbounded', until or date.today(), limit or 'unlimited')
    results = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        try:
            context = browser.new_context()
            page = context.new_page()
            fighter_page = context.new_page() if save else None
            for card_url, card in collect_cards(page, since, until, limit):
                LOG.info('Scraping card: event_date=%s location=%s', card['date'], card['location'])
                navigate(page, card_url, 'table')
                urls = page.locator('a[href*="/fight-details/"], tr[data-link*="/fight-details/"]').evaluate_all(
                    'nodes => [...new Set(nodes.map(n => n.href || n.dataset.link))]')
                if not urls:
                    LOG.warning('No completed fights found: %s', card_url)
                for url in urls:
                    time.sleep(1)
                    navigate(page, urljoin(card_url, url), '.b-fight-details__person')
                    try:
                        record, fighter_urls, gender = collect_fight(page, card)
                    except ValueError:
                        LOG.exception('Could not parse fight %s', page.url)
                        raise
                    if save:
                        save_fight_data(record, fighter_page, fighter_urls, gender)
                    print(json.dumps(record, ensure_ascii=False), flush=True)
                    results.append(record)
        finally:
            browser.close()
    LOG.info('Collected %s fights', len(results))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--since', type=date.fromisoformat)
    parser.add_argument('--until', type=date.fromisoformat, default=date.today())
    parser.add_argument('--limit', type=int, help='Maximum number of cards (default: 7 unless --since is set)')
    parser.add_argument('--headless', action='store_true')
    parser.add_argument('--save', action='store_true', help='Save to the existing PostgreSQL tables')
    args = parser.parse_args()
    if args.since and args.since > args.until:
        parser.error('--since must be on or before --until')
    if args.limit is not None and args.limit < 1:
        parser.error('--limit must be positive')
    logging.basicConfig(level=logging.INFO, format='run_time=%(asctime)s %(levelname)s: %(message)s')
    scrape_fight_data(**vars(args))


if __name__ == '__main__':
    main()
