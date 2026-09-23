"""Collect Tapology odds through a real, visible Chromium browser.

Setup:
    python3 -m pip install playwright
    python3 -m playwright install chromium
    python3 scripts/scrape_fight_odds_playwright.py --since 2023-12-10

Rows are printed as JSON. Optional --save uses the existing fight_odds table
and requires psycopg2 and PostgreSQL PGHOST/PGPORT/PGUSER/PGPASSWORD settings
(or a .pgpass file). PGDATABASE defaults to cedriclinares.
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


SEARCH_URL = (
    "https://www.tapology.com/search?term=ufc&commit=Submit"
    "&model%5Bevents%5D=eventsSearch"
)
LOG = logging.getLogger(__name__)


def parse_odds(text):
    """Return an American odds token, or None when odds are unavailable."""
    text = text.strip().replace("−", "-")
    if re.match(r"^(?:EVEN|EVS|EV)\b", text, re.IGNORECASE):
        return "+100"
    match = re.match(r"^([+-]\d+)\b", text)
    return match.group(1) if match else None


def navigate(page, url, selector):
    """Wait for rendered content; fail explicitly rather than return empty data."""
    from playwright.sync_api import Error as PlaywrightError

    for attempt in range(1, 4):
        try:
            response = page.goto(url, wait_until="domcontentloaded", timeout=60000)
            if response is not None and response.status >= 400:
                raise RuntimeError(f"HTTP {response.status}: {url}")
            page.locator(selector).first.wait_for(state="attached", timeout=30000)
            return
        except (PlaywrightError, RuntimeError) as error:
            if attempt == 3:
                raise RuntimeError(f"Could not load {url} after 3 attempts") from error
            LOG.warning("Navigation attempt %s failed: %s", attempt, error)
            time.sleep(6)


def collect_cards(page, since, until):
    """Read event search results, including available pagination links."""
    cards = {}
    visited = set()
    url = SEARCH_URL
    while url and url not in visited:
        visited.add(url)
        navigate(page, url, "table.fcLeaderboard tr")
        for row in page.locator("table.fcLeaderboard tr").all():
            cells = row.locator("td")
            if cells.count() < 3:
                continue
            link = cells.nth(0).locator("a").first
            if not link.count():
                continue
            name = link.inner_text().strip()
            if not re.match(r"^(UFC \d+\b|UFC Fight Night\b)", name):
                continue
            raw_date = cells.nth(2).inner_text().strip()
            try:
                event_date = datetime.strptime(raw_date, "%Y.%m.%d").date()
            except ValueError:
                LOG.warning("Skipping event with unrecognized date: %s (%s)", name, raw_date)
                continue
            href = link.get_attribute("href")
            if href and since <= event_date <= until:
                cards[urljoin(page.url, href)] = event_date
        next_link = page.locator(
            'a[rel="next"], a.next_page, .pagination a.next'
        ).first
        href = next_link.get_attribute("href") if next_link.count() else None
        url = urljoin(page.url, href) if href else None
        if url and url not in visited:
            time.sleep(6)
    return cards


def collect_fight_odds(page, event_date):
    """Read bout DOM, including comparison tables hidden by collapsed panels."""
    for index, bout in enumerate(page.locator("div[data-bout-wrapper]").all(), 1):
        names = bout.locator('a.link-primary-red[href*="/fightcenter/fighters/"]')
        # Deduplicate repeated desktop/mobile fighter links by their URLs.
        fighters = {}
        for link in names.all():
            name = link.text_content().strip()
            if name:
                fighters.setdefault(link.get_attribute("href"), name)
        if len(fighters) != 2:
            LOG.warning("Skipping bout %s: expected two fighter links, found %s", index, len(fighters))
            continue
        table = bout.locator('table[id="boutComparisonTable"]')
        odds_row = table.locator("tr").filter(
            has_text=re.compile(r"(?:Betting\s+)?Odds", re.IGNORECASE)
        ).first
        if not odds_row.count():
            LOG.warning("Skipping bout %s: no odds row", index)
            continue
        cells = odds_row.locator("td")
        if cells.count() < 5:
            LOG.warning("Skipping bout %s: unexpected odds columns", index)
            continue
        left_odds = parse_odds(cells.nth(0).text_content())
        right_odds = parse_odds(cells.nth(4).text_content())
        weight = bout.locator("span.bg-tap_darkgold").first
        if left_odds is None or right_odds is None or not weight.count():
            LOG.warning("Skipping bout %s: missing odds or weight class", index)
            continue
        left_name, right_name = fighters.values()
        yield {
            "left_name": left_name,
            "right_name": right_name,
            "left_odds": left_odds,
            "right_odds": right_odds,
            "weight_class": weight.text_content().strip(),
            "date": event_date.isoformat(),
        }


def save_odds_data(data):
    """Insert once per matchup/date/weight class; return whether a row was saved.

    Existing odds are preserved. A short transaction-level table lock serializes
    writers so simultaneous scraper runs cannot both insert the same fight.
    No unique constraint or schema migration is required.
    """
    database = os.environ.get("PGDATABASE", "cedriclinares")
    fight = f"{data['left_name']} vs {data['right_name']} ({data['date']})"
    LOG.info("Saving odds to database %s: %s", database, fight)
    try:
        import psycopg2

        with closing(psycopg2.connect(dbname=database)) as conn:
            with conn, conn.cursor() as cursor:
                cursor.execute("LOCK TABLE public.fight_odds IN SHARE ROW EXCLUSIVE MODE")
                cursor.execute(
                    """
                    INSERT INTO public.fight_odds
                        (left_name, right_name, left_odds, right_odds, weight_class, date)
                    SELECT %(left_name)s, %(right_name)s, %(left_odds)s,
                           %(right_odds)s, %(weight_class)s, %(date)s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM public.fight_odds AS existing
                        WHERE existing.left_name IS NOT DISTINCT FROM %(left_name)s
                          AND existing.right_name IS NOT DISTINCT FROM %(right_name)s
                          AND existing.date IS NOT DISTINCT FROM %(date)s::date
                          AND existing.weight_class IS NOT DISTINCT FROM %(weight_class)s
                    )
                    """,
                    data,
                )
                inserted = cursor.rowcount == 1
                if cursor.rowcount not in (0, 1):
                    raise RuntimeError(f"Expected zero or one inserted row, got {cursor.rowcount}")
            # The connection context has exited successfully, committing the row.
            if inserted:
                LOG.info("DB save committed: %s; inserted 1 row into fight_odds", fight)
            else:
                LOG.info("DB save skipped: %s; fight already exists", fight)
            return inserted
    except Exception:
        LOG.exception("DB save failed for %s in database %s", fight, database)
        raise


def scrape_fight_odds(since=date(2023, 12, 10), until=None, *, headless=False, save=False):
    from playwright.sync_api import sync_playwright

    until = until or date.today()
    if since > until:
        raise ValueError("since must be on or before until")
    results = []
    saved_count = 0
    skipped_count = 0
    LOG.info("Database saving %s", "enabled" if save else "disabled (use --save to enable)")
    with sync_playwright() as playwright:
        # The chromium channel uses the full browser even with --headless.
        browser = playwright.chromium.launch(channel="chromium", headless=headless)
        try:
            context = browser.new_context(viewport={"width": 1440, "height": 1000})
            page = context.new_page()
            cards = collect_cards(page, since, until)
            LOG.info("Found %s matching events", len(cards))
            for url, event_date in sorted(cards.items(), key=lambda item: item[1]):
                time.sleep(6)
                navigate(page, url, "div[data-bout-wrapper]")
                count = 0
                for data in collect_fight_odds(page, event_date):
                    print(json.dumps(data, ensure_ascii=False), flush=True)
                    if save:
                        if save_odds_data(data):
                            saved_count += 1
                        else:
                            skipped_count += 1
                    results.append(data)
                    count += 1
                LOG.info("Collected %s bouts from %s", count, url)
            if not results:
                LOG.warning("No odds collected; check the date range and website markup")
        finally:
            LOG.info(
                "Run totals: %s rows collected; %s DB saves committed; %s duplicates skipped",
                len(results), saved_count, skipped_count,
            )
            browser.close()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", type=date.fromisoformat, default=date(2023, 12, 10))
    parser.add_argument("--until", type=date.fromisoformat, default=date.today())
    parser.add_argument("--headless", action="store_true", help="Hide the Chromium window")
    parser.add_argument("--save", action="store_true", help="Insert collected rows into PostgreSQL")
    args = parser.parse_args()
    if args.since > args.until:
        parser.error("--since must be on or before --until")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    scrape_fight_odds(args.since, args.until, headless=args.headless, save=args.save)


if __name__ == "__main__":
    main()
