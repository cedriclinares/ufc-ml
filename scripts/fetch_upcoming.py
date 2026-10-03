"""Fetch upcoming UFC fight card from Tapology with fighter names and odds.

Usage:
    python scripts/fetch_upcoming.py URL
    python scripts/fetch_upcoming.py https://www.tapology.com/fightcenter/events/145961-ufc-fight-night
    python scripts/fetch_upcoming.py URL --headless

Outputs JSON lines, one per bout, with fields:
    red_name, blue_name, red_odds, blue_odds, weight_class, red_decimal, blue_decimal
"""

import argparse
import json
import logging
import re
import time

LOG = logging.getLogger(__name__)


def parse_odds(text):
    """Return American odds string, or None."""
    text = text.strip().replace("\u2212", "-").replace("\u2013", "-")
    if re.match(r"^(?:EVEN|EVS|EV)\b", text, re.IGNORECASE):
        return "+100"
    match = re.match(r"^([+-]\d+)\b", text)
    return match.group(1) if match else None


def american_to_decimal(american):
    """Convert American odds string to decimal odds."""
    odds = int(american)
    if odds > 0:
        return 1 + odds / 100
    else:
        return 1 + 100 / abs(odds)


def navigate(page, url, selector):
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


def collect_upcoming_fights(page):
    """Extract fighter names, odds, and weight class from a Tapology event page."""
    bouts = page.locator("div[data-bout-wrapper]").all()
    LOG.info("Found %d bout wrappers on page", len(bouts))

    seen = set()
    for index, bout in enumerate(bouts, 1):
        # Fighter names
        names = bout.locator('a.link-primary-red[href*="/fightcenter/fighters/"]')
        fighters = {}
        for link in names.all():
            name = link.text_content().strip()
            if name:
                fighters.setdefault(link.get_attribute("href"), name)
        if len(fighters) != 2:
            LOG.warning("Bout %d: expected 2 fighters, found %d — skipping", index, len(fighters))
            continue

        left_name, right_name = fighters.values()
        # Deduplicate desktop/mobile repeated bout wrappers
        bout_key = (left_name, right_name)
        if bout_key in seen:
            continue
        seen.add(bout_key)

        # Odds from comparison table
        table = bout.locator('table[id="boutComparisonTable"]')
        red_odds_str = None
        blue_odds_str = None

        if table.count():
            odds_row = table.locator("tr").filter(
                has_text=re.compile(r"(?:Betting\s+)?Odds", re.IGNORECASE)
            ).first
            if odds_row.count():
                cells = odds_row.locator("td")
                if cells.count() >= 5:
                    red_odds_str = parse_odds(cells.nth(0).text_content())
                    blue_odds_str = parse_odds(cells.nth(4).text_content())

        # Weight class
        weight = bout.locator("span.bg-tap_darkgold").first
        weight_class = weight.text_content().strip() if weight.count() else "Unknown"

        fight = {
            "red_name": left_name,
            "blue_name": right_name,
            "red_odds": red_odds_str,
            "blue_odds": blue_odds_str,
            "weight_class": weight_class,
        }

        if red_odds_str and blue_odds_str:
            fight["red_decimal"] = round(american_to_decimal(red_odds_str), 3)
            fight["blue_decimal"] = round(american_to_decimal(blue_odds_str), 3)

        yield fight


def fetch_upcoming(url, *, headless=False):
    from playwright.sync_api import sync_playwright

    fights = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(channel="chromium", headless=headless)
        try:
            context = browser.new_context(viewport={"width": 1440, "height": 1000})
            page = context.new_page()
            navigate(page, url, "div[data-bout-wrapper]")
            for fight in collect_upcoming_fights(page):
                print(json.dumps(fight, ensure_ascii=False), flush=True)
                fights.append(fight)
        finally:
            browser.close()
    return fights


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("url", help="Tapology event URL")
    parser.add_argument("--headless", action="store_true", help="Hide the browser window")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    fights = fetch_upcoming(args.url, headless=args.headless)
    if not fights:
        LOG.warning("No fights collected — check the URL and page structure")
    else:
        LOG.info("Collected %d fights", len(fights))


if __name__ == "__main__":
    main()
