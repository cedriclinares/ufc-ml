"""Settle saved October 10 predictions without changing pre-fight inputs."""
from pathlib import Path
import json
import hashlib
from datetime import datetime, timezone
import numpy as np
import pandas as pd

OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[1]
source = 'https://www.mmafighting.com/ufc/514924/ufc-vegas-122-results-allen-vs-duncan'
# Verified against UFC recap/scorecards and the full MMA Fighting result list.
outcomes = [
    ('Brendan Allen', 5, '2:46', 'Submission'),
    ('Jai Herbert', 3, '5:00', 'Unanimous decision'),
    ('Loopy Godinez', 3, '5:00', 'Unanimous decision'),
    ('Andre Fili', 2, '0:22', 'KO'),
    ('Otari Tanzilovi', 2, '2:39', 'KO'),
    ('Julius Walker', 2, '3:35', 'TKO'),
    ('Francisco Prado', 1, '4:23', 'KO/TKO'),
    ('Niko Price', 1, '1:58', 'TKO'),
    ('Brendson Ribeiro', 1, '1:49', 'Submission'),
    ('Allen Frye Jr.', 1, '2:59', 'KO/TKO'),
    ('Alice Pereira', 1, '5:00', 'TKO'),
    (None, 3, '5:00', 'Majority draw'),
]
card = json.loads((OUT / 'card.json').read_text())
assert len(card['fights']) == len(outcomes) == 12
results = []
for fight, (winner, rnd, time, method) in zip(card['fights'], outcomes):
    name = f"{fight['red_name']} vs {fight['blue_name']}"
    assert winner is None or winner in (fight['red_name'], fight['blue_name'])
    minutes, seconds = map(int, time.split(':'))
    elapsed = (rnd - 1) * 300 + minutes * 60 + seconds
    results.append(dict(fight=name, winner=winner, method=method, finish_round=rnd,
                        finish_time=time, elapsed_seconds=elapsed,
                        actual_rounds=float(np.ceil(elapsed / 150) / 2),
                        gone_distance=elapsed == fight['number_of_rounds'] * 300))
pd.DataFrame(results).to_csv(OUT / 'fight-results.csv', index=False)
lookup = {r['fight']: r for r in results}
moneyline = pd.read_csv(OUT / 'predictions.csv')
rounds = pd.read_csv(OUT / 'round-bets.csv')
ledger = []
for market, data in [('Moneyline', moneyline), ('Round totals', rounds)]:
    for _, row in data.iterrows():
        ev = row.expected_roi
        if ev < .2:
            continue
        result = lookup[row.fight]
        if market == 'Moneyline':
            assert row.status == 'OK'
            stake = row.calculated_units * .3  # Saved moneyline run used 10×.
            odds = int(row.pick_odds)
            bet = row.value_pick
            status = 'Void' if result['winner'] is None else ('Win' if bet == result['winner'] else 'Loss')
        else:
            stake = row.calculated_units  # Saved rounds run already used 3×.
            odds = int(row.odds)
            bet = f'{row.fight} {row.side} {row.line:g}'
            boundary = row.line * 300
            # Exact elapsed time, not the ceiling-rounded target, settles the bet.
            status = 'Push' if result['elapsed_seconds'] == boundary else (
                'Win' if (result['elapsed_seconds'] > boundary) == (row.side == 'Over') else 'Loss')
        payout = odds / 100 if odds > 0 else 100 / abs(odds)
        net = stake * payout if status == 'Win' else (-stake if status == 'Loss' else 0.)
        ledger.append(dict(market=market, fight=row.fight, bet=bet, odds=odds,
                           expected_roi=ev, qualifies_20=True, qualifies_40=bool(ev >= .4),
                           stake=float(stake), result=status, net_units=float(net)))
ledger = pd.DataFrame(ledger)
assert len(ledger) == 10 and ledger.result.isin(['Win', 'Loss']).all()
assert ledger[['market', 'bet']].duplicated().sum() == 0
ledger.to_csv(OUT / 'settlement.csv', index=False)
summaries = []
for threshold in (.2, .4):
    eligible = ledger[ledger.expected_roi >= threshold]
    for market in ['Moneyline', 'Round totals', 'Combined']:
        subset = eligible if market == 'Combined' else eligible[eligible.market == market]
        stake = float(subset.stake.sum())
        net = float(subset.net_units.sum())
        summaries.append(dict(threshold=threshold, market=market, bets=len(subset),
                              wins=int(subset.result.eq('Win').sum()), losses=int(subset.result.eq('Loss').sum()),
                              stake=stake, net_units=net, roi=net / stake))
audit = dict(settled_at=datetime.now(timezone.utc).isoformat(), source=source,
             corroborating_sources=['https://jp.ufc.com/news/ufc-fight-night-allen-vs-duncan-results',
                                    'https://jp.ufc.com/news/ufc-fight-night-allen-vs-duncan-official-scorecards'],
             basis='Saved full-precision 3× Kelly stakes and original odds; model portfolios, actual bets unconfirmed',
             inputs_sha256={n: hashlib.sha256((OUT / n).read_bytes()).hexdigest()
                            for n in ['card.json', 'predictions.csv', 'round-bets.csv']},
             summary=summaries)
(OUT / 'settlement.json').write_text(json.dumps(audit, indent=2) + '\n')

previous = json.loads((ROOT / 'logs/bet-settlement-2026-10-03.json').read_text())['summary']
tracker = ROOT / 'live_prediction_results.md'
text = tracker.read_text().replace('Last updated: 2026-10-04.', 'Last updated: 2026-10-11.')
assert '| 2026-10-10 |' not in text, 'Event already recorded; review existing settlement rather than double-counting'
new_rows = []
for threshold in (.4, .2):
    totals = next(s for s in summaries if s['threshold'] == threshold and s['market'] == 'Combined')
    prior = next(s for s in previous if s['threshold'] == threshold and s['market'] == 'Combined')
    ml = next(s for s in summaries if s['threshold'] == threshold and s['market'] == 'Moneyline')
    rt = next(s for s in summaries if s['threshold'] == threshold and s['market'] == 'Round totals')
    cumulative = prior['net_units'] + totals['net_units']
    roi = cumulative / (prior['stake'] + totals['stake'])
    new_rows.append(f"| 2026-10-10 | UFC Fight Night: Allen vs Duncan | {threshold:.0%} | {totals['wins']}–{totals['losses']} | {totals['stake']:.4f} | {ml['net_units']:+.4f} | {rt['net_units']:+.4f} | {totals['net_units']:+.4f} | {cumulative:+.4f} | {roi:.2%} |")
marker = '\n## UFC 332 bet ledger'
assert marker in text
text = text.replace(marker, '\n' + '\n'.join(new_rows) + '\n' + marker, 1)
section = ['## UFC Fight Night: Allen vs Duncan bet ledger — 2026-10-10', '',
           'Original saved odds and full-precision 3× Kelly stakes. Moneyline stakes were converted from the saved 10× run by multiplying by 0.3. No pre-fight predictions were changed.', '',
           '| Market | Bet | American odds | EV strategies | Stake (units) | Result | Net profit (units) |',
           '|---|---|---:|---|---:|---|---:|']
for _, r in ledger.iterrows():
    strategies = '20% / 40%' if r.qualifies_40 else '20%'
    section.append(f'| {r.market} | {r.bet} | {r.odds:+d} | {strategies} | {r.stake:.4f} | {r.result} | {r.net_units:+.4f} |')
section += ['', f'Results source: [MMA Fighting — full results]({source}), corroborated with [UFC main-card recap](https://jp.ufc.com/news/ufc-fight-night-allen-vs-duncan-results) and [UFC scorecards](https://jp.ufc.com/news/ufc-fight-night-allen-vs-duncan-official-scorecards).', '',
            'Kareckaite–Gatto ended in a full-time draw. The Under 2.5 bet lost because all three rounds were completed; the draw does not void a total-duration bet. No qualifying moneyline bet was on that fight.', '',
            'Audit records: [fight results](logs/value-predictions-2026-10-10/fight-results.csv), [settlement CSV](logs/value-predictions-2026-10-10/settlement.csv), and [settlement JSON](logs/value-predictions-2026-10-10/settlement.json).', '']
text = text.replace('## Accounting rules', '\n'.join(section) + '\n## Accounting rules', 1)
tracker.write_text(text)
print(json.dumps(summaries, indent=2))
print('\n'.join(new_rows))
