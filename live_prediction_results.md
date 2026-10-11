# Live prediction results

Last updated: 2026-10-11. Starting cumulative profit: 0 units.

Tracks settled pre-fight predictions at the original recorded odds and saved full-precision stakes (3× Kelly sizing). These are model portfolio results; actual wagers have not been confirmed. Display values are rounded, but totals use full precision.

The 20% and 40% EV thresholds are separate strategies. The 20% strategy includes qualifying 40% bets; do not add their profits together.

## Profit over time

Append one row per settled event and strategy. Cumulative profit is the sum of settled event profits within that strategy. Pending bets do not count toward profit.

| Event date | Event | EV threshold | W–L | Staked (units) | Moneyline profit | Round-total profit | Event net profit | Cumulative net profit | ROI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-10-03 | UFC 332 | 40% | 1–1 | 1.1707 | -0.2953 | +1.4969 | +1.2015 | +1.2015 | 102.64% |
| 2026-10-03 | UFC 332 | 20% | 4–3 | 7.2828 | +1.3635 | -0.0558 | +1.3077 | +1.3077 | 17.96% |

| 2026-10-10 | UFC Fight Night: Allen vs Duncan | 40% | 2–2 | 5.4021 | +1.9331 | +0.2838 | +2.2170 | +3.4185 | 52.01% |
| 2026-10-10 | UFC Fight Night: Allen vs Duncan | 20% | 5–5 | 10.6155 | +1.4419 | +1.2370 | +2.6789 | +3.9866 | 22.27% |

## UFC 332 bet ledger — 2026-10-03

Each unique bet appears once. “20% / 40%” means the bet belongs to both portfolios.

| Market | Bet | American odds | EV strategies | Stake (units) | Result | Net profit (units) |
|---|---|---:|---|---:|---|---:|
| Moneyline | Deiveson Figueiredo | +475 | 20% / 40% | 0.2953 | Loss | -0.2953 |
| Moneyline | Roman Kopylov | +195 | 20% | 0.3758 | Win | +0.7329 |
| Moneyline | Ismail Naurdiev | -135 | 20% | 1.2500 | Win | +0.9259 |
| Round totals | King Green vs Esteban Ribovics Under 1.5 | +171 | 20% / 40% | 0.8754 | Win | +1.4969 |
| Round totals | Deiveson Figueiredo vs Payton Talbott Over 2.5 | -110 | 20% | 0.8968 | Loss | -0.8968 |
| Round totals | Natalia Silva vs Wang Cong Over 4.5 | -250 | 20% | 2.0954 | Win | +0.8382 |
| Round totals | Imanol Rodriguez vs Alden Coria Over 2.5 | -140 | 20% | 1.4941 | Loss | -1.4941 |

Results source: [UFC 332 results](https://www.sherdog.com/events/UFC-332-Silva-vs-Wang-114123).

Audit records: [settlement CSV](logs/bet-settlement-2026-10-03.csv) and [settlement JSON](logs/bet-settlement-2026-10-03.json).

## UFC Fight Night: Allen vs Duncan bet ledger — 2026-10-10

Original saved odds and full-precision 3× Kelly stakes. Moneyline stakes were converted from the saved 10× run by multiplying by 0.3. No pre-fight predictions were changed.

| Market | Bet | American odds | EV strategies | Stake (units) | Result | Net profit (units) |
|---|---|---:|---|---:|---|---:|
| Moneyline | Christian Leroy Duncan | +105 | 20% / 40% | 1.3222 | Loss | -1.3222 |
| Moneyline | Matheus Camilo | -210 | 20% | 1.3468 | Loss | -1.3468 |
| Moneyline | Gerald Meerschaert | +254 | 20% | 0.3114 | Loss | -0.3114 |
| Moneyline | Allen Frye Jr. | +288 | 20% / 40% | 1.1303 | Win | +3.2553 |
| Moneyline | Alice Pereira | -135 | 20% | 1.5754 | Win | +1.1670 |
| Round totals | Brendan Allen vs Christian Leroy Duncan Over 3.5 | -125 | 20% / 40% | 1.7964 | Win | +1.4371 |
| Round totals | Matheus Camilo vs Jai Herbert Over 1.5 | -170 | 20% | 1.0285 | Win | +0.6050 |
| Round totals | Julius Walker vs Gerald Meerschaert Over 1.5 | +100 | 20% | 0.6498 | Win | +0.6498 |
| Round totals | Niko Price vs Leon Shahbazyan Over 1.5 | +190 | 20% / 40% | 1.1533 | Loss | -1.1533 |
| Round totals | Ernesta Kareckaite vs Melissa Gatto Under 2.5 | +276 | 20% | 0.3016 | Loss | -0.3016 |

Results source: [MMA Fighting — full results](https://www.mmafighting.com/ufc/514924/ufc-vegas-122-results-allen-vs-duncan), corroborated with [UFC main-card recap](https://jp.ufc.com/news/ufc-fight-night-allen-vs-duncan-results) and [UFC scorecards](https://jp.ufc.com/news/ufc-fight-night-allen-vs-duncan-official-scorecards).

Kareckaite–Gatto ended in a full-time draw. The Under 2.5 bet lost because all three rounds were completed; the draw does not void a total-duration bet. No qualifying moneyline bet was on that fight.

Audit records: [fight results](logs/value-predictions-2026-10-10/fight-results.csv), [settlement CSV](logs/value-predictions-2026-10-10/settlement.csv), and [settlement JSON](logs/value-predictions-2026-10-10/settlement.json).

## Accounting rules

- Winning bet net profit = stake × (decimal odds − 1). Losing bet net profit = −stake. Voids return the stake and contribute 0 profit.
- ROI = cumulative net profit / cumulative settled stake.
- Keep recorded odds and stakes fixed after a fight starts. Record corrections explicitly.
- For future events, append the bet ledger and event rows, carrying forward each strategy’s previous cumulative profit.
- If using the two-decimal stakes from the printed tables, UFC 332 profits would be +1.2048 units (40%) and +1.3217 units (20%). The tracker above consistently uses saved full-precision stakes.
