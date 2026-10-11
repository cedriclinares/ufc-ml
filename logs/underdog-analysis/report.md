# GBT underdog predictions and betting units

Underdog means the fighter with larger decimal odds. Equal-price fights and missing outcomes are excluded. Some near-even underdogs still have negative American odds.
Always predicting an underdog loss is the favorite-picking classifier. Its underdog-win recall is zero. Classification is separate from choosing the highest-EV betting side.
Live replay uses gb_med_multielo_odds (300 iterations) over the original expanding, event-aligned test windows. Comparison trees use 40 iterations in the initial study and 200 in the fuller five-round study; they are distinct models and datasets.

## Classification

| Evaluation | Fights | Actual underdog wins | Always-loss accuracy | GBT accuracy | GBT underdog picks | Upset precision | Upset recall |
|---|---:|---:|---:|---:|---:|---:|---:|
| live_gbt_all | 1588 | 470 | 70.4% | 68.0% | 210 | 41.0% | 18.3% |
| initial_tree_market_3r | 1442 | 417 | 71.1% | 70.0% | 139 | 44.6% | 14.9% |
| initial_tree_market_5r | 146 | 50 | 65.8% | 65.1% | 17 | 47.1% | 16.0% |
| initial_tree_stats_3r | 1442 | 417 | 71.1% | 60.8% | 446 | 33.4% | 35.7% |
| initial_tree_stats_5r | 146 | 50 | 65.8% | 64.4% | 40 | 47.5% | 38.0% |
| fuller_tree_calibrated_market_5r | 146 | 50 | 65.8% | 62.3% | 23 | 39.1% | 18.0% |
| fuller_tree_calibrated_stats_5r | 146 | 50 | 65.8% | 64.4% | 38 | 47.4% | 36.0% |
| fuller_tree_market_5r | 146 | 50 | 65.8% | 62.3% | 23 | 39.1% | 18.0% |
| fuller_tree_stats_5r | 146 | 50 | 65.8% | 64.4% | 38 | 47.4% | 36.0% |

## Betting returns: live_gbt_all

Test dates: 2023-08-05 to 2026-09-19. Excluded: 22 fights.

| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |
|---|---:|---:|---:|---:|---:|---:|
| always_favorite_flat | 1588 | 1118 | 470 | 1588.00 | +3.68 | 0.2% |
| always_underdog_flat | 1588 | 470 | 1118 | 1588.00 | -237.44 | -15.0% |
| gbt_winner_flat | 1588 | 1080 | 508 | 1588.00 | -4.27 | -0.3% |
| gbt_underdog_winner_flat | 210 | 86 | 124 | 210.00 | -12.72 | -6.1% |
| underdog_ev_0_flat | 493 | 175 | 318 | 493.00 | -16.37 | -3.3% |
| underdog_ev_0_kelly3 | 493 | 175 | 318 | 254.99 | +6.27 | 2.5% |
| favorite_same_underdog_ev_0_flat | 493 | 318 | 175 | 493.00 | -21.49 | -4.4% |
| favorite_same_underdog_ev_0_kelly3 | 493 | 318 | 175 | 254.99 | -21.99 | -8.6% |
| all_sides_ev_0_kelly3 | 1374 | 820 | 554 | 1058.57 | +20.70 | 2.0% |
| underdog_ev_20_flat | 276 | 100 | 176 | 276.00 | +6.62 | 2.4% |
| underdog_ev_20_kelly3 | 276 | 100 | 176 | 218.20 | +12.15 | 5.6% |
| favorite_same_underdog_ev_20_flat | 276 | 176 | 100 | 276.00 | -17.12 | -6.2% |
| favorite_same_underdog_ev_20_kelly3 | 276 | 176 | 100 | 218.20 | -23.65 | -10.8% |
| all_sides_ev_20_kelly3 | 427 | 204 | 223 | 446.41 | +29.47 | 6.6% |
| underdog_ev_40_flat | 140 | 54 | 86 | 140.00 | +24.30 | 17.4% |
| underdog_ev_40_kelly3 | 140 | 54 | 86 | 141.57 | +22.48 | 15.9% |
| favorite_same_underdog_ev_40_flat | 140 | 86 | 54 | 140.00 | -19.68 | -14.1% |
| favorite_same_underdog_ev_40_kelly3 | 140 | 86 | 54 | 141.57 | -26.27 | -18.6% |
| all_sides_ev_40_kelly3 | 152 | 63 | 89 | 163.96 | +29.76 | 18.1% |

GBT minus always-loss accuracy: -2.39 percentage points; paired event-date bootstrap 95% interval [-4.19, -0.57].

## Betting returns: initial_tree_market_3r

Test dates: 2023-07-29 to 2026-09-19. Excluded: 19 fights.

| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |
|---|---:|---:|---:|---:|---:|---:|
| always_favorite_flat | 1442 | 1025 | 417 | 1442.00 | +15.64 | 1.1% |
| always_underdog_flat | 1442 | 417 | 1025 | 1442.00 | -240.44 | -16.7% |
| gbt_winner_flat | 1442 | 1010 | 432 | 1442.00 | +23.25 | 1.6% |
| gbt_underdog_winner_flat | 139 | 62 | 77 | 139.00 | -0.22 | -0.2% |
| underdog_ev_0_flat | 716 | 206 | 510 | 716.00 | -96.93 | -13.5% |
| underdog_ev_0_kelly3 | 716 | 206 | 510 | 242.87 | -6.31 | -2.6% |
| favorite_same_underdog_ev_0_flat | 716 | 510 | 206 | 716.00 | +2.02 | 0.3% |
| favorite_same_underdog_ev_0_kelly3 | 716 | 510 | 206 | 242.87 | -12.45 | -5.1% |
| all_sides_ev_0_kelly3 | 1150 | 505 | 645 | 422.47 | -9.62 | -2.3% |
| underdog_ev_20_flat | 346 | 93 | 253 | 346.00 | -19.64 | -5.7% |
| underdog_ev_20_kelly3 | 346 | 93 | 253 | 177.04 | +6.25 | 3.5% |
| favorite_same_underdog_ev_20_flat | 346 | 253 | 93 | 346.00 | -17.66 | -5.1% |
| favorite_same_underdog_ev_20_kelly3 | 346 | 253 | 93 | 177.04 | -16.40 | -9.3% |
| all_sides_ev_20_kelly3 | 355 | 98 | 257 | 185.28 | +5.80 | 3.1% |
| underdog_ev_40_flat | 160 | 40 | 120 | 160.00 | +5.40 | 3.4% |
| underdog_ev_40_kelly3 | 160 | 40 | 120 | 96.49 | +14.34 | 14.9% |
| favorite_same_underdog_ev_40_flat | 160 | 120 | 40 | 160.00 | -14.89 | -9.3% |
| favorite_same_underdog_ev_40_kelly3 | 160 | 120 | 40 | 96.49 | -13.47 | -14.0% |
| all_sides_ev_40_kelly3 | 160 | 40 | 120 | 96.49 | +14.34 | 14.9% |

GBT minus always-loss accuracy: -1.04 percentage points; paired event-date bootstrap 95% interval [-2.60, +0.49].

## Betting returns: initial_tree_market_5r

Test dates: 2023-10-14 to 2026-09-19. Excluded: 4 fights.

| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |
|---|---:|---:|---:|---:|---:|---:|
| always_favorite_flat | 146 | 96 | 50 | 146.00 | -7.29 | -5.0% |
| always_underdog_flat | 146 | 50 | 96 | 146.00 | -8.95 | -6.1% |
| gbt_winner_flat | 146 | 95 | 51 | 146.00 | -4.53 | -3.1% |
| gbt_underdog_winner_flat | 17 | 8 | 9 | 17.00 | +1.25 | 7.4% |
| underdog_ev_0_flat | 74 | 30 | 44 | 74.00 | +6.40 | 8.6% |
| underdog_ev_0_kelly3 | 74 | 30 | 44 | 25.64 | +2.08 | 8.1% |
| favorite_same_underdog_ev_0_flat | 74 | 44 | 30 | 74.00 | -9.78 | -13.2% |
| favorite_same_underdog_ev_0_kelly3 | 74 | 44 | 30 | 25.64 | -3.37 | -13.1% |
| all_sides_ev_0_kelly3 | 123 | 65 | 58 | 41.77 | +1.79 | 4.3% |
| underdog_ev_20_flat | 30 | 14 | 16 | 30.00 | +9.75 | 32.5% |
| underdog_ev_20_kelly3 | 30 | 14 | 16 | 18.54 | +4.38 | 23.6% |
| favorite_same_underdog_ev_20_flat | 30 | 16 | 14 | 30.00 | -8.54 | -28.5% |
| favorite_same_underdog_ev_20_kelly3 | 30 | 16 | 14 | 18.54 | -4.53 | -24.4% |
| all_sides_ev_20_kelly3 | 30 | 14 | 16 | 18.54 | +4.38 | 23.6% |
| underdog_ev_40_flat | 15 | 4 | 11 | 15.00 | -4.15 | -27.7% |
| underdog_ev_40_kelly3 | 15 | 4 | 11 | 11.26 | -2.24 | -19.9% |
| favorite_same_underdog_ev_40_flat | 15 | 11 | 4 | 15.00 | -0.42 | -2.8% |
| favorite_same_underdog_ev_40_kelly3 | 15 | 11 | 4 | 11.26 | -0.49 | -4.3% |
| all_sides_ev_40_kelly3 | 15 | 4 | 11 | 11.26 | -2.24 | -19.9% |

GBT minus always-loss accuracy: -0.68 percentage points; paired event-date bootstrap 95% interval [-6.71, +4.96].

## Betting returns: initial_tree_stats_3r

Test dates: 2023-07-29 to 2026-09-19. Excluded: 19 fights.

| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |
|---|---:|---:|---:|---:|---:|---:|
| always_favorite_flat | 1442 | 1025 | 417 | 1442.00 | +15.64 | 1.1% |
| always_underdog_flat | 1442 | 417 | 1025 | 1442.00 | -240.44 | -16.7% |
| gbt_winner_flat | 1442 | 877 | 565 | 1442.00 | -23.52 | -1.6% |
| gbt_underdog_winner_flat | 446 | 149 | 297 | 446.00 | -42.92 | -9.6% |
| underdog_ev_0_flat | 1055 | 282 | 773 | 1055.00 | -183.62 | -17.4% |
| underdog_ev_0_kelly3 | 1055 | 282 | 773 | 726.13 | -84.51 | -11.6% |
| favorite_same_underdog_ev_0_flat | 1055 | 773 | 282 | 1055.00 | +5.58 | 0.5% |
| favorite_same_underdog_ev_0_kelly3 | 1055 | 773 | 282 | 726.13 | -12.45 | -1.7% |
| all_sides_ev_0_kelly3 | 1299 | 436 | 863 | 846.90 | -80.46 | -9.5% |
| underdog_ev_20_flat | 800 | 200 | 600 | 800.00 | -144.14 | -18.0% |
| underdog_ev_20_kelly3 | 800 | 200 | 600 | 676.04 | -79.38 | -11.7% |
| favorite_same_underdog_ev_20_flat | 800 | 600 | 200 | 800.00 | +1.79 | 0.2% |
| favorite_same_underdog_ev_20_kelly3 | 800 | 600 | 200 | 676.04 | -12.42 | -1.8% |
| all_sides_ev_20_kelly3 | 838 | 224 | 614 | 717.14 | -74.30 | -10.4% |
| underdog_ev_40_flat | 593 | 135 | 458 | 593.00 | -106.20 | -17.9% |
| underdog_ev_40_kelly3 | 593 | 135 | 458 | 561.77 | -61.96 | -11.0% |
| favorite_same_underdog_ev_40_flat | 593 | 458 | 135 | 593.00 | -2.63 | -0.4% |
| favorite_same_underdog_ev_40_kelly3 | 593 | 458 | 135 | 561.77 | -14.12 | -2.5% |
| all_sides_ev_40_kelly3 | 594 | 136 | 458 | 563.23 | -60.74 | -10.8% |

GBT minus always-loss accuracy: -10.26 percentage points; paired event-date bootstrap 95% interval [-12.81, -7.67].

## Betting returns: initial_tree_stats_5r

Test dates: 2023-10-14 to 2026-09-19. Excluded: 4 fights.

| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |
|---|---:|---:|---:|---:|---:|---:|
| always_favorite_flat | 146 | 96 | 50 | 146.00 | -7.29 | -5.0% |
| always_underdog_flat | 146 | 50 | 96 | 146.00 | -8.95 | -6.1% |
| gbt_winner_flat | 146 | 94 | 52 | 146.00 | +12.36 | 8.5% |
| gbt_underdog_winner_flat | 40 | 19 | 21 | 40.00 | +10.85 | 27.1% |
| underdog_ev_0_flat | 100 | 33 | 67 | 100.00 | -3.50 | -3.5% |
| underdog_ev_0_kelly3 | 100 | 33 | 67 | 65.55 | +2.31 | 3.5% |
| favorite_same_underdog_ev_0_flat | 100 | 67 | 33 | 100.00 | -7.57 | -7.6% |
| favorite_same_underdog_ev_0_kelly3 | 100 | 67 | 33 | 65.55 | -7.43 | -11.3% |
| all_sides_ev_0_kelly3 | 135 | 55 | 80 | 85.14 | +1.68 | 2.0% |
| underdog_ev_20_flat | 79 | 27 | 52 | 79.00 | +0.85 | 1.1% |
| underdog_ev_20_kelly3 | 79 | 27 | 52 | 61.99 | +2.97 | 4.8% |
| favorite_same_underdog_ev_20_flat | 79 | 52 | 27 | 79.00 | -8.65 | -10.9% |
| favorite_same_underdog_ev_20_kelly3 | 79 | 52 | 27 | 61.99 | -7.44 | -12.0% |
| all_sides_ev_20_kelly3 | 89 | 33 | 56 | 72.31 | +3.02 | 4.2% |
| underdog_ev_40_flat | 50 | 17 | 33 | 50.00 | +4.10 | 8.2% |
| underdog_ev_40_kelly3 | 50 | 17 | 33 | 47.30 | +2.58 | 5.5% |
| favorite_same_underdog_ev_40_flat | 50 | 33 | 17 | 50.00 | -6.98 | -14.0% |
| favorite_same_underdog_ev_40_kelly3 | 50 | 33 | 17 | 47.30 | -6.02 | -12.7% |
| all_sides_ev_40_kelly3 | 50 | 17 | 33 | 47.30 | +2.58 | 5.5% |

GBT minus always-loss accuracy: -1.37 percentage points; paired event-date bootstrap 95% interval [-10.20, +7.48].

## Betting returns: fuller_tree_calibrated_market_5r

Test dates: 2023-10-14 to 2026-09-19. Excluded: 4 fights.

| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |
|---|---:|---:|---:|---:|---:|---:|
| always_favorite_flat | 146 | 96 | 50 | 146.00 | -7.29 | -5.0% |
| always_underdog_flat | 146 | 50 | 96 | 146.00 | -8.95 | -6.1% |
| gbt_winner_flat | 146 | 91 | 55 | 146.00 | -9.48 | -6.5% |
| gbt_underdog_winner_flat | 23 | 9 | 14 | 23.00 | -1.95 | -8.5% |
| underdog_ev_0_flat | 61 | 21 | 40 | 61.00 | -6.15 | -10.1% |
| underdog_ev_0_kelly3 | 61 | 21 | 40 | 29.01 | -0.28 | -1.0% |
| favorite_same_underdog_ev_0_flat | 61 | 40 | 21 | 61.00 | -2.83 | -4.6% |
| favorite_same_underdog_ev_0_kelly3 | 61 | 40 | 21 | 29.01 | -2.49 | -8.6% |
| all_sides_ev_0_kelly3 | 121 | 64 | 57 | 64.01 | -1.92 | -3.0% |
| underdog_ev_20_flat | 38 | 14 | 24 | 38.00 | -2.35 | -6.2% |
| underdog_ev_20_kelly3 | 38 | 14 | 24 | 24.62 | -0.64 | -2.6% |
| favorite_same_underdog_ev_20_flat | 38 | 24 | 14 | 38.00 | -3.00 | -7.9% |
| favorite_same_underdog_ev_20_kelly3 | 38 | 24 | 14 | 24.62 | -2.06 | -8.4% |
| all_sides_ev_20_kelly3 | 44 | 16 | 28 | 31.04 | -3.49 | -11.3% |
| underdog_ev_40_flat | 10 | 2 | 8 | 10.00 | -4.80 | -48.0% |
| underdog_ev_40_kelly3 | 10 | 2 | 8 | 9.69 | -2.86 | -29.6% |
| favorite_same_underdog_ev_40_flat | 10 | 8 | 2 | 10.00 | +0.95 | 9.5% |
| favorite_same_underdog_ev_40_kelly3 | 10 | 8 | 2 | 9.69 | +0.25 | 2.6% |
| all_sides_ev_40_kelly3 | 10 | 2 | 8 | 9.69 | -2.86 | -29.6% |

GBT minus always-loss accuracy: -3.42 percentage points; paired event-date bootstrap 95% interval [-10.42, +3.47].

## Betting returns: fuller_tree_calibrated_stats_5r

Test dates: 2023-10-14 to 2026-09-19. Excluded: 4 fights.

| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |
|---|---:|---:|---:|---:|---:|---:|
| always_favorite_flat | 146 | 96 | 50 | 146.00 | -7.29 | -5.0% |
| always_underdog_flat | 146 | 50 | 96 | 146.00 | -8.95 | -6.1% |
| gbt_winner_flat | 146 | 94 | 52 | 146.00 | +8.72 | 6.0% |
| gbt_underdog_winner_flat | 38 | 18 | 20 | 38.00 | +9.00 | 23.7% |
| underdog_ev_0_flat | 95 | 31 | 64 | 95.00 | -6.05 | -6.4% |
| underdog_ev_0_kelly3 | 95 | 31 | 64 | 57.79 | +5.38 | 9.3% |
| favorite_same_underdog_ev_0_flat | 95 | 64 | 31 | 95.00 | -6.69 | -7.0% |
| favorite_same_underdog_ev_0_kelly3 | 95 | 64 | 31 | 57.79 | -8.49 | -14.7% |
| all_sides_ev_0_kelly3 | 131 | 52 | 79 | 80.82 | +4.53 | 5.6% |
| underdog_ev_20_flat | 69 | 25 | 44 | 69.00 | +6.35 | 9.2% |
| underdog_ev_20_kelly3 | 69 | 25 | 44 | 52.12 | +8.17 | 15.7% |
| favorite_same_underdog_ev_20_flat | 69 | 44 | 25 | 69.00 | -10.81 | -15.7% |
| favorite_same_underdog_ev_20_kelly3 | 69 | 44 | 25 | 52.12 | -9.98 | -19.1% |
| all_sides_ev_20_kelly3 | 80 | 32 | 48 | 64.39 | +10.09 | 15.7% |
| underdog_ev_40_flat | 48 | 17 | 31 | 48.00 | +5.55 | 11.6% |
| underdog_ev_40_kelly3 | 48 | 17 | 31 | 42.37 | +7.33 | 17.3% |
| favorite_same_underdog_ev_40_flat | 48 | 31 | 17 | 48.00 | -8.10 | -16.9% |
| favorite_same_underdog_ev_40_kelly3 | 48 | 31 | 17 | 42.37 | -8.97 | -21.2% |
| all_sides_ev_40_kelly3 | 48 | 17 | 31 | 42.37 | +7.33 | 17.3% |

GBT minus always-loss accuracy: -1.37 percentage points; paired event-date bootstrap 95% interval [-10.46, +7.38].

## Betting returns: fuller_tree_market_5r

Test dates: 2023-10-14 to 2026-09-19. Excluded: 4 fights.

| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |
|---|---:|---:|---:|---:|---:|---:|
| always_favorite_flat | 146 | 96 | 50 | 146.00 | -7.29 | -5.0% |
| always_underdog_flat | 146 | 50 | 96 | 146.00 | -8.95 | -6.1% |
| gbt_winner_flat | 146 | 91 | 55 | 146.00 | -9.48 | -6.5% |
| gbt_underdog_winner_flat | 23 | 9 | 14 | 23.00 | -1.95 | -8.5% |
| underdog_ev_0_flat | 53 | 21 | 32 | 53.00 | +1.85 | 3.5% |
| underdog_ev_0_kelly3 | 53 | 21 | 32 | 27.88 | -0.11 | -0.4% |
| favorite_same_underdog_ev_0_flat | 53 | 32 | 21 | 53.00 | -5.55 | -10.5% |
| favorite_same_underdog_ev_0_kelly3 | 53 | 32 | 21 | 27.88 | -2.11 | -7.6% |
| all_sides_ev_0_kelly3 | 127 | 74 | 53 | 86.19 | -1.24 | -1.4% |
| underdog_ev_20_flat | 29 | 10 | 19 | 29.00 | -5.20 | -17.9% |
| underdog_ev_20_kelly3 | 29 | 10 | 19 | 23.10 | -2.00 | -8.6% |
| favorite_same_underdog_ev_20_flat | 29 | 19 | 10 | 29.00 | -0.43 | -1.5% |
| favorite_same_underdog_ev_20_kelly3 | 29 | 19 | 10 | 23.10 | -0.81 | -3.5% |
| all_sides_ev_20_kelly3 | 41 | 16 | 25 | 39.28 | -5.17 | -13.2% |
| underdog_ev_40_flat | 10 | 2 | 8 | 10.00 | -4.80 | -48.0% |
| underdog_ev_40_kelly3 | 10 | 2 | 8 | 11.35 | -3.80 | -33.5% |
| favorite_same_underdog_ev_40_flat | 10 | 8 | 2 | 10.00 | +1.60 | 16.0% |
| favorite_same_underdog_ev_40_kelly3 | 10 | 8 | 2 | 11.35 | +1.32 | 11.6% |
| all_sides_ev_40_kelly3 | 11 | 2 | 9 | 12.86 | -5.32 | -41.3% |

GBT minus always-loss accuracy: -3.42 percentage points; paired event-date bootstrap 95% interval [-10.42, +3.47].

## Betting returns: fuller_tree_stats_5r

Test dates: 2023-10-14 to 2026-09-19. Excluded: 4 fights.

| Strategy | Bets | Wins | Losses | Wagered units | Net units | ROI |
|---|---:|---:|---:|---:|---:|---:|
| always_favorite_flat | 146 | 96 | 50 | 146.00 | -7.29 | -5.0% |
| always_underdog_flat | 146 | 50 | 96 | 146.00 | -8.95 | -6.1% |
| gbt_winner_flat | 146 | 94 | 52 | 146.00 | +8.72 | 6.0% |
| gbt_underdog_winner_flat | 38 | 18 | 20 | 38.00 | +9.00 | 23.7% |
| underdog_ev_0_flat | 82 | 30 | 52 | 82.00 | +4.65 | 5.7% |
| underdog_ev_0_kelly3 | 82 | 30 | 52 | 55.70 | +8.96 | 16.1% |
| favorite_same_underdog_ev_0_flat | 82 | 52 | 30 | 82.00 | -9.81 | -12.0% |
| favorite_same_underdog_ev_0_kelly3 | 82 | 52 | 30 | 55.70 | -9.58 | -17.2% |
| all_sides_ev_0_kelly3 | 132 | 61 | 71 | 98.73 | +7.46 | 7.6% |
| underdog_ev_20_flat | 60 | 24 | 36 | 60.00 | +12.45 | 20.8% |
| underdog_ev_20_kelly3 | 60 | 24 | 36 | 52.05 | +9.53 | 18.3% |
| favorite_same_underdog_ev_20_flat | 60 | 36 | 24 | 60.00 | -10.04 | -16.7% |
| favorite_same_underdog_ev_20_kelly3 | 60 | 36 | 24 | 52.05 | -9.33 | -17.9% |
| all_sides_ev_20_kelly3 | 80 | 35 | 45 | 79.99 | +8.47 | 10.6% |
| underdog_ev_40_flat | 40 | 16 | 24 | 40.00 | +9.15 | 22.9% |
| underdog_ev_40_kelly3 | 40 | 16 | 24 | 42.14 | +8.98 | 21.3% |
| favorite_same_underdog_ev_40_flat | 40 | 24 | 16 | 40.00 | -7.83 | -19.6% |
| favorite_same_underdog_ev_40_kelly3 | 40 | 24 | 16 | 42.14 | -8.90 | -21.1% |
| all_sides_ev_40_kelly3 | 44 | 19 | 25 | 49.43 | +11.09 | 22.4% |

GBT minus always-loss accuracy: -1.37 percentage points; paired event-date bootstrap 95% interval [-10.46, +7.38].

## Settlement and limits

Flat stakes are 1 unit per bet. Project Kelly units = max(0, 3 × EV / (decimal odds − 1)); EV = probability × decimal odds − 1. This is the existing unit convention, without bankroll compounding.
Underdog EV bets require the underdog to be the live script's highest-EV side and meet the specified threshold. A model can place a value bet on an underdog without predicting that underdog is more likely than the favorite to win.
favorite_same strategies bet the opposing favorite on exactly the same opportunities and, for Kelly, using exactly the same stake sizes. The no-underdog-bets baseline has zero wagers and zero profit; it is different from wagering on every favorite.
20% and 40% thresholds are the existing project choices, not optimized in this analysis. Portfolios overlap; do not add their profits. Full-precision stakes settle at recorded historical prices with wins paying stake × (decimal − 1), losses costing the stake.
Per-fight stakes and profits are in bet-audit.csv. results.json includes paired event-date bootstrap intervals for accuracy differences and mean profit per test fight, using 10,000 samples.
The legacy live GBT replay assumes exported statistics are pre-fight; it does not independently rebuild them. Historical odds timestamps are unverified. These are retrospective returns on a previously examined test period, not future profitability guarantees.

The full 1,610-fight live-model replay exactly matched the archived accuracy (67.64%) and flat-stake profit (−9.0848627148 units) before excluding 22 equal-price fights. All 171 strategy settlement totals and wager sums matched their per-fight audit rows. See verification.json.
