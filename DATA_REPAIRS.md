# Fight data repair and feature generation

Use the project's virtual environment. All repair and total-stat commands use
PostgreSQL's `PG*` settings; `PGDATABASE` defaults to `cedriclinares`.

```sh
source .venv/bin/activate
python scripts/repair_fight_data.py                 # read-only preview
python scripts/repair_fight_data.py --apply         # odds and totals
python scripts/repair_fight_data.py --only odds     # odds-only preview
python scripts/make_total_stats.py --apply         # rebuild totals after scraping/backfills
```

Every apply run locks the source tables, saves their complete before-images to
`backups/repairs/before-data-repair-*.json.gz`, writes within one transaction,
and checks the resulting data and computed training features before committing. JSON audit reports are written
to `logs/`. Keep the backup: it contains the original rows, IDs, training-view definition, and constraints for recovery.
Preview never changes database rows or schema. Imports never trigger a rebuild.
Re-running the repair with unchanged raw data produces no row corrections.

## Odds

The repair checks existing `fights.fight_odds_id` links, reciprocal
`fight_odds.fight_id`, event dates, and normalized fighter names. It assigns each
source odds value to its verified corner without clearing valid links. Incomplete,
ambiguous, or conflicting links are reported and left unchanged. Historical odds
that have never been linked still use the ordinary combine-tables workflow.

## Training totals: only post-fight snapshots are stored

`total_fight_stats` contains one post-fight row per fighter/bout, identified by
`snapshot_type = 'post'`. A database check constraint prevents pre rows from
being inserted. The migration preserves post IDs and deletes backed-up pre rows.

`fights.r_total_stats_id` and `fights.b_total_stats_id` reference each fighter's
latest snapshot from a **strictly earlier date**. They are NULL for the first
recorded fight. They do not point to the current fight's own post snapshot.
The builder refreshes these links whenever it rebuilds history, including after
backfills. Bouts on the same date cannot use one another's results.

Use the **ordinary SQL view** `training_fight_totals` to export training data.
It stores no additional rows. It reads the linked prior post snapshot, supplies
zero cumulative counts for a debut, preserves genuinely unknown measurements,
and uses age on the current fight date. Its computed `snapshot_type = 'pre'`
describes the returned features, not another stored snapshot.

Join the view by the current `fight_id` and `corner`:

```sql
SELECT f.id AS fight_id, f.date,
       r.wins AS r_wins, r.losses AS r_losses, r.age AS r_age,
       b.wins AS b_wins, b.losses AS b_losses, b.age AS b_age
FROM fights AS f
JOIN training_fight_totals AS r ON r.fight_id = f.id AND r.corner = 'r'
JOIN training_fight_totals AS b ON b.fight_id = f.id AND b.corner = 'b'
ORDER BY f.date, f.id;
```

Add other training columns explicitly using the same aliases. The view's `id`
identifies the current bout's post row; `source_snapshot_id` identifies the earlier
row supplying cumulative history. Neither is a prediction feature. Do not join
`fights.*_total_stats_id` to the view's `id`, and do not train directly on a
current fight's post row. A direct inner join to the prior source table would
also discard debuts and use age from the previous fight, so use the view instead.

The builder replays all available raw history, including old backfills. It does
not derive pre-fight values by subtracting wins from old cumulative rows. Bouts
on the same date share the history available at the start of the day. Missing
measurements propagate as unknown; an unfought round contributes zero. Empty
winner values from overturned/no-contest bouts do not count as draws. Historical
results reflect the currently stored official outcome, not an archived history
of when later overturns were announced.

The database's existing `legs_*` aggregate names are preserved, sourced from
`fight_stats.leg_*`. Ages remain in days and durations in seconds. The legacy
`opponent_fight_time` field retains its definition as the sum of prior career
fight time of previously faced opponents. `subs_evaded` remains opponent
submission attempts, as in the original schema; it is not a measured success rate.

Missing fighter IDs are listed in the report. Unambiguous existing name matches
can supply identity for the derived history; no fighter records or raw fight IDs
are silently rewritten. Unresolved names retain unknown ID/age. This repair does
not fill gaps in source coverage or claim lifetime records outside that coverage.

## Prediction and retraining

Both prediction scripts now retrieve **post** snapshots using event date, not
insertion order. `get_last_total_stats(fighter_id, before=event_date)` restricts
history to earlier dates; its default cutoff is today. These scripts use the same
database settings as the repair.

Re-export training CSVs from the corrected database and retrain the models.
Existing CSVs and model files still contain the old features. This change repairs
the historical total-stat leakage; it does not change random training splits,
calibration, or the separately identified MLP inference/scaling bug. Exclude
current-fight outcome fields (including actual duration) from prediction inputs.
