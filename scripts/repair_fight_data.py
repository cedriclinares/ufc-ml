"""Preview or apply verified odds corrections and rebuild post-fight totals.

    python scripts/repair_fight_data.py
    python scripts/repair_fight_data.py --apply
    python scripts/repair_fight_data.py --only odds --apply

Uses PG* settings (PGDATABASE defaults to cedriclinares). Apply takes table locks,
saves a compressed before-image, then commits all changes in one transaction.
Ambiguous odds links are reported and left unchanged. Raw bout stats are retained.
Re-export training CSVs after applying; existing CSVs/models are not rewritten.
"""
import argparse
from contextlib import closing
from datetime import datetime, timezone
from itertools import groupby
import gzip
import json
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_values

try:
    from .combine_tables import connect_database, normalize_fighter_name
    from .fight_history import COUNT_FIELDS, DERIVED_FEATURES, as_date, build_snapshots
except ImportError:
    from combine_tables import connect_database, normalize_fighter_name
    from fight_history import COUNT_FIELDS, DERIVED_FEATURES, as_date, build_snapshots

TABLES = ('fights', 'fight_odds', 'fight_stats', 'fighters', 'total_fight_stats')
ROOT = Path(__file__).resolve().parents[1]


def read_data(cursor):
    result = {}
    for table in TABLES:
        cursor.execute(sql.SQL('SELECT * FROM {} ORDER BY id').format(sql.Identifier(table)))
        result[table] = [dict(row) for row in cursor.fetchall()]
    return result


def odds_plan(data):
    odds = {row['id']: row for row in data['fight_odds']}
    references = {}
    for fight in data['fights']:
        if fight['fight_odds_id'] is not None:
            references.setdefault(fight['fight_odds_id'], []).append(fight['id'])
    changes, skipped = [], []
    for fight in data['fights']:
        oid = fight['fight_odds_id']
        if oid is None:
            continue
        source = odds.get(oid)
        reason = None
        if source is None:
            reason = 'missing_odds_row'
        elif len(references[oid]) != 1:
            reason = 'odds_row_linked_to_multiple_fights'
        elif source['fight_id'] != fight['id']:
            reason = 'nonreciprocal_link'
        elif str(source['date']) != str(fight['date']):
            reason = 'date_mismatch'
        elif any(source[k] in (None, 0) for k in ('left_odds', 'right_odds')):
            reason = 'missing_or_invalid_odds'
        if reason:
            skipped.append({'fight_id': fight['id'], 'odds_id': oid, 'reason': reason})
            continue
        left, right, red, blue = [normalize_fighter_name(name) for name in
                                 (source['left_name'], source['right_name'], fight['r_name'], fight['b_name'])]
        if red == blue or left == right:
            skipped.append({'fight_id': fight['id'], 'odds_id': oid, 'reason': 'ambiguous_names'})
            continue
        if (left, right) == (red, blue):
            expected = (source['left_odds'], source['right_odds'])
        elif (left, right) == (blue, red):
            expected = (source['right_odds'], source['left_odds'])
        else:
            skipped.append({'fight_id': fight['id'], 'odds_id': oid, 'reason': 'name_mismatch',
                            'fight_names': [fight['r_name'], fight['b_name']],
                            'odds_names': [source['left_name'], source['right_name']]})
            continue
        old = (fight['r_fighter_odds'], fight['b_fighter_odds'])
        if old != expected:
            changes.append({'fight_id': fight['id'], 'odds_id': oid,
                            'red': fight['r_name'], 'blue': fight['b_name'],
                            'before': old, 'after': expected})
    return changes, skipped


def snapshot_key(row, kind):
    return (row['fight_id'], row['fighter_id'],
            normalize_fighter_name(row['name']) if row['fighter_id'] is None else '', kind)


def stored_value(key, value):
    # These two historical columns are TEXT even though their unit is seconds.
    return str(value) if value is not None and key in ('ctrl_time', 'opponent_ctrl_time') else value


def totals_plan(data):
    # Pre values exist only in memory to validate the historical lookup.
    snapshots, issues = build_snapshots(data['fights'], data['fighters'], data['fight_stats'])
    existing, deletes = {}, []
    for row in data['total_fight_stats']:
        kind = row.get('snapshot_type', 'post')
        if kind == 'pre':
            deletes.append(row['id'])
            continue
        if kind != 'post':
            raise ValueError(f"Unexpected snapshot type: {kind}")
        key = snapshot_key(row, kind)
        if key in existing:
            raise ValueError(f'Duplicate existing total snapshot: {key}')
        existing[key] = row
    inserts, updates, links = [], [], []
    next_id = max((row['id'] for row in data['total_fight_stats']), default=0) + 1
    ids, post_by_id = {}, {}
    for (fight_id, corner, kind), snapshot in snapshots.items():
        if kind != 'post':
            continue
        wanted = {key: stored_value(key, value) for key, value in snapshot.items()}
        wanted['snapshot_type'] = 'post'
        previous = existing.get(snapshot_key(snapshot, 'post'))
        if previous:
            wanted['id'] = previous['id']
            if any(previous.get(key, 'post' if key == 'snapshot_type' else None) != value
                   for key, value in wanted.items()):
                updates.append(wanted)
        else:
            wanted['id'] = next_id
            next_id += 1
            inserts.append(wanted)
        ids[(fight_id, corner)] = wanted['id']
        post_by_id[wanted['id']] = snapshot
    latest = {}
    ordered = sorted(data['fights'], key=lambda f: (as_date(f['date']), f['id']))
    for _, group in groupby(ordered, key=lambda f: as_date(f['date'])):
        fights = list(group)
        for fight in fights:
            prior_ids = []
            for corner in ('r', 'b'):
                snapshot = snapshots[(fight['id'], corner, 'post')]
                identity = snapshot_key(snapshot, 'post')[1:3]
                prior_id = latest.get(identity)
                prior_ids.append(prior_id)
                expected = snapshots[(fight['id'], corner, 'pre')]
                history = post_by_id[prior_id] if prior_id is not None else dict.fromkeys(COUNT_FIELDS, 0)
                if any(history[key] != expected[key] for key in COUNT_FIELDS):
                    raise ValueError(f"Historical lookup disagrees with replay: {fight['id']} {corner}")
            expected_ids = tuple(prior_ids)
            if expected_ids != (fight['r_total_stats_id'], fight['b_total_stats_id']):
                links.append((fight['id'], *expected_ids))
        # No bout may use results from another bout on the same date.
        for fight in fights:
            for corner in ('r', 'b'):
                identity = snapshot_key(snapshots[(fight['id'], corner, 'post')], 'post')[1:3]
                latest[identity] = ids[(fight['id'], corner)]
    return dict(inserts=inserts, updates=updates, links=links, deletes=deletes, issues=issues,
                snapshot_count=len(ids))


def write_training_view(cursor):
    """Expose pre-fight features as a regular, non-materialized view of prior posts.

    Preserve the existing column order/types. The view's id identifies the current
    fight's post row, while source_snapshot_id identifies the earlier history.
    Join exports by fight_id and corner, never by this view's id.
    """
    cursor.execute("""SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'total_fight_stats'
        ORDER BY ordinal_position""")
    columns = [row['column_name'] for row in cursor.fetchall()]
    expressions = []
    for key in columns:
        if key in COUNT_FIELDS:
            zero = sql.SQL("'0'::text") if key in ('ctrl_time', 'opponent_ctrl_time') else sql.SQL('0')
            expression = sql.SQL('CASE WHEN history.id IS NULL THEN {} ELSE history.{} END').format(
                zero, sql.Identifier(key))
        elif key in DERIVED_FEATURES:
            # These columns on the current post row describe the matchup before
            # that fight, so they are safe training features.
            expression = sql.SQL('target.{}').format(sql.Identifier(key))
        elif key == 'snapshot_type':
            expression = sql.SQL("'pre'::text")
        elif key in ('id', 'name', 'age', 'fighter_id', 'fight_id'):
            expression = sql.SQL('target.{}').format(sql.Identifier(key))
        else:
            raise ValueError(f'Unrecognized total-stat column for training view: {key}')
        expressions.append(sql.SQL('{} AS {}').format(expression, sql.Identifier(key)))
    # Adding derived columns changes the view's column order. PostgreSQL cannot
    # replace a view when existing columns would move, so recreate this helper
    # view after the table schema is updated.
    cursor.execute('DROP VIEW IF EXISTS training_fight_totals')
    cursor.execute(sql.SQL("""CREATE VIEW training_fight_totals AS
        SELECT {},
            CASE WHEN target.name = f.r_name THEN 'r'::text ELSE 'b'::text END AS corner,
            history.id AS source_snapshot_id
        FROM total_fight_stats AS target
        JOIN fights AS f ON f.id = target.fight_id
        LEFT JOIN total_fight_stats AS history ON history.id =
            CASE WHEN target.name = f.r_name THEN f.r_total_stats_id ELSE f.b_total_stats_id END
            AND history.snapshot_type = 'post'
            AND EXISTS (SELECT 1 FROM fights prior WHERE prior.id = history.fight_id AND prior.date < f.date)
        WHERE target.snapshot_type = 'post'
        """).format(sql.SQL(', ').join(expressions)))


def verify_training_view(cursor, data):
    """Check every computed training row against the raw-history replay."""
    snapshots, _ = build_snapshots(data['fights'], data['fighters'], data['fight_stats'])
    cursor.execute('SELECT * FROM training_fight_totals')
    rows = cursor.fetchall()
    if len(rows) != 2 * len(data['fights']):
        raise RuntimeError('Training view has missing or duplicate fight rows')
    for row in rows:
        expected = snapshots[(row['fight_id'], row['corner'], 'pre')]
        expected_derived = snapshots[(row['fight_id'], row['corner'], 'post')]
        if row['snapshot_type'] != 'pre' or any(
                row[key] != stored_value(key, value) for key, value in expected.items()) or any(
                row[key] != stored_value(key, value)
                for key, value in expected_derived.items() if key in DERIVED_FEATURES):
            raise RuntimeError(f"Training features changed: fight {row['fight_id']}, corner {row['corner']}")


def write_totals(cursor, plan):
    cursor.execute("ALTER TABLE total_fight_stats ADD COLUMN IF NOT EXISTS snapshot_type TEXT NOT NULL DEFAULT 'post'")
    for column in DERIVED_FEATURES:
        cursor.execute(sql.SQL(
            'ALTER TABLE total_fight_stats ADD COLUMN IF NOT EXISTS {} DOUBLE PRECISION'
        ).format(sql.Identifier(column)))
    for action in ('inserts', 'updates'):
        rows = plan[action]
        if not rows:
            continue
        columns = list(rows[0])
        query = sql.SQL('INSERT INTO total_fight_stats ({}) VALUES %s').format(
            sql.SQL(', ').join(map(sql.Identifier, columns)))
        if action == 'updates':
            query += sql.SQL(' ON CONFLICT (id) DO UPDATE SET {}').format(sql.SQL(', ').join(
                sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(key), sql.Identifier(key))
                for key in columns if key != 'id'))
        execute_values(cursor, query.as_string(cursor), [[row[key] for key in columns] for row in rows], page_size=500)
    if plan['links']:
        execute_values(cursor, '''UPDATE fights AS f SET r_total_stats_id = v.red::integer,
            b_total_stats_id = v.blue::integer FROM (VALUES %s) AS v(id, red, blue)
            WHERE f.id = v.id''', plan['links'])
    if plan['deletes']:
        cursor.execute("DELETE FROM total_fight_stats WHERE snapshot_type = 'pre' AND id = ANY(%s)",
                       (plan['deletes'],))
    cursor.execute("""DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint
                       WHERE conrelid = 'total_fight_stats'::regclass
                         AND conname = 'total_fight_stats_post_only') THEN
            ALTER TABLE total_fight_stats ADD CONSTRAINT total_fight_stats_post_only
                CHECK (snapshot_type = 'post');
        END IF;
    END $$""")
    cursor.execute("SELECT pg_get_serial_sequence('total_fight_stats', 'id') AS sequence")
    sequence = cursor.fetchone()['sequence']
    if sequence:
        cursor.execute(sql.SQL('SELECT last_value FROM {}').format(sql.Identifier(*sequence.split('.'))))
        last_value = cursor.fetchone()['last_value']
        cursor.execute('SELECT max(id) AS maximum FROM total_fight_stats')
        maximum = cursor.fetchone()['maximum'] or 1
        cursor.execute('SELECT setval(%s, %s, true)', (sequence, max(last_value, maximum)))
    write_training_view(cursor)


def repair(*, apply=False, only='all', report_path=None, backup_dir=None):
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    report_path = Path(report_path or ROOT / 'logs' / f'data-repair-{stamp}.json')
    backup_dir = Path(backup_dir or ROOT / 'backups' / 'repairs')
    with closing(connect_database()) as conn:
        conn.set_session(isolation_level='REPEATABLE READ', readonly=not apply)
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if apply:
                cursor.execute('SET LOCAL lock_timeout = \'10s\'')
                cursor.execute('LOCK TABLE fights, fight_odds, fight_stats, fighters, total_fight_stats IN SHARE ROW EXCLUSIVE MODE')
            data = read_data(cursor)
            odds, skipped = odds_plan(data) if only in ('all', 'odds') else ([], [])
            totals = totals_plan(data) if only in ('all', 'stats') else None
            cursor.execute('SELECT current_database() AS name')
            report = {'database': cursor.fetchone()['name'], 'applied': False,
                      'odds_corrections': odds, 'odds_skipped': skipped,
                      'totals': None if totals is None else {
                          'snapshots': totals['snapshot_count'], 'inserted': len(totals['inserts']),
                          'updated': len(totals['updates']), 'relinked_fights': len(totals['links']),
                          'deleted_pre_snapshots': len(totals['deletes']),
                          'identity_issues': totals['issues']}}
            if apply:
                backup_dir.mkdir(parents=True, exist_ok=True)
                backup = backup_dir / f'before-data-repair-{stamp}.json.gz'
                cursor.execute("SELECT pg_get_viewdef(to_regclass('training_fight_totals'), true) AS definition")
                view_before = cursor.fetchone()['definition']
                cursor.execute('''SELECT conname, pg_get_constraintdef(oid) AS definition
                    FROM pg_constraint WHERE conrelid = 'total_fight_stats'::regclass''')
                constraints_before = [dict(row) for row in cursor.fetchall()]
                with gzip.open(backup, 'xt') as target:
                    json.dump({'database': report['database'], 'tables': data,
                               'training_view_sql': view_before,
                               'total_stats_constraints': constraints_before}, target, default=str)
                report['backup'] = str(backup)
                if odds:
                    execute_values(cursor, '''UPDATE fights AS f SET r_fighter_odds = v.red,
                        b_fighter_odds = v.blue FROM (VALUES %s) AS v(id, red, blue)
                        WHERE f.id = v.id''', [(row['fight_id'], *row['after']) for row in odds])
                if totals is not None:
                    write_totals(cursor, totals)
                # Rebuild the plan against the transaction's writes before committing.
                after = read_data(cursor)
                if only in ('all', 'odds') and odds_plan(after)[0]:
                    raise RuntimeError('Odds verification failed; transaction will roll back')
                if totals is not None:
                    check = totals_plan(after)
                    if check['inserts'] or check['updates'] or check['links'] or check['deletes']:
                        raise RuntimeError('Totals verification failed; transaction will roll back')
                    verify_training_view(cursor, after)
            # Ensure the audit report can be written before committing any changes.
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, default=str) + '\n')
        if apply:
            report['applied'] = True
            report_path.write_text(json.dumps(report, indent=2, default=str) + '\n')
    print(json.dumps({**{key: value for key, value in report.items() if key not in ('odds_corrections', 'odds_skipped', 'totals')},
                      'odds_corrections': len(odds), 'odds_skipped': len(skipped),
                      'totals': None if totals is None else {key: value for key, value in report['totals'].items() if key != 'identity_issues'},
                      'report': str(report_path)}, indent=2))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--only', choices=('all', 'odds', 'stats'), default='all')
    parser.add_argument('--report', dest='report_path')
    parser.add_argument('--backup-dir')
    repair(**vars(parser.parse_args()))


if __name__ == '__main__':
    main()
