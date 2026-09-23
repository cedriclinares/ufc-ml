"""Rebuild historical features without including the fight being predicted.

    python scripts/make_total_stats.py           # preview
    python scripts/make_total_stats.py --apply   # back up, rebuild, verify, commit

Only POST-fight snapshots are stored. fights.r_total_stats_id/b_total_stats_id
reference the latest earlier-date snapshot (NULL for debuts). Use the ordinary
training_fight_totals view for current-fight age and zero debut history.
Every run replays raw history, including backfilled bouts; reruns are idempotent.
The rebuild also fills matchup ratio and red-minus-blue difference columns on
each post-fight snapshot for use by the training_fight_totals view.
"""
import argparse

try:
    from .repair_fight_data import repair
except ImportError:
    from repair_fight_data import repair


def create_cumulative_fight_data(*, apply=False):
    return repair(apply=apply, only='stats')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    create_cumulative_fight_data(**vars(parser.parse_args()))
