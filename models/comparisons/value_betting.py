"""Value betting: bet on the side with highest expected value when edge is sufficient."""
import numpy as np


def value_bet_evaluate(y, probabilities, decimal, ev_threshold=0.05):
    """Evaluate betting using expected-value-based side selection.

    For each fight, computes EV = model_prob * decimal_odds - 1 for both
    sides. Bets on the side with the highest EV, but only when that EV
    exceeds ev_threshold. This correctly identifies underdog value: the
    model may slightly favor Red, but if Blue's odds are generous enough,
    the EV on Blue can be higher.

    Args:
        y: Array of true labels (0=Red, 1=Blue).
        probabilities: Array of shape (n, 2) with predicted probabilities.
        decimal: Array of shape (n, 2) with decimal odds.
        ev_threshold: Minimum expected value to place a bet.

    Returns:
        Dict with test_accuracy, bets, betting_units, roi, skipped.
    """
    y = np.asarray(y)
    probabilities = np.asarray(probabilities)

    # EV for each side: model_prob * payout - stake
    ev = probabilities * decimal - 1  # shape (n, 2)

    # Pick the side with the best expected value
    best_side = ev.argmax(axis=1)
    best_ev = ev[np.arange(len(y)), best_side]

    bet_mask = best_ev >= ev_threshold

    if not bet_mask.any():
        return {
            'test_accuracy': 0.0, 'bets': 0, 'betting_units': 0.0,
            'roi': 0.0, 'skipped': len(y),
        }

    selected_decimal = decimal[np.arange(len(y)), best_side]
    profits = np.where(best_side == y, selected_decimal - 1, -1)

    bet_profits = profits[bet_mask]
    bet_correct = (best_side[bet_mask] == y[bet_mask]).sum()

    return {
        'test_accuracy': float(bet_correct / bet_mask.sum()),
        'bets': int(bet_mask.sum()),
        'betting_units': float(bet_profits.sum()),
        'roi': float(bet_profits.mean()),
        'skipped': int((~bet_mask).sum()),
    }


def prediction_filtered_evaluate(y, probabilities, decimal, ev_threshold=0.05):
    """Bet on the model's predicted winner, but only when that side has positive EV.

    Unlike value_bet_evaluate which picks the highest-EV side (often underdogs),
    this bets on the model's argmax prediction and filters by EV. This
    leverages the model's classification accuracy while ensuring value.
    """
    y = np.asarray(y)
    probabilities = np.asarray(probabilities)

    predicted = probabilities.argmax(axis=1)  # model's pick
    ev_for_pick = probabilities[np.arange(len(y)), predicted] * decimal[np.arange(len(y)), predicted] - 1
    bet_mask = ev_for_pick >= ev_threshold

    if not bet_mask.any():
        return {
            'test_accuracy': 0.0, 'bets': 0, 'betting_units': 0.0,
            'roi': 0.0, 'skipped': len(y),
        }

    selected_decimal = decimal[np.arange(len(y)), predicted]
    profits = np.where(predicted == y, selected_decimal - 1, -1)

    bet_profits = profits[bet_mask]
    bet_correct = (predicted[bet_mask] == y[bet_mask]).sum()

    return {
        'test_accuracy': float(bet_correct / bet_mask.sum()),
        'bets': int(bet_mask.sum()),
        'betting_units': float(bet_profits.sum()),
        'roi': float(bet_profits.mean()),
        'skipped': int((~bet_mask).sum()),
    }


def kelly_evaluate(y, probabilities, decimal, fraction=0.25, min_ev=0.0):
    """Evaluate with Kelly-proportional bet sizing.

    Bet size = fraction * (model_prob * odds - 1) / (odds - 1) for
    the highest-EV side. Profits scale with bet size.

    Args:
        fraction: Kelly fraction (0.25 = quarter-Kelly for safety).
        min_ev: Minimum EV to place a bet.
    """
    y = np.asarray(y)
    probabilities = np.asarray(probabilities)

    ev = probabilities * decimal - 1
    best_side = ev.argmax(axis=1)
    best_ev = ev[np.arange(len(y)), best_side]
    best_prob = probabilities[np.arange(len(y)), best_side]
    best_odds = decimal[np.arange(len(y)), best_side]

    kelly_fraction = (best_prob * best_odds - 1) / (best_odds - 1)
    bet_size = np.maximum(0, fraction * kelly_fraction)

    bet_mask = (best_ev >= min_ev) & (bet_size > 0)

    if not bet_mask.any():
        return {
            'bets': 0, 'betting_units': 0.0, 'total_wagered': 0.0,
            'roi': 0.0, 'skipped': len(y),
        }

    selected_decimal = decimal[np.arange(len(y)), best_side]
    raw_profits = np.where(best_side == y, selected_decimal - 1, -1)
    sized_profits = raw_profits * bet_size

    bet_profits = sized_profits[bet_mask]

    return {
        'bets': int(bet_mask.sum()),
        'betting_units': float(bet_profits.sum()),
        'total_wagered': float(bet_size[bet_mask].sum()),
        'roi': float(bet_profits.sum() / bet_size[bet_mask].sum()) if bet_size[bet_mask].sum() > 0 else 0.0,
        'skipped': int((~bet_mask).sum()),
        'avg_bet_size': float(bet_size[bet_mask].mean()),
    }


def kelly_filtered_evaluate(y, probabilities, decimal, ev_threshold=0.05,
                             fraction=0.25):
    """EV-filtered bets with Kelly-proportional sizing.

    First filters to bets where best-side EV >= ev_threshold, then sizes
    each bet using Kelly: size = fraction * (prob * odds - 1) / (odds - 1).
    This combines selectivity (only bet when edge is real) with proportional
    sizing (bet more when edge is large).
    """
    y = np.asarray(y)
    probabilities = np.asarray(probabilities)

    ev = probabilities * decimal - 1
    best_side = ev.argmax(axis=1)
    best_ev = ev[np.arange(len(y)), best_side]
    best_prob = probabilities[np.arange(len(y)), best_side]
    best_odds = decimal[np.arange(len(y)), best_side]

    kelly_frac = (best_prob * best_odds - 1) / (best_odds - 1)
    bet_size = np.maximum(0, fraction * kelly_frac)

    bet_mask = (best_ev >= ev_threshold) & (bet_size > 0)

    if not bet_mask.any():
        return {
            'bets': 0, 'betting_units': 0.0, 'total_wagered': 0.0,
            'roi': 0.0, 'skipped': len(y), 'avg_bet_size': 0.0,
        }

    selected_decimal = decimal[np.arange(len(y)), best_side]
    raw_profits = np.where(best_side == y, selected_decimal - 1, -1)
    sized_profits = raw_profits * bet_size

    bet_profits = sized_profits[bet_mask]
    wagered = bet_size[bet_mask].sum()

    return {
        'bets': int(bet_mask.sum()),
        'betting_units': float(bet_profits.sum()),
        'total_wagered': float(wagered),
        'roi': float(bet_profits.sum() / wagered) if wagered > 0 else 0.0,
        'skipped': int((~bet_mask).sum()),
        'avg_bet_size': float(bet_size[bet_mask].mean()),
    }


def optimize_threshold(y, probabilities, decimal, thresholds=None):
    """Find the EV threshold that maximizes betting units.

    Args:
        y: Array of true labels.
        probabilities: Array of shape (n, 2).
        decimal: Array of shape (n, 2).
        thresholds: List of thresholds to try.

    Returns:
        (best_threshold, best_metrics) tuple.
    """
    if thresholds is None:
        thresholds = [0.0, 0.02, 0.05, 0.08, 0.10, 0.12, 0.15,
                      0.18, 0.20, 0.25, 0.30, 0.40, 0.50]

    best_threshold = 0.0
    best_metrics = value_bet_evaluate(y, probabilities, decimal, 0.0)

    for t in thresholds:
        metrics = value_bet_evaluate(y, probabilities, decimal, t)
        if metrics['betting_units'] > best_metrics['betting_units']:
            best_threshold = t
            best_metrics = metrics

    return best_threshold, best_metrics
