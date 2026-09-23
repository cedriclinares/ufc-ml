"""Value betting: only bet when model probability exceeds market-implied probability."""
import numpy as np


def value_bet_evaluate(y, probabilities, decimal, edge_threshold=0.05):
    """Evaluate betting with value-based filtering.

    Only places a bet when the model's predicted probability for the
    chosen side exceeds the market-implied probability by at least
    edge_threshold.

    Args:
        y: Array of true labels (0=Red, 1=Blue).
        probabilities: Array of shape (n, 2) with predicted probabilities.
        decimal: Array of shape (n, 2) with decimal odds.
        edge_threshold: Minimum edge required to place a bet.

    Returns:
        Dict with test_accuracy, bets, betting_units, roi, skipped.
    """
    y = np.asarray(y)
    probabilities = np.asarray(probabilities)

    implied = 1 / decimal
    market = implied / implied.sum(axis=1, keepdims=True)

    predicted = probabilities.argmax(axis=1)  # Red wins exact ties
    model_prob = probabilities[np.arange(len(y)), predicted]
    market_prob = market[np.arange(len(y)), predicted]
    edge = model_prob - market_prob

    bet_mask = edge >= edge_threshold

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


def optimize_threshold(y, probabilities, decimal, thresholds=None):
    """Find the edge threshold that maximizes betting units.

    Args:
        y: Array of true labels.
        probabilities: Array of shape (n, 2).
        decimal: Array of shape (n, 2).
        thresholds: List of thresholds to try.

    Returns:
        (best_threshold, best_metrics) tuple.
    """
    if thresholds is None:
        thresholds = [0.0, 0.02, 0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25, 0.30]

    best_threshold = 0.0
    best_metrics = value_bet_evaluate(y, probabilities, decimal, 0.0)

    for t in thresholds:
        metrics = value_bet_evaluate(y, probabilities, decimal, t)
        if metrics['betting_units'] > best_metrics['betting_units']:
            best_threshold = t
            best_metrics = metrics

    return best_threshold, best_metrics
