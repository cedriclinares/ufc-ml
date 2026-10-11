import unittest

from scripts.evaluate_finish_decision import binary_metrics


class FinishDecisionTests(unittest.TestCase):
    def test_distance_and_finish_metrics(self):
        result = binary_metrics([0, 0, 1, 1], [.1, .8, .5, .9])
        self.assertEqual(result['accuracy'], .75)
        self.assertEqual(result['balanced_accuracy'], .75)
        self.assertEqual(result['finish_recall'], .5)
        self.assertEqual(result['distance_recall'], 1)
        self.assertEqual(result['confusion_matrix_finish_distance'], [[1, 1], [0, 2]])
        self.assertAlmostEqual(result['distance_precision'], 2/3)


if __name__ == '__main__':
    unittest.main()
