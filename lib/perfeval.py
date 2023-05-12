import pandas as pd

from lib.input_const import *

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa

from typing import Optional


class PaymentStoriesPerformanceEvaluator:

    def __init__(self, source_file: Path, codename: str):
        self._file = source_file
        self.source_codename = codename
        self._stories: Optional[pa.Table] = None
        self._confusion_matrices = {}

    def stories(self) -> pa.Table:
        if self._stories is None:
            self._stories = pq.read_table(self._file)
        return self._stories

    def confusion_matrix(self, predictor_col: str,
                         threshold: float, actual_col: str) -> tuple[tuple[int, int], tuple[int, int]]:
        _cached_cm_per_column = self._confusion_matrices.get(predictor_col+actual_col)
        if _cached_cm_per_column is None:
            _cached_cm_per_column = {}
            self._confusion_matrices[predictor_col+actual_col] = _cached_cm_per_column
        if threshold not in _cached_cm_per_column:
            _predicator = self.stories().column(predictor_col)
            _actual = self.stories().column(actual_col)
            _pred = pc.greater(_predicator, threshold)
            _tp = pc.sum(pc.and_(_pred, _actual)).as_py()
            _tn = pc.sum(pc.and_(pc.invert(_pred), pc.invert(_actual))).as_py()
            _fp = pc.sum(pc.and_(_pred, pc.invert(_actual))).as_py()
            _fn = pc.sum(pc.and_(pc.invert(_pred), _actual)).as_py()
            _cached_cm_per_column[threshold] = (_tp, _fn), (_fp, _tn)
        return _cached_cm_per_column[threshold]

    def _true_positives(self, predictor_col: str, threshold: float, actual_col: str):
        return self.confusion_matrix(predictor_col, threshold, actual_col)[0][0]

    def _true_negatives(self, predictor_col: str, threshold: float, actual_col: str):
        return self.confusion_matrix(predictor_col, threshold, actual_col)[1][1]

    def _false_positives(self, predictor_col: str, threshold: float, actual_col: str):
        return self.confusion_matrix(predictor_col, threshold, actual_col)[1][0]

    def _false_negatives(self, predictor_col: str, threshold: float, actual_col: str):
        return self.confusion_matrix(predictor_col, threshold, actual_col)[0][1]

    def _actual_positive_count(self, actual_col: str) -> int:
        return pc.sum(self.stories().column(actual_col)).as_py()

    def _actual_negative_count(self, actual_col: str) -> int:
        return pc.sum(pc.invert(self.stories().column(actual_col))).as_py()

    def true_positive_rate(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        """
        TPR | Recall | Sensitivity
        :param predictor_col:
        :param threshold:
        :param actual_col:
        :return:
        """
        _p = self._actual_positive_count(actual_col)
        return None if _p == 0 else self._true_positives(predictor_col, threshold, actual_col) / _p

    recall = true_positive_rate
    sensitivity = true_positive_rate

    def false_positive_rate(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        _n = self._actual_negative_count(actual_col)
        return None if _n == 0 else self._false_positives(predictor_col, threshold, actual_col) / _n

    def accuracy(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        """
        ACC = tp + tn / N
        I will not use it, this is just to have full stack of important scores.
        As the pay-delay-to-debt problem is strongly imbalanced,
        it is easy to get high accuracy (because of high tp or tn), but it does not say much
        :param predictor_col:
        :param threshold:
        :param actual_col:
        :return:
        """
        _pn = self.stories().num_rows
        return None if _pn == 0 else \
            (self._true_positives(predictor_col, threshold, actual_col) +
             self._true_negatives(predictor_col, threshold, actual_col)) / _pn

    def precision(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        _pp = self._true_positives(predictor_col, threshold, actual_col) + \
              self._false_positives(predictor_col, threshold, actual_col)
        return None if _pp == 0 else self._true_positives(predictor_col, threshold, actual_col) / _pp

    def f1_score(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        _p = self.precision(predictor_col, threshold, actual_col)
        _r = self.recall(predictor_col, threshold, actual_col)
        return None if _p + _r == 0 else 2 * _p * _r / (_p + _r)

    def roc_auc(self, predictor_col: str, actual_col: str):
        pass

    def cohen_kappa(self):
        pass

    def precision_recall_auc(self, predictor_col: str, actual_col: str):
        pass

    def _predictor(self, preditor_col: str) -> pa.ChunkedArray:
        if preditor_col in (PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name, PaymentStoriesColumns.TendencyCoefficient_ForDelay.name):
            return self.stories().filter(
                pc.field(preditor_col).is_valid()
                & (pc.field(PaymentStoriesColumns.PaymentsCount.name) > 2)
                & ~pc.field(preditor_col).is_nan()
            ).column(preditor_col)

        return self.stories().filter(pc.field(preditor_col).is_valid()).column(preditor_col)

    def predictor_min(self, predictor_col: str) -> float:
        return pc.min(self._predictor(predictor_col)).as_py()

    def predictor_max(self, predictor_col: str) -> float:
        return pc.max(self._predictor(predictor_col)).as_py()

    def predictor_mean(self, predictor_col: str) -> float:
        return pc.mean(self._predictor(predictor_col)).as_py()

    def predictor_median(self, predictor_col: str) -> float:
        return pc.approximate_median(self._predictor(predictor_col)).as_py()

    def predictor_vcount(self, predictor_col: str) -> float:
        return pc.count(self._predictor(predictor_col)).as_py()

    def roc_curve(self, predictor_col: str, threshold_min: float, threshold_max: float, steps: int, actual_col: str) -> pd.DataFrame:
        _thresholds = [threshold_min + _i*(threshold_max - threshold_min)/steps for _i in range(steps+1)]
        _tpr = [
            self.recall(predictor_col=predictor_col, threshold=_th, actual_col=actual_col) for _th in _thresholds
        ]
        _fpr = [
            self.false_positive_rate(
                predictor_col=predictor_col, threshold=_th, actual_col=actual_col
            ) for _th in _thresholds
        ]
        return pd.DataFrame({"False Positive Rate": _fpr, "True Positive Rate": _tpr}, index=_thresholds)
