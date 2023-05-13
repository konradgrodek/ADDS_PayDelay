import pandas as pd
import math

from lib.input_const import *

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa

from typing import Optional
from typing import Union


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

    def count_stories(self) -> int:
        return self.stories().num_rows

    def story_length_mean(self) -> float:
        return pc.mean(self.stories().column(PaymentStoriesColumns.PaymentsCount.name)).as_py()

    def story_duration_mean(self) -> float:
        return pc.mean(self.stories().column(PaymentStoriesColumns.Duration.name)).as_py()

    def stories_per_legal_entity(self) -> float:
        _entities = pc.count_distinct(self.stories().column(PaymentStoriesColumns.EntityId.name)).as_py()
        return None if _entities == 0 else self.count_stories() / _entities

    def risk_rate(self, actual_col: str) -> float:
        return pc.sum(self.stories().column(actual_col)).as_py() / self.count_stories()

    def _predictor(self, predictor_col: str,
                   actual_col: str = None) -> Union[pa.ChunkedArray, tuple[pa.ChunkedArray, pa.ChunkedArray]]:
        if predictor_col in (PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name,
                             PaymentStoriesColumns.TendencyCoefficient_ForDelay.name):
            _filtered = self.stories().filter(
                pc.field(predictor_col).is_valid()
                & (pc.field(PaymentStoriesColumns.PaymentsCount.name) > 2)
                & ~pc.field(predictor_col).is_nan()
            )
            _pred = pc.negate(_filtered.column(predictor_col))
            return (_pred, _filtered.column(actual_col)) if actual_col is not None else _pred

        _filtered = self.stories().filter(pc.field(predictor_col).is_valid() & ~pc.field(predictor_col).is_nan() & (pc.field(PaymentStoriesColumns.PaymentsCount.name) > 1))
        return (_filtered.column(predictor_col), _filtered.column(actual_col)) if actual_col is not None \
            else _filtered.column(predictor_col)

    def confusion_matrix(self, predictor_col: str,
                         threshold: float, actual_col: str) -> tuple[tuple[int, int], tuple[int, int]]:
        _cached_cm_per_column = self._confusion_matrices.get(predictor_col+actual_col)
        if _cached_cm_per_column is None:
            _cached_cm_per_column = {}
            self._confusion_matrices[predictor_col+actual_col] = _cached_cm_per_column
        if threshold not in _cached_cm_per_column:
            _predictor, _actual = self._predictor(predictor_col, actual_col)
            _pred = pc.greater(_predictor, threshold)
            _tp = pc.sum(pc.and_(_pred, _actual)).as_py()
            _tn = pc.sum(pc.and_(pc.invert(_pred), pc.invert(_actual))).as_py()
            _fp = pc.sum(pc.and_(_pred, pc.invert(_actual))).as_py()
            _fn = pc.sum(pc.and_(pc.invert(_pred), _actual)).as_py()
            _cached_cm_per_column[threshold] = \
                (_tp if _tp is not None else 0, _fn if _fn is not None else 0), \
                (_fp if _fp is not None else 0, _tn if _tn is not None else 0)
        return _cached_cm_per_column[threshold]

    def _true_positives(self, predictor_col: str, threshold: float, actual_col: str):
        return self.confusion_matrix(predictor_col, threshold, actual_col)[0][0]

    def _true_negatives(self, predictor_col: str, threshold: float, actual_col: str):
        return self.confusion_matrix(predictor_col, threshold, actual_col)[1][1]

    def _false_positives(self, predictor_col: str, threshold: float, actual_col: str):
        return self.confusion_matrix(predictor_col, threshold, actual_col)[1][0]

    def _false_negatives(self, predictor_col: str, threshold: float, actual_col: str):
        return self.confusion_matrix(predictor_col, threshold, actual_col)[0][1]

    def _actual_positive_count(self, predictor_col: str, actual_col: str) -> int:
        return pc.sum(self._predictor(predictor_col, actual_col)[1]).as_py()

    def _actual_negative_count(self, predictor_col: str, actual_col: str) -> int:
        return pc.sum(pc.invert(self._predictor(predictor_col, actual_col)[1])).as_py()

    def true_positive_rate(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        """
        TPR | Recall | Sensitivity
        :param predictor_col:
        :param threshold:
        :param actual_col:
        :return:
        """
        _p = self._actual_positive_count(predictor_col, actual_col)
        return 0 if _p is None or _p == 0 else self._true_positives(predictor_col, threshold, actual_col) / _p

    recall = true_positive_rate
    sensitivity = true_positive_rate

    def false_positive_rate(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        _n = self._actual_negative_count(predictor_col, actual_col)
        return 0 if _n is None or _n == 0 else self._false_positives(predictor_col, threshold, actual_col) / _n

    def accuracy(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        """
        ACC = tp + tn / N
        It will not work for this case - it is highly inbalanced
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

    def precision_recall_auc(self, predictor_col: str, actual_col: str):
        raise NotImplementedError()

    def predictor_min(self, predictor_col: str) -> float:
        return pc.min(self._predictor(predictor_col)).as_py()

    def predictor_max(self, predictor_col: str) -> float:
        return pc.max(self._predictor(predictor_col)).as_py()

    def predictor_mean(self, predictor_col: str) -> float:
        return pc.mean(self._predictor(predictor_col)).as_py()

    def predictor_median(self, predictor_col: str) -> float:
        return pc.approximate_median(self._predictor(predictor_col)).as_py()

    def predictor_stddev(self, predictor_col: str) -> float:
        return pc.stddev(self._predictor(predictor_col)).as_py()

    def predictor_vcount(self, predictor_col: str) -> float:
        return pc.count(self._predictor(predictor_col)).as_py()

    def _default_threshold_min(self, predictor_col: str):
        _mean = self.predictor_mean(predictor_col)
        _stddev = self.predictor_stddev(predictor_col)
        return _mean - 3 * _stddev \
            if _mean is not None and not math.isnan(_mean) and _stddev is not None and not math.isnan(_stddev) else 1

    def _default_threshold_max(self, predictor_col: str):
        _mean = self.predictor_mean(predictor_col)
        _stddev = self.predictor_stddev(predictor_col)
        return _mean + 3 * _stddev \
            if _mean is not None and not math.isnan(_mean) and _stddev is not None and not math.isnan(_stddev) else 1

    def roc_curve(self, predictor_col: str, actual_col: str,
                  threshold_min: float = None, threshold_max: float = None, steps: int = 100) -> pd.DataFrame:
        if threshold_min is None:
            threshold_min = self._default_threshold_min(predictor_col)
        if threshold_max is None:
            threshold_max = self._default_threshold_max(predictor_col)

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

    def roc_auc(self, predictor_col: str, actual_col: str, sampling: int = 100):
        _roc = self.roc_curve(predictor_col=predictor_col, actual_col=actual_col, steps=sampling)
        _fpr = list(reversed(_roc[_roc.columns[0]].values))
        _tpr = list(reversed(_roc[_roc.columns[1]].values))
        return sum([
            (_fpr_n - _fpr_p) * (_tpr_p + _tpr_n) / 2
            for _fpr_p, _fpr_n, _tpr_p, _tpr_n in zip(_fpr[:-1], _fpr[1:], _tpr[:-1], _tpr[1:])
        ])

    def f1_score(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        _p = self.precision(predictor_col, threshold, actual_col)
        _r = self.recall(predictor_col, threshold, actual_col)
        return None if _p is None or _r is None or _p + _r == 0 else 2 * _p * _r / (_p + _r)

    def f1_curve(self, predictor_col: str, actual_col: str, threshold_min: float = None,
                 threshold_max: float = None, steps: int = 100) -> pd.DataFrame:
        if threshold_min is None:
            threshold_min = self._default_threshold_min(predictor_col)
        if threshold_max is None:
            threshold_max = self._default_threshold_max(predictor_col)

        _thresholds = [threshold_min + _i*(threshold_max - threshold_min)/steps for _i in range(steps+1)]
        f1s = [self.f1_score(predictor_col=predictor_col, threshold=_th, actual_col=actual_col) for _th in _thresholds]
        return pd.DataFrame({"F1 Score": f1s}, index=_thresholds)

    def f1_max(self, predictor_col: str, actual_col: str, threshold_min: float = None,
               threshold_max: float = None, min_precision=0.001) -> tuple[float, float]:
        if threshold_min is None:
            threshold_min = self._default_threshold_min(predictor_col)
        if threshold_max is None:
            threshold_max = self._default_threshold_max(predictor_col)

        _steps = 100
        _step = (threshold_max - threshold_min) / _steps
        _thresholds = [threshold_min + _i*_step for _i in range(_steps+1)]
        f1s = [(self.f1_score(predictor_col, _th, actual_col), _th) for _th in _thresholds]
        f1m = f1s[0][0]
        thm = f1s[0][1]
        for f1, th in f1s:
            if f1 is not None and f1 > f1m:
                f1m = f1
                thm = th
        if _step <= min_precision:
            return f1m, thm
        return self.f1_max(predictor_col, actual_col, thm, thm+_step, min_precision)

    def accuracy_curve(self, predictor_col: str, threshold_min: float, threshold_max: float,
                       steps: int, actual_col: str) -> pd.DataFrame:
        _thresholds = [threshold_min + _i*(threshold_max - threshold_min)/steps for _i in range(steps+1)]
        f1s = [self.accuracy(predictor_col=predictor_col, threshold=_th, actual_col=actual_col) for _th in _thresholds]
        return pd.DataFrame({"Accuracy": f1s}, index=_thresholds)

    def cohen_kappa(self, predictor_col: str, threshold: float, actual_col: str) -> float:
        _p = self.accuracy(predictor_col, threshold, actual_col)
        _p_random = self.risk_rate(actual_col)
        return (_p - _p_random) / (1 - _p_random)

    def cohen_kappa_curve(self, predictor_col: str, actual_col: str, threshold_min: float = None,
                          threshold_max: float = None, steps: int = 100) -> pd.DataFrame:
        if threshold_min is None:
            threshold_min = self._default_threshold_min(predictor_col)
        if threshold_max is None:
            threshold_max = self._default_threshold_max(predictor_col)

        _thresholds = [threshold_min + _i*(threshold_max - threshold_min)/steps for _i in range(steps+1)]
        cp = [
            self.cohen_kappa(predictor_col=predictor_col, threshold=_th, actual_col=actual_col)
            for _th in _thresholds
        ]
        return pd.DataFrame({"Cohen-Kappa": cp}, index=_thresholds)

    def cohen_kappa_max(self, predictor_col: str, actual_col: str, threshold_min: float = None,
                        threshold_max: float = None, min_precision=0.001) -> tuple[float, float]:
        if threshold_min is None:
            threshold_min = self._default_threshold_min(predictor_col)
        if threshold_max is None:
            threshold_max = self._default_threshold_max(predictor_col)

        _steps = 100
        _step = (threshold_max - threshold_min) / _steps
        _thresholds = [threshold_min + _i*_step for _i in range(_steps+1)]
        cps = [(self.cohen_kappa(predictor_col, _th, actual_col), _th) for _th in _thresholds]
        cpm = cps[0][0]
        thm = cps[0][1]
        for cp, th in cps:
            if cp > cpm:
                cpm = cp
                thm = th
        if _step <= min_precision:
            return cpm, thm
        return self.cohen_kappa_max(predictor_col, actual_col, thm, thm+_step, min_precision)
