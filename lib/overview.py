from lib.input_const import PayDelayColumns, MALE, FEMALE, OUTLIER__MAX_DELAY, OUTLIER__MIN_DELAY
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path
from typing import Optional
import pandas as pd
from functools import cache


class PayDelayStatistics:

    MIN_AGE = 18
    MAX_AGE = 100

    MIN_AMOUNT = 1
    MAX_AMOUNT = 10000

    REPORT_RECORD_COUNT_ALL = "records-count-all"
    REPORT_RECORD_COUNT_WO_OUTLIERS = "records-count-wo-outliers"
    REPORT_ENTITIES_COUNT = "entities-count"
    REPORT_ENTITIES_WITH_LATER_RS4 = "entities-with-later-severe-debt"
    REPORT_SOCDEM_GENDER_RATIO = 'sociodemographic-m-w-ratio'
    REPORT_SOCDEM_UNKNOWN_GENDER_RATIO = 'sociodemographic-unknown-gender-ratio'
    REPORT_SOCDEM_AGE_MEAN = 'sociodemographic-age-mean'
    REPORT_SOCDEM_AGE_STDDEV = 'sociodemographic-age-stddev'
    REPORT_SOCDEM_AGE_SKEWNESS = 'sociodemographic-age-skewness'
    REPORT_AMOUNT_COUNT_UNKNOWN = 'amount-count-unknown'
    REPORT_AMOUNT_COUNT_TOO_HIGH = 'amount-count-too-high'
    REPORT_AMOUNT_MEAN = 'amount-mean'
    REPORT_AMOUNT_STDDEV = 'amount-stddev'
    REPORT_PAYMENT_DAYSDIFF_MEAN = 'payment-days-diff-mean'
    REPORT_PAYMENT_DAYSDIFF_STDDEV = 'payment-days-diff-stddev'
    REPORT_PREPAID_DAYSDIFF_MEAN = 'payment-prepaid-days-mean'
    REPORT_PREPAID_DAYSDIFF_STDDEV = 'payment-prepaid-days-stddev'
    REPORT_PREPAID_DAYSDIFF_COUNT = 'payment-prepaid-days-count'
    REPORT_DELAYED_DAYSDIFF_MEAN = 'payment-delayed-days-mean'
    REPORT_DELAYED_DAYSDIFF_STDDEV = 'payment-delayed-days-stddev'
    REPORT_DELAYED_DAYSDIFF_COUNT = 'payment-delayed-days-count'
    REPORT_PAYMENT_ONTIME_COUNT = 'payment-on-time-count'

    ROC_TPR = 'sensitivity'
    ROC_FPR = '1 - specifity'

    def __init__(self, source_file: Path, codename: str):
        self._file = source_file
        self.source_codename = codename
        self._content: Optional[pa.Table] = None

    def content(self, wo_outliers=True) -> pa.Table:
        if self._content is None:
            self._content = pq.read_table(self._file)
        return self._content.filter(pc.field(PayDelayColumns.IsOutlier.name) == False) if wo_outliers else self._content

    @cache
    def count_rows(self) -> int:
        return self.content(wo_outliers=False).num_rows

    def count_rows_wo_outliers(self) -> int:
        return self.content().num_rows

    def count_outliers_min_delay(self) -> int:
        return self.content(wo_outliers=False).filter(
            (pc.field(PayDelayColumns.DelayDays.name) < OUTLIER__MIN_DELAY)
        ).num_rows

    def count_outliers_max_delay(self) -> int:
        return self.content(wo_outliers=False).filter(
            (pc.field(PayDelayColumns.DelayDays.name) > OUTLIER__MAX_DELAY)
        ).num_rows

    def count_entities(self) -> int:
        return len(self.content().column(PayDelayColumns.EntityId.name).value_counts())

    def count_entities_with_later_debt_rc4(self) -> int:
        return len(
            self.content()
                .filter(pc.field(PayDelayColumns.LaterDebtsMaxCreditStatus.name) == 4)
                .column(PayDelayColumns.EntityId.name)
                .value_counts()
        )

    @cache
    def measure_age_stats(self) -> tuple[float, float, float]:
        _ages = self.content().filter(
            (pc.field(PayDelayColumns.Age.name) >= self.MIN_AGE) &
            (pc.field(PayDelayColumns.Age.name) <= self.MAX_AGE)
        ).group_by(PayDelayColumns.EntityId.name)\
            .aggregate([(PayDelayColumns.Age.name, 'min')])\
            .column(f"{PayDelayColumns.Age.name}_min")

        _mean = pc.mean(_ages).as_py()
        _stddev = pc.stddev(_ages).as_py()
        _skewness = (_mean - pc.mode(_ages)[0]['mode'].as_py()) / _stddev if _stddev != 0 else 0

        return _mean, _stddev, _skewness

    @cache
    def measure_gender_ratio(self) -> tuple:
        _counted = self.content()\
            .group_by(PayDelayColumns.Sex.name)\
            .aggregate([(PayDelayColumns.EntityId.name, 'count_distinct')])\
            .to_pandas()

        _men, _women, _unknown = 0, 0, 0
        for _count in _counted.itertuples(index=False):
            if _count[1] == MALE:
                _men = _count[0]
            elif _count[1] == FEMALE:
                _women = _count[0]
            else:
                _unknown = _count[0]

        return _men / _women if _men*_women != 0 else None, \
            _unknown / sum([_men, _women, _unknown]) if _unknown > 0 else None

    @cache
    def measure_amount_stats(self) -> tuple:
        _amount_valid = self.content().filter(
            (pc.field(PayDelayColumns.InvoicedAmount.name) >= self.MIN_AMOUNT) &
            (pc.field(PayDelayColumns.InvoicedAmount.name) <= self.MAX_AMOUNT))\
            .column(PayDelayColumns.InvoicedAmount.name)
        _amount_unknown = self.content().filter(
            (pc.field(PayDelayColumns.InvoicedAmount.name) < self.MIN_AMOUNT))
        _amount_too_high = self.content().filter(
            (pc.field(PayDelayColumns.InvoicedAmount.name) > self.MAX_AMOUNT))

        _mean = pc.mean(_amount_valid).as_py()
        _stddev = pc.stddev(_amount_valid).as_py()
        _cnt_unknown = _amount_unknown.num_rows
        _cnt_too_high = _amount_too_high.num_rows

        return _mean, _stddev, _cnt_unknown, _cnt_too_high

    @cache
    def measure_payment_daysdiff_stats(self) -> tuple:
        _days_difference = self.content().column(PayDelayColumns.DelayDays.name)

        _mean = pc.mean(_days_difference).as_py()
        _stddev = pc.stddev(_days_difference).as_py()

        return _mean, _stddev

    @cache
    def measure_delayed_daysdiff_stats(self) -> tuple:
        _days_difference = self.content().filter(pc.field(PayDelayColumns.DelayDays.name) > 0)\
            .column(PayDelayColumns.DelayDays.name)

        _mean = pc.mean(_days_difference).as_py()
        _stddev = pc.stddev(_days_difference).as_py()
        _count = _days_difference.length()

        return _mean, _stddev, _count

    @cache
    def measure_prepaid_daysdiff_stats(self) -> tuple:
        _days_difference = self.content().filter(pc.field(PayDelayColumns.DelayDays.name) < 0)\
            .column(PayDelayColumns.DelayDays.name)

        _mean = pc.mean(_days_difference).as_py()
        _stddev = pc.stddev(_days_difference).as_py()
        _count = _days_difference.length()

        return _mean, _stddev, _count

    def count_ontime_payments(self) -> int:
        return self.content().filter(pc.field(PayDelayColumns.DelayDays.name) == 0).num_rows

    @cache
    def calculate_roc_positive_payments(self, risk_class: int, positive_if_no_debt_within_years: int) -> pd.DataFrame:
        _positive_if_no_debt_within_days = positive_if_no_debt_within_years * 365
        _later_debt_colname = PayDelayColumns.LaterDebtsMinDaysToValidFrom(risk_class).name
        _positive_payments = self.content()\
            .filter(pc.field(PayDelayColumns.DelayDays.name) <= 0)\
            .select([PayDelayColumns.DelayDays.name, _later_debt_colname])

        roc = {self.ROC_FPR: list(), self.ROC_TPR: list()}
        thresholds = list(range(0, abs(OUTLIER__MIN_DELAY)+1))
        for threshold in thresholds:
            positive = _positive_payments.filter(
                pc.field(_later_debt_colname).is_null() |
                (pc.field(_later_debt_colname) > _positive_if_no_debt_within_days)
            ).num_rows
            true_positive = _positive_payments.filter(
                (pc.abs(pc.field(PayDelayColumns.DelayDays.name)) >= threshold) &
                (pc.field(_later_debt_colname).is_null() |
                (pc.field(_later_debt_colname) > _positive_if_no_debt_within_days))
            ).num_rows
            false_positive = _positive_payments.filter(
                (pc.abs(pc.field(PayDelayColumns.DelayDays.name)) >= threshold) &
                (pc.field(_later_debt_colname) <= _positive_if_no_debt_within_days)
            ).num_rows
            negative = _positive_payments.filter(
                pc.field(_later_debt_colname) <= _positive_if_no_debt_within_days
            ).num_rows
            roc[self.ROC_FPR].append(false_positive / negative if negative != 0 else 0)
            roc[self.ROC_TPR].append(true_positive / positive if positive != 0 else 0)

        return pd.DataFrame(roc, index=thresholds)

    def report(self) -> pd.DataFrame:
        return pd.DataFrame({
            self.REPORT_RECORD_COUNT_ALL: [self.count_rows()],
            self.REPORT_RECORD_COUNT_WO_OUTLIERS: [self.count_rows_wo_outliers()],
            self.REPORT_ENTITIES_COUNT: [self.count_entities()],
            self.REPORT_ENTITIES_WITH_LATER_RS4: [self.count_entities_with_later_debt_rc4()],
            self.REPORT_SOCDEM_AGE_MEAN: [self.measure_age_stats()[0]],
            self.REPORT_SOCDEM_AGE_STDDEV: [self.measure_age_stats()[1]],
            self.REPORT_SOCDEM_AGE_SKEWNESS: [self.measure_age_stats()[2]],
            self.REPORT_SOCDEM_GENDER_RATIO: [self.measure_gender_ratio()[0]],
            self.REPORT_SOCDEM_UNKNOWN_GENDER_RATIO: [self.measure_gender_ratio()[1]],
            self.REPORT_AMOUNT_MEAN: [self.measure_amount_stats()[0]],
            self.REPORT_AMOUNT_STDDEV: [self.measure_amount_stats()[1]],
            self.REPORT_AMOUNT_COUNT_UNKNOWN: [self.measure_amount_stats()[2]],
            self.REPORT_AMOUNT_COUNT_TOO_HIGH: [self.measure_amount_stats()[3]],
            self.REPORT_PAYMENT_DAYSDIFF_MEAN: [self.measure_payment_daysdiff_stats()[0]],
            self.REPORT_PAYMENT_DAYSDIFF_STDDEV: [self.measure_payment_daysdiff_stats()[1]],
            self.REPORT_DELAYED_DAYSDIFF_MEAN: [self.measure_delayed_daysdiff_stats()[0]],
            self.REPORT_DELAYED_DAYSDIFF_STDDEV: [self.measure_delayed_daysdiff_stats()[1]],
            self.REPORT_DELAYED_DAYSDIFF_COUNT: [self.measure_delayed_daysdiff_stats()[2]],
            self.REPORT_PREPAID_DAYSDIFF_MEAN: [self.measure_prepaid_daysdiff_stats()[0]],
            self.REPORT_PREPAID_DAYSDIFF_STDDEV: [self.measure_prepaid_daysdiff_stats()[1]],
            self.REPORT_PREPAID_DAYSDIFF_COUNT: [self.measure_prepaid_daysdiff_stats()[2]],
            self.REPORT_PAYMENT_ONTIME_COUNT: [self.count_ontime_payments()],
        }, index=[self.source_codename])

