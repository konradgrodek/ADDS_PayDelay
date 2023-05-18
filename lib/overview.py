from lib.input_const import *
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
    MAX_AMOUNT = 100000

    # ROC_TPR = 'sensitivity'
    # ROC_FPR = '1 - specifity'

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
        return pc.count_distinct(self.content().column(PayDelayColumns.EntityId.name)).as_py()

    def count_entities_with_later_debt(self) -> int:
        return pc.count_distinct(
            self.content()
                .filter(pc.field(PayDelayColumns.LaterDebtsMaxCreditStatus.name) > 0)
                .column(PayDelayColumns.EntityId.name)
        ).as_py()

    def count_entities_with_later_debt_rc4(self) -> int:
        return pc.count_distinct(
            self.content()
                .filter(pc.field(PayDelayColumns.LaterDebtsMaxCreditStatus.name) == 4)
                .column(PayDelayColumns.EntityId.name)
        ).as_py()

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
            if _count[0] == MALE:
                _men = _count[1]
            elif _count[0] == FEMALE:
                _women = _count[1]
            else:
                _unknown = _count[1]

        return _men / _women if _men*_women != 0 else None, \
            _unknown / sum([_men, _women, _unknown]) if _unknown > 0 else None

    @cache
    def measure_amount_stats(self) -> tuple:
        _amount_valid = self.content().filter(
            (pc.field(PayDelayColumns.InvoicedAmount.name) >= self.MIN_AMOUNT) &
            (pc.field(PayDelayColumns.InvoicedAmount.name) <= self.MAX_AMOUNT))\
            .column(PayDelayColumns.InvoicedAmount.name)
        _amount_unknown = self.content().filter(
            (pc.field(PayDelayColumns.InvoicedAmount.name).is_null()))
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

    def industry(self) -> str:
        return self.content().column(PayDelayColumns.Industry.name).unique().to_pylist()[0]

    # @cache
    # def calculate_roc_positive_payments(self, risk_class: int, positive_if_no_debt_within_years: int) -> pd.DataFrame:
    #     _positive_if_no_debt_within_days = positive_if_no_debt_within_years * 365
    #     _later_debt_colname = PayDelayColumns.LaterDebtsMinDaysToValidFrom(risk_class).name
    #     _positive_payments = self.content()\
    #         .filter(pc.field(PayDelayColumns.DelayDays.name) <= 0)\
    #         .select([PayDelayColumns.DelayDays.name, _later_debt_colname])
    #
    #     roc = {self.ROC_FPR: list(), self.ROC_TPR: list()}
    #     thresholds = list(range(0, abs(OUTLIER__MIN_DELAY)+1))
    #     for threshold in thresholds:
    #         positive = _positive_payments.filter(
    #             pc.field(_later_debt_colname).is_null() |
    #             (pc.field(_later_debt_colname) > _positive_if_no_debt_within_days)
    #         ).num_rows
    #         true_positive = _positive_payments.filter(
    #             (pc.abs(pc.field(PayDelayColumns.DelayDays.name)) >= threshold) &
    #             (pc.field(_later_debt_colname).is_null() |
    #             (pc.field(_later_debt_colname) > _positive_if_no_debt_within_days))
    #         ).num_rows
    #         false_positive = _positive_payments.filter(
    #             (pc.abs(pc.field(PayDelayColumns.DelayDays.name)) >= threshold) &
    #             (pc.field(_later_debt_colname) <= _positive_if_no_debt_within_days)
    #         ).num_rows
    #         negative = _positive_payments.filter(
    #             pc.field(_later_debt_colname) <= _positive_if_no_debt_within_days
    #         ).num_rows
    #         roc[self.ROC_FPR].append(false_positive / negative if negative != 0 else 0)
    #         roc[self.ROC_TPR].append(true_positive / positive if positive != 0 else 0)
    #
    #     return pd.DataFrame(roc, index=thresholds)

    def report(self) -> pd.DataFrame:
        return pd.DataFrame({
            OverviewReportColNames.Industry: [self.industry()],
            OverviewReportColNames.RecordsCountAll: [self.count_rows()],
            OverviewReportColNames.RecordsCountWithoutOutliers: [self.count_rows_wo_outliers()],
            OverviewReportColNames.EntitiesCount: [self.count_entities()],
            OverviewReportColNames.EntitiesWithLaterDebt: [self.count_entities_with_later_debt()],
            OverviewReportColNames.EntitiesWithLaterSevereDebt: [self.count_entities_with_later_debt_rc4()],
            OverviewReportColNames.AgeMean: [self.measure_age_stats()[0]],
            OverviewReportColNames.AgeStddev: [self.measure_age_stats()[1]],
            OverviewReportColNames.AgeSkewness: [self.measure_age_stats()[2]],
            OverviewReportColNames.GendersRatio: [self.measure_gender_ratio()[0]],
            OverviewReportColNames.UnknownGenderRatio: [self.measure_gender_ratio()[1]],
            OverviewReportColNames.AmountMean: [self.measure_amount_stats()[0]],
            OverviewReportColNames.AmountStandardDeviation: [self.measure_amount_stats()[1]],
            OverviewReportColNames.AmountUnknownCount: [self.measure_amount_stats()[2]],
            OverviewReportColNames.AmountTooHighCount: [self.measure_amount_stats()[3]],
            OverviewReportColNames.PaymentDaysMean: [self.measure_payment_daysdiff_stats()[0]],
            OverviewReportColNames.PaymentDaysStddev: [self.measure_payment_daysdiff_stats()[1]],
            OverviewReportColNames.PrepaidDaysMean: [self.measure_prepaid_daysdiff_stats()[0]],
            OverviewReportColNames.PrepaidDaysStddev: [self.measure_prepaid_daysdiff_stats()[1]],
            OverviewReportColNames.PrepaidCount: [self.measure_prepaid_daysdiff_stats()[2]],
            OverviewReportColNames.DelayDaysMean: [self.measure_delayed_daysdiff_stats()[0]],
            OverviewReportColNames.DelayDaysStddev: [self.measure_delayed_daysdiff_stats()[1]],
            OverviewReportColNames.DelayDaysCount: [self.measure_delayed_daysdiff_stats()[2]],
            OverviewReportColNames.PaidOnTimeCount: [self.count_ontime_payments()],
        }, index=[self.source_codename])

