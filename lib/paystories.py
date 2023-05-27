from lib.input_const import *
from lib.subarrow import ArrowAggregate

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa

from typing import Optional


class PaymentHistoryGrouper:
    """
    The class is responsible for grouping payment delay records into "payment stories",
    which are the timely ordered instances of payments of entities. Any detected debt causes the payment-story to be
    interrupted. This is because the existence of a debt is very important to evaluate the prediction capabilities
    of preceding payment-story
    """

    COL_DIVIDING_CS = PaymentGroupsColumns.DividingCreditStatus.name
    COL_DIVIDING_ID = 'dividing_id'
    COL_DIVIDING_DAYS_TO_DEBT = PaymentGroupsColumns.DividingDaysToDebt.name
    NEXT = '_next'
    COL_STORY_ID = PaymentGroupsColumns.StoryId.name

    def __init__(self, source_file: Path, codename: str):
        self._file = source_file
        self.source_codename = codename
        self._content: Optional[pa.Table] = None
        self._dividers: Optional[pa.Table] = None
        self._story_ids: Optional[pa.Table] = None

    def content(self) -> pa.Table:
        """
        Loads the payment-delay data with debt information from parquet file.
        Only non-outliers are loaded. The loaded content is sorted by pd-id
        (sic! interestingly the order of records is not preserved when store - restore from parquet file)
        :return: the reference to pyarrow Table (use it to display number of rows and allocated memory)
        """
        if self._content is None:
            self._content = pq.read_table(
                self._file, columns=[
                    PayDelayColumns.Id.name,
                    PayDelayColumns.EntityId.name,
                    PayDelayColumns.DueDate.name,
                    PayDelayColumns.DelayDays.name,
                    PayDelayColumns.InvoicedAmount.name,
                    PayDelayColumns.PriorDebtsMaxCreditStatus.name,
                    PayDelayColumns.LaterDebtsMinDaysToValidFrom(1).name,
                    PayDelayColumns.LaterDebtsMinDaysToValidFrom(2).name,
                    PayDelayColumns.LaterDebtsMinDaysToValidFrom(3).name,
                    PayDelayColumns.LaterDebtsMinDaysToValidFrom(4).name
                ],
                filters=~pc.field(PayDelayColumns.IsOutlier.name)
            )
        self._content = self._content.sort_by(PayDelayColumns.Id.name)
        return self._content

    def detect_dividers(self) -> pa.Table:
        _next = self._content.set_column(
            self._content.schema.get_field_index(PayDelayColumns.Id.name),
            PayDelayColumns.Id.name,
            pc.cast(pc.add(self._content.column(PayDelayColumns.Id.name), -1), PayDelayColumns.Id.otype)
        )

        self._content = self._content.join(
            _next,
            keys=[PayDelayColumns.Id.name, PayDelayColumns.EntityId.name],
            join_type='left outer',
            right_suffix=self.NEXT
        )
        _next = None

        for credit_status in range(1, 5):
            self._content = self._content.append_column(
                f"{self.COL_DIVIDING_CS}_{credit_status}",
                pc.if_else(
                    pc.and_kleene(
                        pc.is_valid(self._content.column(PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name)),
                        pc.or_kleene(
                            pc.is_null(
                                self._content.column(PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name + self.NEXT)),
                            pc.greater(
                                self._content.column(PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name + self.NEXT),
                                self._content.column(PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name)
                            )
                        )
                    ),
                    pc.cast(credit_status, pa.uint8()), pc.cast(0, pa.uint8())
                )
            )

        self._content = self._content.append_column(
            self.COL_DIVIDING_CS,
            pc.max_element_wise(*[
                self._content.column(f"{self.COL_DIVIDING_CS}_{credit_status}")
                for credit_status in range(1, 5)
            ]))

        for credit_status in range(1, 5):
            self._content = self._content.remove_column(
                self._content.schema.get_field_index(f"{self.COL_DIVIDING_CS}_{credit_status}")
            )

        # divider is defined by either one where debt was detected (actually, the previous record will be taken)
        # or the last one (detected by missing delay-days, an effect of left-join to next record)
        self._dividers = self._content.filter(
            (pc.field(self.COL_DIVIDING_CS) > 0) |
            pc.field(PayDelayColumns.DelayDays.name + self.NEXT).is_null()
        )

        # choosing the appropriate later-debt-in-days is tricky
        # first, replace the values in the _1, ..., _4 columns with NULL if the dividing-cs is not equal particular cs
        # then choose max, which shall be only one not null
        for credit_status in range(1, 5):
            self._dividers = self._dividers.set_column(
                self._dividers.schema.get_field_index(PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name),
                PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name,
                pc.if_else(
                    pc.equal(self._dividers.column(self.COL_DIVIDING_CS), credit_status),
                    self._dividers.column(PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name),
                    pa.nulls(self._dividers.num_rows)
                )
            )
        self._dividers = self._dividers.append_column(
            self.COL_DIVIDING_DAYS_TO_DEBT,
            pc.max_element_wise(*[
                self._dividers.column(PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name)
                for credit_status in range(1, 5)
            ])
        ).select(
            [PayDelayColumns.Id.name, PayDelayColumns.EntityId.name, self.COL_DIVIDING_CS,self.COL_DIVIDING_DAYS_TO_DEBT]
        ).rename_columns(
            [self.COL_DIVIDING_ID, PayDelayColumns.EntityId.name, self.COL_DIVIDING_CS, self.COL_DIVIDING_DAYS_TO_DEBT]
        )

        # get rid of columns, which are no longer needed
        for credit_status in range(1, 5):
            self._content = self._content.remove_column(
                self._content.schema.get_field_index(PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name))
            self._content = self._content.remove_column(
                self._content.schema.get_field_index(
                    PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status).name + self.NEXT))
        self._content = self._content.remove_column(
            self._content.schema.get_field_index(PayDelayColumns.DueDate.name + self.NEXT))
        self._content = self._content.remove_column(
            self._content.schema.get_field_index(PayDelayColumns.DelayDays.name + self.NEXT))
        self._content = self._content.remove_column(
            self._content.schema.get_field_index(PayDelayColumns.InvoicedAmount.name + self.NEXT))
        self._content = self._content.remove_column(
            self._content.schema.get_field_index(self.COL_DIVIDING_CS))
        self._content = self._content.remove_column(
            self._content.schema.get_field_index(PayDelayColumns.PriorDebtsMaxCreditStatus.name+self.NEXT))

        return self._dividers

    def calculate_story_ids(self) -> pa.Table:
        self._story_ids = self._content.select([PayDelayColumns.Id.name, PayDelayColumns.EntityId.name]).join(
            self._dividers.select([PayDelayColumns.EntityId.name, self.COL_DIVIDING_ID]),
            keys=[PayDelayColumns.EntityId.name],
            join_type='left outer'
        )

        self._story_ids = self._story_ids.append_column(
            self.COL_STORY_ID,
            pc.if_else(
                pc.less(self._story_ids.column(self.COL_DIVIDING_ID), self._story_ids.column(PayDelayColumns.Id.name)),
                pa.nulls(self._story_ids.num_rows),
                self._story_ids.column(self.COL_DIVIDING_ID)
            )
        ).filter(
            pc.field(self.COL_STORY_ID).is_valid()
        ).group_by(
            PayDelayColumns.Id.name
        ).aggregate(
            [(self.COL_STORY_ID, 'min')]
        ).rename_columns(
            [PayDelayColumns.Id.name, self.COL_STORY_ID]
        )

        return self._story_ids

    def combine_and_store(self, input_code: str) -> pa.Table:
        # construct the final output
        self._content = self._content.join(
            self._story_ids,
            keys=PayDelayColumns.Id.name
        ).join(
            self._dividers.select([self.COL_DIVIDING_ID, self.COL_DIVIDING_CS, self.COL_DIVIDING_DAYS_TO_DEBT]),
            keys=self.COL_STORY_ID,
            right_keys=self.COL_DIVIDING_ID
        )

        pq.write_table(self._content, payments_grouped_by_stories_file(input_code, self.source_codename))
        return self._content


class PaymentStoriesBuilder:
    """
    """

    _USE_SUBARROW_WHEN_LARGER_THAN_RECORDS = 10000

    def __init__(self, source_file: Path, codename: str):
        self._file = source_file
        self.source_codename = codename
        self._payments: Optional[pa.Table] = None
        self._stories: Optional[pa.Table] = None
        self._delay_mean = None
        self._delay_stddev = None
        self._amount_median = None
        self._amount_Q_3_1 = None

    def payments(self) -> pa.Table:
        if self._payments is None:
            # FIXME consider loading only part of the columns
            # some columns are calculated by this class and then grouped payments are updated!
            # on one hand it is cool to have them calculated once, on the other: what if something will change?
            self._payments = pq.read_table(self._file, columns=[
                PaymentGroupsColumns.Id.name,
                PaymentGroupsColumns.EntityId.name,
                PaymentGroupsColumns.DueDate.name,
                PaymentGroupsColumns.DelayDays.name,
                PaymentGroupsColumns.InvoicedAmount.name,
                PaymentGroupsColumns.PriorCreditStatusMax.name,
                PaymentGroupsColumns.StoryId.name,
                PaymentGroupsColumns.DividingCreditStatus.name,
                PaymentGroupsColumns.DividingDaysToDebt.name
            ])

        return self._payments

    def delay_mean(self) -> pa.float32():
        if self._delay_mean is None:
            self._delay_mean = pc.mean(self.payments().column(PaymentGroupsColumns.DelayDays.name))
        return pc.cast(self._delay_mean, pa.float32())

    def delay_stddev(self) -> pa.float32():
        if self._delay_stddev is None:
            self._delay_stddev = pc.stddev(self.payments().column(PaymentGroupsColumns.DelayDays.name))
        return pc.cast(self._delay_stddev, pa.float32())

    def amount_median(self):
        if self._amount_median is None:
            self._amount_median = pc.approximate_median(self.payments().column(PaymentGroupsColumns.InvoicedAmount.name))
        return pc.cast(self._amount_median, pa.float32())

    def amount_quantile_range(self):
        if self._amount_Q_3_1 is None:
            _Q_1 = pc.quantile(self.payments().column(PaymentGroupsColumns.InvoicedAmount.name), 0.25)
            _Q_3 = pc.quantile(self.payments().column(PaymentGroupsColumns.InvoicedAmount.name), 0.75)
            self._amount_Q_3_1 = pc.cast(pc.subtract(_Q_3, _Q_1)[0], pa.float32())
        return self._amount_Q_3_1

    def scaled_delays(self) -> pa.Table:
        if PaymentGroupsColumns.DelayDaysScaled.name not in self.payments().column_names:
            self._payments = self.payments().append_column(
                PaymentGroupsColumns.DelayDaysScaled.name,
                pc.divide(
                    pc.subtract(
                        self.payments().column(PaymentGroupsColumns.DelayDays.name),
                        self.delay_mean()
                    ),
                    self.delay_stddev())
            )
        return self._payments

    def scaled_amount(self) -> pa.Table:
        if PaymentGroupsColumns.InvoicedAmountScaled.name not in self.payments().column_names:
            # if count of missing amounts is < 10% then replace it with median
            # otherwise the records should not be taken into consideration when calculating
            # features based on amounts
            if (self.payments().column(PaymentGroupsColumns.InvoicedAmount.name).null_count / self.payments().num_rows) < 0.1:
                self._payments = self.payments().set_column(
                    self._payments.schema.get_field_index(PaymentGroupsColumns.InvoicedAmount.name),
                    PaymentGroupsColumns.InvoicedAmount.name,
                    self.payments().column(PaymentGroupsColumns.InvoicedAmount.name).fill_null(self.amount_median())
                )
                self._payments = self.payments().append_column(
                    PaymentGroupsColumns.InvoicedAmountScaled.name,
                    pc.divide(
                        pc.subtract(
                            self.payments().column(PaymentGroupsColumns.InvoicedAmount.name),
                            self.amount_median()
                        ),
                        self.amount_quantile_range()
                    )
                )
            else:
                self._payments = self.payments().append_column(
                    PaymentGroupsColumns.InvoicedAmountScaled.name,
                    pa.nulls(self._payments.num_rows, PaymentGroupsColumns.InvoicedAmount.otype).fill_null(0)
                )
        return self._payments

    def story_timeline(self) -> pa.Table:
        """
        Ensures that in payments table there is a column with days-since-min-due-date.
        This will play role of x-axis for regression line.
        :return: the payments table with the desired column
        """
        if PaymentGroupsColumns.StoryTimeline.name not in self.payments().column_names:
            # note that in order to get payments, the method "severity" is invoked
            # this is to ensure that the step is already executed
            _min_due_date = self.severity().group_by(
                PaymentGroupsColumns.StoryId.name
            ).aggregate(
                [(PaymentGroupsColumns.DueDate.name, 'min')]
            )

            self._payments = self._payments.join(
                _min_due_date,
                keys=PaymentGroupsColumns.StoryId.name
            )

            self._payments = self._payments.append_column(
                PaymentGroupsColumns.StoryTimeline.name,
                pc.days_between(
                    self._payments.column(PaymentGroupsColumns.DueDate.name + '_min'),
                    self._payments.column(PaymentGroupsColumns.DueDate.name)
                )
            )

            self._payments = self._payments.remove_column(self._payments.schema.get_field_index(PaymentGroupsColumns.DueDate.name + '_min'))

        return self._payments

    def severity(self) -> pa.Table:
        if PaymentGroupsColumns.Severity.name not in self.payments().column_names:
            delay_scaled = self.scaled_delays().column(PaymentGroupsColumns.DelayDaysScaled.name)
            amount_scaled = self.scaled_amount().column(PaymentGroupsColumns.InvoicedAmountScaled.name)
            self._payments = self.payments().append_column(
                PaymentGroupsColumns.Severity.name,
                pc.multiply(
                    delay_scaled,
                    pc.add(amount_scaled, pc.add(pc.abs(pc.min(amount_scaled)), 1.0))
                )
            )

        return self.payments()

    def stories(self) -> pa.Table:
        if self._stories is None:
            self.story_timeline()
            _col_paid = 'paid_after_days_since_story_start'
            _payments = self.severity().append_column(
                _col_paid,
                pc.add(
                    self.severity().column(PaymentGroupsColumns.StoryTimeline.name),
                    self.severity().column(PaymentGroupsColumns.DelayDays.name)
                )
            )

            self._stories = _payments.group_by(
                PaymentGroupsColumns.StoryId.name
            ).aggregate([
                (PaymentGroupsColumns.Id.name, 'min'),
                (PaymentGroupsColumns.EntityId.name, 'min'),
                (PaymentGroupsColumns.PriorCreditStatusMax.name, 'min'),
                (PaymentGroupsColumns.DividingCreditStatus.name, 'min'),
                (PaymentGroupsColumns.DividingDaysToDebt.name, 'min'),
                (PaymentGroupsColumns.DueDate.name, 'min'),
                (PaymentGroupsColumns.DueDate.name, 'max'),
                (_col_paid, 'max'),
                (PaymentGroupsColumns.Id.name, 'count'),
                (PaymentGroupsColumns.DelayDaysScaled.name, 'mean'),
                (PaymentGroupsColumns.InvoicedAmountScaled.name, 'mean'),
                (PaymentGroupsColumns.Severity.name, 'mean'),
                (PaymentGroupsColumns.StoryTimeline.name, 'mean')
            ]).rename_columns([
                PaymentStoriesColumns.StoryId.name,
                PaymentStoriesColumns.FirstPaymentId.name,
                PaymentStoriesColumns.EntityId.name,
                PaymentStoriesColumns.BeginsWithCreditStatus.name,
                PaymentStoriesColumns.EndsWithCreditStatus.name,
                PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name,
                PaymentStoriesColumns.BeginsAt.name,
                PaymentStoriesColumns.EndsAt.name,
                PaymentStoriesColumns.Duration.name,
                PaymentStoriesColumns.PaymentsCount.name,
                PaymentStoriesColumns.ScaledDelayMean.name,
                PaymentStoriesColumns.ScaledAmountMean.name,
                PaymentStoriesColumns.SeverityMean.name,
                PaymentStoriesColumns.DaysSinceBeginMean.name,
            ])

            self._stories = self.stories().append_column(
                pa.field(
                    PaymentStoriesColumns.DenotesAnyRisk.name, PaymentStoriesColumns.DenotesAnyRisk.otype, False
                ),
                pc.fill_null(
                    pc.and_(
                        pc.greater(
                            pc.min_element_wise(
                                pc.max_element_wise(
                                    self.stories().column(PaymentStoriesColumns.Duration.name),
                                    pa.nulls(
                                        self._stories.num_rows, PaymentStoriesColumns.Duration.otype
                                    ).fill_null(DENOTES_RISK_MIN_TIME_WINDOW_DAYS)
                                ),
                                pa.nulls(
                                    self._stories.num_rows, PaymentStoriesColumns.Duration.otype
                                ).fill_null(DENOTES_RISK_MAX_TIME_WINDOW_DAYS)
                            ),
                            self.stories().column(PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name)
                        ),
                        pc.greater(
                            self._stories.column(PaymentStoriesColumns.EndsWithCreditStatus.name), 0
                        )
                    ),
                    False
                )
            ).append_column(
                pa.field(
                    PaymentStoriesColumns.DenotesSignificantRisk.name,
                    PaymentStoriesColumns.DenotesSignificantRisk.otype, False
                ),
                pc.fill_null(
                    pc.and_(
                        pc.greater(
                            pc.min_element_wise(
                                pc.max_element_wise(
                                    self.stories().column(PaymentStoriesColumns.Duration.name),
                                    pa.nulls(
                                        self._stories.num_rows, PaymentStoriesColumns.Duration.otype
                                    ).fill_null(DENOTES_RISK_MIN_TIME_WINDOW_DAYS)
                                ),
                                pa.nulls(
                                    self._stories.num_rows, PaymentStoriesColumns.Duration.otype
                                ).fill_null(DENOTES_RISK_MAX_TIME_WINDOW_DAYS)
                            ),
                            self.stories().column(PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name)
                        ),
                        pc.greater(
                            self._stories.column(PaymentStoriesColumns.EndsWithCreditStatus.name), 2
                        )
                    ),
                    False
                )
            )

        return self._stories

    def tendencies(self) -> pa.Table:
        if PaymentStoriesColumns.TendencyCoefficient_ForDelay.name in self.stories().column_names:
            return self.stories()

        # 1. calculate regression line parameters a0 and a1
        _col_distance_to_mean_x = 'distance-to-mean-x'
        _col_distance_to_mean_x_squared = 'distance-to-mean-x-squared'
        _col_distance_to_mean_y_delay = 'distance-to-mean-y-delay'
        _col_distance_to_mean_y_severity = 'distance-to-mean-y-severity'
        _col_xdist_m_ydist_delay = 'distance-to-mean-x-multiplied-by-distance-to-mean-y-delay'
        _col_xdist_m_ydist_severity = 'distance-to-mean-x-multiplied-by-distance-to-mean-y-severity'
        _payments = self.story_timeline().select([
            PaymentGroupsColumns.StoryId.name,
            PaymentGroupsColumns.EntityId.name,
            PaymentGroupsColumns.StoryTimeline.name,
            PaymentGroupsColumns.DelayDaysScaled.name,
            PaymentGroupsColumns.Severity.name
        ]).join(
            self.stories().select([
                PaymentStoriesColumns.StoryId.name,
                PaymentStoriesColumns.EntityId.name,
                PaymentStoriesColumns.DaysSinceBeginMean.name,
                PaymentStoriesColumns.ScaledDelayMean.name,
                PaymentStoriesColumns.SeverityMean.name
            ]), keys=[PaymentGroupsColumns.StoryId.name, PaymentGroupsColumns.EntityId.name]
        )
        _payments = _payments.append_column(
            _col_distance_to_mean_x,
            pc.subtract(
                _payments.column(PaymentGroupsColumns.StoryTimeline.name),
                _payments.column(PaymentStoriesColumns.DaysSinceBeginMean.name)
            )
        ).append_column(
            _col_distance_to_mean_y_delay,
            pc.subtract(
                _payments.column(PaymentGroupsColumns.DelayDaysScaled.name),
                _payments.column(PaymentStoriesColumns.ScaledDelayMean.name)
            )
        ).append_column(
            _col_distance_to_mean_y_severity,
            pc.subtract(
                _payments.column(PaymentGroupsColumns.Severity.name),
                _payments.column(PaymentStoriesColumns.SeverityMean.name)
            )
        )
        _payments = _payments.append_column(
            _col_distance_to_mean_x_squared,
            pc.power(_payments.column(_col_distance_to_mean_x), 2)
        )
        _payments = _payments.append_column(
            _col_xdist_m_ydist_delay,
            pc.multiply(
                _payments.column(_col_distance_to_mean_x),
                _payments.column(_col_distance_to_mean_y_delay)
            )
        ).append_column(
            _col_xdist_m_ydist_severity,
            pc.multiply(
                _payments.column(_col_distance_to_mean_x),
                _payments.column(_col_distance_to_mean_y_severity)
            )
        )

        if _payments.num_rows > self._USE_SUBARROW_WHEN_LARGER_THAN_RECORDS:
            _a1_components = ArrowAggregate(_payments, [PaymentGroupsColumns.StoryId.name], [
                (_col_xdist_m_ydist_delay, 'sum'),
                (_col_xdist_m_ydist_severity, 'sum'),
                (_col_distance_to_mean_x_squared, 'sum')
            ], tempdir=DIR_PROCESSING.absolute()).aggregate()
        else:
            _a1_components = _payments.group_by(
                PaymentGroupsColumns.StoryId.name
            ).aggregate([
                (_col_xdist_m_ydist_delay, 'sum'),
                (_col_xdist_m_ydist_severity, 'sum'),
                (_col_distance_to_mean_x_squared, 'sum')
            ])

        _a1_components = _a1_components.append_column(
            PaymentStoriesColumns.TendencyCoefficient_ForDelay.name,
            pc.divide(
                _a1_components.column(_col_xdist_m_ydist_delay + '_sum'),
                _a1_components.column(_col_distance_to_mean_x_squared + '_sum'),
            )
        ).append_column(
            PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name,
            pc.divide(
                _a1_components.column(_col_xdist_m_ydist_severity + '_sum'),
                _a1_components.column(_col_distance_to_mean_x_squared + '_sum'),
            )
        )

        self._stories = self.stories().join(
            _a1_components.select([
                PaymentStoriesColumns.StoryId.name,
                PaymentStoriesColumns.TendencyCoefficient_ForDelay.name,
                PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name
            ]),
            keys=PaymentStoriesColumns.StoryId.name
        )

        self._stories = self.stories().append_column(
            PaymentStoriesColumns.TendencyConstant_ForDelay.name,
            pc.subtract(
                self.stories().column(PaymentStoriesColumns.ScaledDelayMean.name),
                pc.multiply(
                    self.stories().column(PaymentStoriesColumns.TendencyCoefficient_ForDelay.name),
                    self.stories().column(PaymentStoriesColumns.DaysSinceBeginMean.name)
                )
            )
        ).append_column(
            PaymentStoriesColumns.TendencyConstant_ForSeverity.name,
            pc.subtract(
                self.stories().column(PaymentStoriesColumns.SeverityMean.name),
                pc.multiply(
                    self.stories().column(PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name),
                    self.stories().column(PaymentStoriesColumns.DaysSinceBeginMean.name)
                )
            )
        )

        # 2. calculate theoretical value to each payment
        _col_theoretical_delay_scaled_from_regrline = 'theoretical-delay-scaled-from-regression-line'
        _col_theoretical_severity_from_regression_line = 'theoretical-severity-from-regression-line'
        _payments = _payments.join(
            self.stories().select([
                PaymentStoriesColumns.StoryId.name,
                PaymentStoriesColumns.TendencyConstant_ForDelay.name,
                PaymentStoriesColumns.TendencyConstant_ForSeverity.name,
                PaymentStoriesColumns.TendencyCoefficient_ForDelay.name,
                PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name
            ]),
            keys=PaymentStoriesColumns.StoryId.name
        )
        _payments = _payments.append_column(
            _col_theoretical_delay_scaled_from_regrline,
            pc.add(
                pc.multiply(
                    _payments.column(PaymentStoriesColumns.TendencyCoefficient_ForDelay.name),
                    _payments.column(PaymentGroupsColumns.StoryTimeline.name)
                ),
                _payments.column(PaymentStoriesColumns.TendencyConstant_ForDelay.name)
            )
        ).append_column(
            _col_theoretical_severity_from_regression_line,
            pc.add(
                pc.multiply(
                    _payments.column(PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name),
                    _payments.column(PaymentGroupsColumns.StoryTimeline.name)
                ),
                _payments.column(PaymentStoriesColumns.TendencyConstant_ForSeverity.name)
            )
        )

        # 2A. keep the value of last point - this will be used as the predictor (not the coefficient!)
        _last_story_record = _payments.group_by(
            PaymentGroupsColumns.StoryId.name
        ).aggregate(
            [(PaymentGroupsColumns.StoryTimeline.name, 'max')]
        )
        _tendencies_values = self.stories().join(_last_story_record, PaymentGroupsColumns.StoryId.name)
        _tendencies_values = _tendencies_values.append_column(
            PaymentStoriesColumns.Tendency_ForDelay.name,
            pc.add(
                pc.multiply(
                    _tendencies_values.column(PaymentStoriesColumns.TendencyCoefficient_ForDelay.name),
                    _tendencies_values.column(PaymentGroupsColumns.StoryTimeline.name+'_max')
                ),
                _tendencies_values.column(PaymentStoriesColumns.TendencyConstant_ForDelay.name)
            )
        ).append_column(
            PaymentStoriesColumns.Tendency_ForSeverity.name,
            pc.add(
                pc.multiply(
                    _tendencies_values.column(PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name),
                    _tendencies_values.column(PaymentGroupsColumns.StoryTimeline.name+'_max')
                ),
                _tendencies_values.column(PaymentStoriesColumns.TendencyConstant_ForSeverity.name)
            )
        )
        _tendencies_values = _tendencies_values.append_column(
            PaymentStoriesColumns.TendencyMinusMean_ForDelay.name,
            pc.subtract(
                _tendencies_values.column(PaymentStoriesColumns.Tendency_ForDelay.name),
                _tendencies_values.column(PaymentStoriesColumns.ScaledDelayMean.name)
            )
        ).append_column(
            PaymentStoriesColumns.TendencyMinusMean_ForSeverity.name,
            pc.subtract(
                _tendencies_values.column(PaymentStoriesColumns.Tendency_ForSeverity.name),
                _tendencies_values.column(PaymentStoriesColumns.SeverityMean.name)
            )
        ).select([
            PaymentGroupsColumns.StoryId.name,
            PaymentStoriesColumns.Tendency_ForDelay.name,
            PaymentStoriesColumns.Tendency_ForSeverity.name,
            PaymentStoriesColumns.TendencyMinusMean_ForDelay.name,
            PaymentStoriesColumns.TendencyMinusMean_ForSeverity.name
        ])
        self._stories = self.stories().join(_tendencies_values, PaymentGroupsColumns.StoryId.name)

        # 3. calculate r-squared (coefficient of determination)
        _col_distance_to_mean_y_theoretical_delay_squared = 'distance-to-mean-y-delay-theoretical-squared'
        _col_distance_to_mean_y_theoretical_severity_squared = 'distance-to-mean-y-severity-theoretical-squared'
        _col_distance_to_mean_y_delay_squared = 'distance-to-mean-y-delay-squared'
        _col_distance_to_mean_y_severity_squared = 'distance-to-mean-y-severity-squared'
        _payments = _payments.append_column(
            _col_distance_to_mean_y_theoretical_delay_squared,
            pc.power(
                pc.subtract(
                    _payments.column(_col_theoretical_delay_scaled_from_regrline),
                    _payments.column(PaymentGroupsColumns.DelayDaysScaled.name)
                ), 2
            )
        ).append_column(
            _col_distance_to_mean_y_theoretical_severity_squared,
            pc.power(
                pc.subtract(
                    _payments.column(_col_theoretical_severity_from_regression_line),
                    _payments.column(PaymentGroupsColumns.Severity.name)
                ), 2
            )
        ).append_column(
            _col_distance_to_mean_y_delay_squared,
            pc.power(_payments.column(_col_distance_to_mean_y_delay), 2)
        ).append_column(
            _col_distance_to_mean_y_severity_squared,
            pc.power(_payments.column(_col_distance_to_mean_y_severity), 2)
        )

        if _payments.num_rows > self._USE_SUBARROW_WHEN_LARGER_THAN_RECORDS:
            _rsquare_components = ArrowAggregate(_payments, [PaymentGroupsColumns.StoryId.name], [
                    (_col_distance_to_mean_y_theoretical_delay_squared, 'sum'),
                    (_col_distance_to_mean_y_theoretical_severity_squared, 'sum'),
                    (_col_distance_to_mean_y_delay_squared, 'sum'),
                    (_col_distance_to_mean_y_severity_squared, 'sum')
            ], tempdir=DIR_PROCESSING.absolute()).aggregate()
        else:
            _rsquare_components = _payments.group_by(
                PaymentGroupsColumns.StoryId.name
            ).aggregate([
                (_col_distance_to_mean_y_theoretical_delay_squared, 'sum'),
                (_col_distance_to_mean_y_theoretical_severity_squared, 'sum'),
                (_col_distance_to_mean_y_delay_squared, 'sum'),
                (_col_distance_to_mean_y_severity_squared, 'sum')
            ])

        _rsquare_components = _rsquare_components.append_column(
            PaymentStoriesColumns.TendencyError_ForDelay.name,
            pc.subtract(
                pa.nulls(_rsquare_components.num_rows, PaymentStoriesColumns.TendencyError_ForDelay.otype).fill_null(1.0),
                pc.divide(
                    _rsquare_components.column(_col_distance_to_mean_y_theoretical_delay_squared + '_sum'),
                    _rsquare_components.column(_col_distance_to_mean_y_delay_squared + '_sum')
                )
            )
        ).append_column(
            PaymentStoriesColumns.TendencyError_ForSeverity.name,
            pc.subtract(
                pa.nulls(_rsquare_components.num_rows, PaymentStoriesColumns.TendencyError_ForSeverity.otype).fill_null(1.0),
                pc.divide(
                    _rsquare_components.column(_col_distance_to_mean_y_theoretical_severity_squared + '_sum'),
                    _rsquare_components.column(_col_distance_to_mean_y_severity_squared + '_sum')
                )
            )
        )

        self._stories = self.stories().join(
            _rsquare_components.select([
                PaymentStoriesColumns.StoryId.name,
                PaymentStoriesColumns.TendencyError_ForDelay.name,
                PaymentStoriesColumns.TendencyError_ForSeverity.name
            ]),
            keys=PaymentStoriesColumns.StoryId.name
        )

        return self._stories

    def write_stories(self, input_code: str) -> Path:
        _file = payment_stories_file(input_code, self.source_codename)
        pq.write_table(self.stories(), _file)
        return _file

    def update_payment_groups(self, input_code: str) -> Path:
        _file = payments_grouped_by_stories_file(input_code, self.source_codename)
        pq.write_table(self.payments(), _file)
        return _file

#
# class PaymentHistoryBuilderPandas:
#     """
#     This solution works great, but is soooo slooooow!
#     """
#
#     def __init__(self, source_file: Path, codename: str):
#         self._file = source_file
#         self.source_codename = codename
#         self._input_content: Optional[pd.DataFrame] = None
#         self._timelines: Optional[pd.DataFrame] = None
#         raise ValueError('DO NOT USE. IT IS TOO SLOW')
#
#     def content(self) -> pd.DataFrame:
#         if self._input_content is None:
#             self._input_content = pd.read_parquet(
#                 self._file,
#                 columns=[
#                     PayDelayColumns.Id.name,
#                     PayDelayColumns.EntityId.name,
#                     PayDelayColumns.DueDate.name,
#                     PayDelayColumns.DelayDays.name,
#                     PayDelayColumns.InvoicedAmount.name,
#                     PayDelayColumns.PriorDebtsMaxCreditStatus.name,
#                     PayDelayColumns.LaterDebtsMinDaysToValidFrom(1).name,
#                     PayDelayColumns.LaterDebtsMinDaysToValidFrom(2).name,
#                     PayDelayColumns.LaterDebtsMinDaysToValidFrom(3).name,
#                     PayDelayColumns.LaterDebtsMinDaysToValidFrom(4).name,
#                     PayDelayColumns.IsOutlier.name,
#                 ],
#                 engine='pyarrow',
#                 dtype_backend='pyarrow'
#             )
#         return self._input_content
#
#     def build_timelines(self) -> pd.DataFrame:
#         return self.content().groupby(by=PayDelayColumns.EntityId.name, sort=False).apply(self._process_entity)
#
#     class Payment:
#
#         def __init__(self, row: pd.Series):
#             self._row = row
#
#         def new_debt_detected_rc(self, previous) -> int:
#             """
#             'Dividing debt' is detected when the min-days-to-debt either:
#             - turns from not-null to null
#             OR
#             - the value INCREASES
#             :param previous:
#             :return: max credit-status of "dividing debt"
#             """
#             if previous is None:
#                 return 0
#             for risk_class in range(4, 0, -1):
#                 _f = PayDelayColumns.LaterDebtsMinDaysToValidFrom(risk_class).name
#                 if not pd.isnull(previous._row[_f]) \
#                         and (pd.isnull(self._row[_f]) or (previous._row[_f] - self._row[_f]) < 0):
#
#                     return risk_class
#             return 0
#
#         def prior_max_rc(self) -> int:
#             return 0 if pd.isnull(self._row[PayDelayColumns.PriorDebtsMaxCreditStatus.name]) \
#                 else self._row[PayDelayColumns.PriorDebtsMaxCreditStatus.name]
#
#         def p_id(self) -> int:
#             return self._row[PayDelayColumns.Id.name]
#
#         def entity_id(self) -> int:
#             return self._row[PayDelayColumns.EntityId.name]
#
#         def due_date(self):
#             return self._row[PayDelayColumns.DueDate.name]
#
#         def delay_days(self):
#             return self._row[PayDelayColumns.DelayDays.name]
#
#         def amount(self):
#             return self._row[PayDelayColumns.InvoicedAmount.name]
#
#         def days_to_debts(self) -> tuple:
#             return tuple(self._row[PayDelayColumns.LaterDebtsMinDaysToValidFrom(risk_class).name]
#                          for risk_class in range(1, 5))
#
#     def _process_entity(self, payments_entity: pd.DataFrame) -> pd.DataFrame:
#         _timelines = list[tuple]()
#         _current_timeline = list[self.Payment]()
#         _prev = None
#         for _row in payments_entity[~payments_entity[PayDelayColumns.IsOutlier.name]].iterrows():
#             _curr = self.Payment(_row[1])
#             _ndd_rc = _curr.new_debt_detected_rc(_prev)
#             if _ndd_rc > 0:
#                 _timelines.append(self._prepare_timeline(_current_timeline, _ndd_rc))
#                 # Well, dividing the stories by each update of debts will scatter the story into possibly tiny pieces.
#                 # To be considered: instead, create stories per each detected debt.
#                 # If decided so, it is enough to simply comment below line
#                 # _current_timeline = list()
#                 # Maybe then, as a trade-off, reset the timeline only if longer than 2 elements?
#                 if len(_current_timeline) > 2:
#                     _current_timeline = list()
#
#             _current_timeline.append(_curr)
#             _prev = _curr
#         if len(_current_timeline) > 0:
#             _timelines.append(self._prepare_timeline(_current_timeline, 0))
#         return pd.DataFrame(
#             _timelines,
#             columns=[
#                 PaymentStoryColumns.FirstPaymentId.name,
#                 PaymentStoryColumns.EntityId.name,
#                 PaymentStoryColumns.BeginsAt.name,
#                 PaymentStoryColumns.EndsAt.name,
#                 PaymentStoryColumns.PaymentsList.name,
#                 PaymentStoryColumns.BeginsWithCreditStatus.name,
#                 PaymentStoryColumns.EndsWithCreditStatus.name,
#                 PaymentStoryColumns.LaterDebtMinDaysToValidFrom.name
#             ]
#         )
#
#     def _prepare_timeline(self, payments: list[Payment], dividing_rc: int) -> tuple:
#         return (
#             payments[0].p_id(),
#             payments[0].entity_id(),
#             payments[0].due_date(),
#             payments[-1].due_date(),
#             [(p.delay_days(), p.amount(), (p.due_date() - payments[0].due_date()).days) for p in payments],
#             payments[0].prior_max_rc(),
#             dividing_rc,
#             payments[-1].days_to_debts()
#         )
#



