from pathlib import Path

from lib.input_const import PayDelayColumns, PaymentGroupsColumns, payments_grouped_by_stories_file

import pandas as pd
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
        (yes, interestingly the order of records is not preserved when store - restore from parquet file)
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
            [self.COL_STORY_ID, PayDelayColumns.Id.name]
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



