from pathlib import Path

from lib.input_const import PayDelayColumns, PaymentStoryColumns

import pandas as pd
from typing import Optional


class PaymentHistoryBuilder:

    def __init__(self, source_file: Path, codename: str):
        self._file = source_file
        self.source_codename = codename
        self._input_content: Optional[pd.DataFrame] = None
        self._timelines: Optional[pd.DataFrame] = None

    def content(self) -> pd.DataFrame:
        if self._input_content is None:
            self._input_content = pd.read_parquet(
                self._file,
                columns=[
                    PayDelayColumns.Id.name,
                    PayDelayColumns.EntityId.name,
                    PayDelayColumns.DueDate.name,
                    PayDelayColumns.DelayDays.name,
                    PayDelayColumns.InvoicedAmount.name,
                    PayDelayColumns.PriorDebtsMaxCreditStatus.name,
                    PayDelayColumns.LaterDebtsMinDaysToValidFrom(1).name,
                    PayDelayColumns.LaterDebtsMinDaysToValidFrom(2).name,
                    PayDelayColumns.LaterDebtsMinDaysToValidFrom(3).name,
                    PayDelayColumns.LaterDebtsMinDaysToValidFrom(4).name,
                    PayDelayColumns.IsOutlier.name,
                ],
                engine='pyarrow',
                dtype_backend='pyarrow'
            )
        return self._input_content

    def build_timelines(self) -> pd.DataFrame:
        return self.content().groupby(by=PayDelayColumns.EntityId.name, sort=False).apply(self._process_entity)

    class Payment:

        def __init__(self, row: pd.Series):
            self._row = row

        def new_debt_detected_rc(self, previous) -> int:
            """
            'Dividing debt' is detected when the min-days-to-debt either:
            - turns from not-null to null
            OR
            - the value INCREASES
            :param previous:
            :return: max credit-status of "dividing debt"
            """
            if previous is None:
                return 0
            for risk_class in range(4, 0, -1):
                _f = PayDelayColumns.LaterDebtsMinDaysToValidFrom(risk_class).name
                if not pd.isnull(previous._row[_f]) \
                        and (pd.isnull(self._row[_f]) or (previous._row[_f] - self._row[_f]) < 0):

                    return risk_class
            return 0

        def prior_max_rc(self) -> int:
            return 0 if pd.isnull(self._row[PayDelayColumns.PriorDebtsMaxCreditStatus.name]) \
                else self._row[PayDelayColumns.PriorDebtsMaxCreditStatus.name]

        def p_id(self) -> int:
            return self._row[PayDelayColumns.Id.name]

        def entity_id(self) -> int:
            return self._row[PayDelayColumns.EntityId.name]

        def due_date(self):
            return self._row[PayDelayColumns.DueDate.name]

        def delay_days(self):
            return self._row[PayDelayColumns.DelayDays.name]

        def amount(self):
            return self._row[PayDelayColumns.InvoicedAmount.name]

        def days_to_debts(self) -> tuple:
            return tuple(self._row[PayDelayColumns.LaterDebtsMinDaysToValidFrom(risk_class).name]
                         for risk_class in range(1, 5))

    def _process_entity(self, payments_entity: pd.DataFrame) -> pd.DataFrame:
        _timelines = list[tuple]()
        _current_timeline = list[self.Payment]()
        _prev = None
        for _row in payments_entity[~payments_entity[PayDelayColumns.IsOutlier.name]].iterrows():
            _curr = self.Payment(_row[1])
            _ndd_rc = _curr.new_debt_detected_rc(_prev)
            if _ndd_rc > 0:
                _timelines.append(self._prepare_timeline(_current_timeline, _ndd_rc))
                # Well, dividing the stories by each update of debts will scatter the story into possibly tiny pieces.
                # To be considered: instead, create stories per each detected debt.
                # If decided so, it is enough to simply comment below line
                # _current_timeline = list()
                # Maybe then, as a trade-off, reset the timeline only if longer than 2 elements?
                if len(_current_timeline) > 2:
                    _current_timeline = list()

            _current_timeline.append(_curr)
            _prev = _curr
        if len(_current_timeline) > 0:
            _timelines.append(self._prepare_timeline(_current_timeline, 0))
        return pd.DataFrame(
            _timelines,
            columns=[
                PaymentStoryColumns.FirstPaymentId.name,
                PaymentStoryColumns.EntityId.name,
                PaymentStoryColumns.BeginsAt.name,
                PaymentStoryColumns.EndsAt.name,
                PaymentStoryColumns.PaymentsList.name,
                PaymentStoryColumns.BeginsWithCreditStatus.name,
                PaymentStoryColumns.EndsWithCreditStatus.name,
                PaymentStoryColumns.LaterDebtMinDaysToValidFrom.name
            ]
        )

    def _prepare_timeline(self, payments: list[Payment], dividing_rc: int) -> tuple:
        return (
            payments[0].p_id(),
            payments[0].entity_id(),
            payments[0].due_date(),
            payments[-1].due_date(),
            [(p.delay_days(), p.amount(), (p.due_date() - payments[0].due_date()).days) for p in payments],
            payments[0].prior_max_rc(),
            dividing_rc,
            payments[-1].days_to_debts()
        )




