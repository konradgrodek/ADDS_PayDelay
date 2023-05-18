import pyarrow.parquet as pq
import pyarrow.compute as pc
import sys
from rich import print
from rich.console import Console
from datetime import datetime
from lib.input_const import *
from lib.util import report_processing

console = Console()

if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the set of files')
        exit(1)

    _input_code = sys.argv[1]

    _pd_file = DIR_INPUT / f'{PREFIX_PAY_DELAY}_{_input_code}{EXTENSION_PARQUET}'
    _debt_file = DIR_INPUT / f'{PREFIX_DEBTS}_{_input_code}{EXTENSION_PARQUET}'
    _output_pd = pay_delay_with_debts_file(_input_code)

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_pd_file}', spinner="bouncingBall"):
        pay_delay = pq.read_table(_pd_file)
    report_processing(f'File {_pd_file} loaded', _mark, pay_delay)

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_debt_file}', spinner="bouncingBall"):
        debt = pq.read_table(_debt_file)
    report_processing(f'File {_debt_file} loaded', _mark, debt)

    _mark = datetime.now()
    with console.status(f'[blue]Joining debts to pay-delay', spinner="bouncingBall"):
        pay_delay_with_debts = pay_delay.select(
            [PayDelayColumns.Id.name, PayDelayColumns.EntityId.name, PayDelayColumns.DueDate.name]).join(
            debt.select([
                DebtColumns.LiabilityOwner.name,
                DebtColumns.CreditStatus.name,
                DebtColumns.ValidFrom.name,
                DebtColumns.ValidTo.name]),
            keys=PayDelayColumns.EntityId.name,
            right_keys=DebtColumns.LiabilityOwner.name, join_type='left outer')
    debt = None
    report_processing(f'Payment-delay and debts joined', _mark, pay_delay_with_debts)

    _mark = datetime.now()
    with console.status(f'[blue]Selecting prior debts', spinner="bouncingBall"):
        pay_delay_with_prior_debts = pay_delay_with_debts.filter(
            (pc.field(PayDelayColumns.DueDate.name) > pc.field(DebtColumns.ValidFrom.name)) &
            (pc.field(PayDelayColumns.DueDate.name) < pc.field(DebtColumns.ValidTo.name))
        )
    report_processing(f'Prior debts selected', _mark, pay_delay_with_prior_debts)

    _mark = datetime.now()
    with console.status(f'[blue]Finding maximum credit status of prior debts', spinner="bouncingBall"):
        pay_delay = pay_delay.join(
            pay_delay_with_prior_debts.group_by(
                PayDelayColumns.Id.name
            ).aggregate(
                [(DebtColumns.CreditStatus.name, 'max')]
            ).rename_columns(
                [PayDelayColumns.Id.name, PayDelayColumns.PriorDebtsMaxCreditStatus.name]
            ),
            keys=PayDelayColumns.Id.name,
            right_keys=PayDelayColumns.Id.name,
            join_type='left outer'
        )
    pay_delay_with_prior_debts = None
    report_processing(f'Maximum credit status of prior debts found', _mark, pay_delay)

    _mark = datetime.now()
    with console.status(f'[blue]Selecting later debts', spinner="bouncingBall"):
        pay_delay_with_later_debts = pay_delay_with_debts.filter(
            pc.field(PayDelayColumns.DueDate.name) < pc.field(DebtColumns.ValidFrom.name)
        )
    pay_delay_with_debts = None
    report_processing(f'Later debts selected', _mark, pay_delay_with_later_debts)

    _mark = datetime.now()
    with console.status(f'[blue]Processing later debts', spinner="bouncingBall"):
        pay_delay = pay_delay.join(
            pay_delay_with_later_debts.group_by(
                PayDelayColumns.Id.name
            ).aggregate([
                (DebtColumns.CreditStatus.name, 'max'),
                (DebtColumns.CreditStatus.name, 'count')
            ]).rename_columns([
                PayDelayColumns.Id.name,
                PayDelayColumns.LaterDebtsMaxCreditStatus.name,
                PayDelayColumns.LaterDebtsCount.name
            ]),
            keys=PayDelayColumns.Id.name,
            right_keys=PayDelayColumns.Id.name,
            join_type='left outer'
        )
    report_processing(f'Later debts processed (counted, max cs found)', _mark, pay_delay)

    for credit_status in [1, 2, 3, 4]:
        _mark = datetime.now()
        with console.status(f'[blue]Finding min valid-from for credit-status {credit_status}', spinner="bouncingBall"):
            _col = PayDelayColumns.LaterDebtsMinDaysToValidFrom(credit_status)
            pay_delay = pay_delay.join(
                pay_delay_with_later_debts.filter(pc.field(DebtColumns.CreditStatus.name) == credit_status).group_by(
                    PayDelayColumns.Id.name
                ).aggregate(
                    [(DebtColumns.ValidFrom.name, 'min')]
                ).rename_columns(
                    [PayDelayColumns.Id.name, _col.name]
                ),
                keys=PayDelayColumns.Id.name,
                right_keys=PayDelayColumns.Id.name,
                join_type='left outer'
            )
            pay_delay = pay_delay.set_column(
                pay_delay.schema.get_field_index(_col.name),
                _col.name,
                pc.cast(
                    pc.days_between(
                        pay_delay.column(PayDelayColumns.DueDate.name),
                        pay_delay.column(_col.name)
                    ),
                    _col.otype
                )
            )
        report_processing(f'Minimum valid-from found for credit-status {credit_status}', _mark, pay_delay)

    _mark = datetime.now()
    with console.status(f'[blue]Storing enriched pay-delay in {_output_pd}', spinner="bouncingBall"):
        pq.write_table(pay_delay, _output_pd)
    print(f'[green]Enriched pay-delay file {_output_pd} stored in {(datetime.now() - _mark).total_seconds():.1f} s')
    print(f'[green]DONE')

