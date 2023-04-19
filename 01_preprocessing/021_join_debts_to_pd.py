import pathlib
import pyarrow.parquet as pq
import sys
from rich import print
from rich.console import Console
from datetime import datetime
from input_const import *

console = Console()

if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the set of files')
        exit(1)

    _pd_file = DIR_INPUT / f'{PREFIX_PAY_DELAY}_{sys.argv[1]}{EXTENSION_PARQUET}'
    _debt_file = DIR_INPUT / f'{PREFIX_DEBTS}_{sys.argv[1]}{EXTENSION_PARQUET}'

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_pd_file}', spinner="bouncingBall"):
        pay_delay = pq.read_table(_pd_file)
    print(f'[green]File {_pd_file} loaded in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_debt_file}', spinner="bouncingBall"):
        debt = pq.read_table(_debt_file)
    print(f'[green]File {_debt_file} loaded in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

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
    print(f'[green]Payment-delay and debts joined in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    pay_delay_with_debts.take(list(range(100))).to_pandas()
