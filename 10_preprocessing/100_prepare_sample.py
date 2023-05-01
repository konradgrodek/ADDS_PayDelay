import pyarrow.compute as pc
from pyarrow import csv
import sys
from rich import print
from rich.console import Console
from datetime import datetime
from lib.input_const import *
import random

console = Console()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the files to process')
        exit(1)

    SAMPLE_SIZE = 1000

    _input_pd_csv_path = DIR_INPUT / f'{PREFIX_PAY_DELAY}_{sys.argv[1]}.csv'
    _output_pd_csv_path = DIR_INPUT / f'{PREFIX_PAY_DELAY}_SAMPLE.csv'
    _input_d_csv_path = DIR_INPUT / f'{PREFIX_DEBTS}_{sys.argv[1]}.csv'
    _output_d_csv_path = DIR_INPUT / f'{PREFIX_DEBTS}_SAMPLE.csv'

    if not _input_pd_csv_path.exists():
        print(f'[red]The input file {_input_pd_csv_path} does not exist')
        exit(1)
    if not _input_d_csv_path.exists():
        print(f'[red]The input file {_input_d_csv_path} does not exist')
        exit(1)

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_input_pd_csv_path}', spinner="bouncingBall"):
        pdelay_full = csv.read_csv(_input_pd_csv_path,
                                   convert_options=csv.ConvertOptions(column_types=PayDelayColumns.InputColumnTypes))
    print(f'[green]File {_input_pd_csv_path} loaded in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    random.seed(2023)

    _mark = datetime.now()
    with console.status(f'[blue]Selecting sample from payment_delay', spinner="bouncingBall"):
        i_subset = list()
        _step = pdelay_full.num_rows / SAMPLE_SIZE
        _nextpos = 0
        while True:
            _nextpos += random.randint(int(0.8 * _step), int(1.2 * _step))
            if _nextpos >= pdelay_full.num_rows:
                break
            i_subset.append(_nextpos)

        _entities = pdelay_full.take(i_subset).column(PayDelayColumns.EntityId.name).unique().to_pylist()
        pdelay_full = pdelay_full.filter(pc.field(PayDelayColumns.EntityId.name).isin(_entities))

    print(f'[green]Subset of {pdelay_full.num_rows} pay-delays for {len(_entities)} entities '
          f'was selected in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    csv.write_csv(pdelay_full, _output_pd_csv_path)
    print(f'[green]File {_output_pd_csv_path} wrote to disk')

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_input_pd_csv_path}', spinner="bouncingBall"):
        debts_full = csv.read_csv(_input_d_csv_path,
                                  convert_options=csv.ConvertOptions(column_types=DebtColumns.InputColumnTypes))
    print(f'[green]File {_input_d_csv_path} loaded in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    _mark = datetime.now()
    with console.status(f'[blue]Selecting debts for the legal entities', spinner="bouncingBall"):
        debts_full = debts_full.filter(pc.field(DebtColumns.LiabilityOwner.name).isin(_entities))
    print(f'[green]Subset of {debts_full.num_rows} debts was selected '
          f'in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    csv.write_csv(debts_full, _output_d_csv_path)
    print(f'[green] File {_output_d_csv_path} wrote to disk')
