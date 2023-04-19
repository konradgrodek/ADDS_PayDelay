import pathlib
import pyarrow.parquet as pq
from pyarrow import csv
import sys
from rich import print
from rich.console import Console
from datetime import datetime
from input_const import *

console = Console()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the file with debts information')
        exit(1)

    _input_csv_path = DIR_INPUT / f'{PREFIX_DEBTS}_{sys.argv[1]}.csv'
    _output_parquet_path = DIR_INPUT / f'{PREFIX_DEBTS}_{sys.argv[1]}{EXTENSION_PARQUET}'

    if not _input_csv_path.exists():
        print(f'[red]The input file {_input_csv_path} does not exist')
        exit(1)

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_input_csv_path}', spinner="bouncingBall"):
        debts = csv.read_csv(
            _input_csv_path,
            convert_options=csv.ConvertOptions(column_types=DebtColumns.InputColumnTypes))
    print(f'[green]File {_input_csv_path} loaded in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    _mark = datetime.now()
    with console.status(f'[blue]Generating debt id', spinner="bouncingBall"):
        debts = debts.add_column(
            0, pa.field(*DebtColumns.Id), pa.array(range(1, debts.num_rows+1), type=DebtColumns.Id.otype))
    print(f'[green]Id generated in {(datetime.now() - _mark).total_seconds():.1f} s')

    _mark = datetime.now()
    with console.status(f'[blue]Converting info-type to categorical values', spinner="bouncingBall"):
        debts = debts.set_column(
            debts.schema.get_field_index(DebtColumns.InfoType.name),
            DebtColumns.InfoType.name,
            debts.column(DebtColumns.InfoType.name).dictionary_encode())
    print(f'[green]Info-type converted to categorical values in {(datetime.now() - _mark).total_seconds():.1f} s')

    _mark = datetime.now()
    with console.status(f'[blue]Converting credit-status to categorical values', spinner="bouncingBall"):
        debts = debts.set_column(
            debts.schema.get_field_index(DebtColumns.CreditStatus.name),
            DebtColumns.CreditStatus.name,
            debts.column(DebtColumns.CreditStatus.name).dictionary_encode())
    print(f'[green]Credit-status converted to categorical values in {(datetime.now() - _mark).total_seconds():.1f} s')

    _mark = datetime.now()
    with console.status(f'[blue]Storing data in parquet file {_output_parquet_path}', spinner="bouncingBall"):
        pq.write_table(debts, _output_parquet_path)
    print(f'[green]Parquet file stored in {(datetime.now() - _mark).total_seconds():.1f} s')
