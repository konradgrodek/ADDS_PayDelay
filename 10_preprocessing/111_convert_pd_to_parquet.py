import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import csv
import sys
from rich import print
from rich.console import Console
from datetime import datetime
from lib.input_const import *

console = Console()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the file with pay-delay information')
        exit(1)

    _input_code = sys.argv[1]

    _input_csv_path = pay_delay_ori_file(_input_code)
    _output_parquet_path = pay_delay_file(_input_code)

    if not _input_csv_path.exists():
        print(f'[red]The input file {_input_csv_path} does not exist')
        exit(1)

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_input_csv_path}', spinner="bouncingBall"):
        pdelay_full = csv.read_csv(_input_csv_path,
                                   convert_options=csv.ConvertOptions(column_types=PayDelayColumns.InputColumnTypes))
    print(f'[green]File {_input_csv_path} loaded in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    _mark = datetime.now()
    with console.status(f'[blue]Sorting', spinner="bouncingBall"):
        pdelay_full = pdelay_full.sort_by(
            [
                (PayDelayColumns.DataSource.name, 'ascending'),
                (PayDelayColumns.EntityId.name, 'ascending'),
                (PayDelayColumns.DueDate.name, 'ascending')
            ]
        )
    print(f'[green]Payment delays sorted in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    _mark = datetime.now()
    with console.status(f'[blue]Generating payment delay id', spinner="bouncingBall"):
        pdelay_full = pdelay_full.add_column(
            0, pa.field(*PayDelayColumns.Id), pa.array(range(1, pdelay_full.num_rows+1), type=PayDelayColumns.Id.otype))
    print(f'[green]Id generated in {(datetime.now() - _mark).total_seconds():.1f} s')

    _mark = datetime.now()
    with console.status(f'[blue]Converting industry to categorical values', spinner="bouncingBall"):
        pdelay_full = pdelay_full.set_column(
            pdelay_full.schema.get_field_index(PayDelayColumns.Industry.name),
            PayDelayColumns.Industry.name,
            pdelay_full.column(PayDelayColumns.Industry.name).dictionary_encode())
    print(f'[green]Industry converted to categorical values in {(datetime.now() - _mark).total_seconds():.1f} s')

    _mark = datetime.now()
    with console.status(f'[blue]Converting gender to categorical values', spinner="bouncingBall"):
        pdelay_full = pdelay_full.set_column(
            pdelay_full.schema.get_field_index(PayDelayColumns.Sex.name),
            PayDelayColumns.Sex.name,
            pdelay_full.column(PayDelayColumns.Sex.name).dictionary_encode())
    print(f'[green]Gender converted to categorical values in {(datetime.now() - _mark).total_seconds():.1f} s')

    _mark = datetime.now()
    with console.status(f'[blue]Calculating age at due-date', spinner="bouncingBall"):
        pdelay_full = pdelay_full.append_column(
            PayDelayColumns.Age.name,
            pc.cast(
                pc.divide(
                    pc.subtract(
                        pc.add(
                            pc.multiply(pc.year(pdelay_full.column(PayDelayColumns.DueDate.name)), 10000),
                            pc.add(
                                pc.multiply(pc.month(pdelay_full.column(PayDelayColumns.DueDate.name)), 100),
                                pc.day(pdelay_full.column(PayDelayColumns.DueDate.name)))
                        ),
                        pdelay_full.column(PayDelayColumns.BirthDateInt.name)
                    ), 10000
                ), options=pc.CastOptions(target_type=pa.int16())
            )
        )
    print(f'[green]Age of entities calculated in {(datetime.now() - _mark).total_seconds():.1f} s')

    _mark = datetime.now()
    with console.status(f'[blue]Marking outliers', spinner="bouncingBall"):
        pdelay_full = pdelay_full.append_column(
            PayDelayColumns.IsOutlier.name,
            pc.or_(
                pc.less(pdelay_full.column(PayDelayColumns.DelayDays.name), OUTLIER__MIN_DELAY),
                pc.greater(pdelay_full.column(PayDelayColumns.DelayDays.name), OUTLIER__MAX_DELAY)
            )
        )
    print(f'[green]Outliers found in {(datetime.now() - _mark).total_seconds():.1f} s')

    # the amount, even if appearing to be incorrect, will be ignored, not marked as outlier
    # a pay-delay with unknown amount is useful information, the pay-delay without delay is useless
    _mark = datetime.now()
    with console.status(f'[blue]Cleaning amount', spinner="bouncingBall"):
        pdelay_full = pdelay_full.set_column(
            pdelay_full.schema.get_field_index(PayDelayColumns.InvoicedAmount.name),
            PayDelayColumns.InvoicedAmount.name,
            pc.if_else(
                pc.less_equal(pdelay_full.column(PayDelayColumns.InvoicedAmount.name), 0),
                pa.nulls(pdelay_full.num_rows),
                pdelay_full.column(PayDelayColumns.InvoicedAmount.name)
            )
        )
    print(f'[green]Amount cleaned in {(datetime.now() - _mark).total_seconds():.1f} s')

    pdelay_full = pdelay_full.remove_column(pdelay_full.schema.get_field_index(PayDelayColumns.BirthDateInt.name))
    print(f'[blue]Birthdate column removed')

    _mark = datetime.now()
    with console.status(f'[blue]Storing data in parquet file {_output_parquet_path}', spinner="bouncingBall"):
        pq.write_table(pdelay_full, _output_parquet_path)
    print(f'[green]Parquet file stored in {(datetime.now() - _mark).total_seconds():.1f} s')

    # dataset = ds.dataset(_input_csv_path, format='csv')
    # print(dataset.head(10))
    # print(dataset.count_rows())
    # print(dataset.schema)
    # print(dataset.scanner(columns=['data_source_id']).to_table().column(0).unique())
    # _src = dataset.scanner(filter=pc.field('data_source_id') == 717)
    # print(_src.head(100))
    # print(_src.count_rows())



