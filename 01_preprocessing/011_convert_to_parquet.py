import pathlib
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa
from pyarrow import csv
import sys
from rich import print
from rich.console import Console
from datetime import datetime

console = Console()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input file path')
        exit(1)

    _input_csv_path = pathlib.Path(sys.argv[1])
    _output_parquet_path = pathlib.Path(sys.argv[1].rstrip('.csv') + '.parquet')

    if not _input_csv_path.exists():
        print(f'[red]The provided input file {_input_csv_path} does not exist')
        exit(1)

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_input_csv_path}', spinner="bouncingBall"):
        pdelay_full = csv.read_csv(_input_csv_path,
                                   convert_options=csv.ConvertOptions(column_types={
                                        "legal_entity_id": pa.uint32(),
                                        "due_date": pa.timestamp('s'),
                                        "delay_days": pa.int32(),
                                        "invoiced_amount": pa.float32(),
                                        "industry": pa.string(),
                                        "data_source_id": pa.uint16(),
                                        "birth_date_int": pa.uint32(),
                                        "sex": pa.uint8()
                                    }))
    print(f'[green]File {_input_csv_path} loaded in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    _mark = datetime.now()
    with console.status(f'[blue]Converting industry to categorical values', spinner="bouncingBall"):
        pdelay_full = pdelay_full.set_column(
            pdelay_full.schema.get_field_index('industry'),
            "industry",
            pdelay_full.column('industry').dictionary_encode())
    print(f'[green]Industry converted to categorical values in {(datetime.now() - _mark).total_seconds():.1f} s')

    _mark = datetime.now()
    with console.status(f'[blue]Converting gender to categorical values', spinner="bouncingBall"):
        pdelay_full = pdelay_full.set_column(
            pdelay_full.schema.get_field_index('sex'),
            "sex",
            pdelay_full.column('sex').dictionary_encode())
    print(f'[green]Gender converted to categorical values in {(datetime.now() - _mark).total_seconds():.1f} s')

    _mark = datetime.now()
    with console.status(f'[blue]Casting timestamp of due-date to date only', spinner="bouncingBall"):
        pdelay_full = pdelay_full.set_column(
            pdelay_full.schema.get_field_index('due_date'),
            "due_date",
            pc.cast(pdelay_full.column('due_date'),
                    options=pc.CastOptions(target_type=pa.date32(), allow_time_truncate=True)))
    print(f'[green]Due-date truncated to date32 type in {(datetime.now() - _mark).total_seconds():.1f} s')

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
