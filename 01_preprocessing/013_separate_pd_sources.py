import pyarrow.compute as pc
import pyarrow.parquet as pq
from rich import print
from rich.console import Console
import sys
import csv
from datetime import datetime
from input_const import *
from lib.util import CodenameGen

console = Console()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the file with pay-delay information')
        exit(1)

    _input_parquet_path = DIR_INPUT / f'{PREFIX_PAY_DELAY}_{sys.argv[1]}{EXTENSION_PARQUET}'

    if not _input_parquet_path.exists():
        print(f'[red]The input file {_input_parquet_path.absolute()} does not exist')
        exit(1)

    print(f'[green]Removing existing per-source parquet files from {DIR_PROCESSING.absolute()}')
    for _f in DIR_PROCESSING.iterdir():
        if _f.name.startswith(PREFIX_PAY_DELAY):
            _f.unlink()
            print(f'[red]File {_f.absolute()} deleted')

    _sources_codenames_cache = DIR_PROCESSING / '_src_codenames.cache'

    _sources_codenames = {}
    if _sources_codenames_cache.exists():
        with open(_sources_codenames_cache, 'r') as _file:
            for _row in csv.reader(_file):
                _sources_codenames[int(_row[0])] = _row[1]

    _mark = datetime.now()
    with console.status(f'[blue]Loading file {_input_parquet_path}', spinner="bouncingBall"):
        pdelay_full = pq.read_table(_input_parquet_path)

    print(f'[green]File {_input_parquet_path} loaded in {(datetime.now() - _mark).total_seconds():.1f} s. '
          f'Memory consumed: [red]{pa.total_allocated_bytes()/(1024*1024*1024):.1f} GB')

    # isolate unique sources, count records for each source
    sources_counted = pdelay_full.group_by(PayDelayColumns.DataSource.name).\
        aggregate([(PayDelayColumns.DataSource.name, 'count')]).to_pandas().set_index(PayDelayColumns.DataSource.name)

    sources_counted = sources_counted.sort_values(f'{PayDelayColumns.DataSource.name}_count')
    codenames = CodenameGen(seed=2023)
    new_codename_found = False
    for _src, _size in sources_counted.itertuples():
        if _src not in _sources_codenames:
            codename_len = 5 if _size >= 10000000 \
                else 6 if 10000000 > _size >= 1000000 \
                else 7 if 1000000 > _size >= 10000 \
                else 8
            codename = codenames.generate(fixed_length=codename_len, style=CodenameGen.STYLE_TITLE)
            while codename in _sources_codenames.values():
                codename = codenames.generate(fixed_length=codename_len, style=CodenameGen.STYLE_TITLE)
            _sources_codenames[_src] = codename
            print(f'[red]{_src}\t{_size}\t{_sources_codenames[_src]}')
            new_codename_found = True
        else:
            print(f'{_src}\t{_size}\t{_sources_codenames[_src]}')
        # now store the separated sources
        _mark = datetime.now()
        with console.status(f'[blue]Storing source {_sources_codenames[_src]} ({_src})', spinner="bouncingBall"):
            _target_file = DIR_PROCESSING / f'{PREFIX_PAY_DELAY}_{_sources_codenames[_src]}.parquet'
            pq.write_table(
                pdelay_full.filter(pc.field(PayDelayColumns.DataSource.name) == _src),
                _target_file
            )
        print(f'[green]Source {_sources_codenames[_src]} stored to parquet in '
              f'{(datetime.now() - _mark).total_seconds():.1f} s.')

    # store the codenames
    if new_codename_found:
        with open(_sources_codenames_cache, 'w') as _file:
            csv.writer(_file).writerows([(_src, _sources_codenames[_src]) for _src in _sources_codenames])




