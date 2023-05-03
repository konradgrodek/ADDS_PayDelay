import sys
from datetime import datetime

import pandas as pd
from rich import print
from rich.console import Console

from lib.input_const import PayDelayWithDebtsDirectory, DIR_PROCESSING, payment_stories_file
from lib.timeline import *


console = Console()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the group of files to be processed')
        exit(1)

    _input_code = sys.argv[1]

    for pd_source_file in PayDelayWithDebtsDirectory(DIR_PROCESSING).pay_delay_file_names():
        timeline_builder = PaymentHistoryBuilder(pd_source_file.file(DIR_PROCESSING), pd_source_file.codename())
        _mark = datetime.now()
        with console.status(f'[blue]Loading {timeline_builder.source_codename}', spinner="bouncingBall"):
            _content = timeline_builder.content()
        print(f'[green]Source {timeline_builder.source_codename} '
              f'loaded in {(datetime.now()-_mark).total_seconds():.1f} s, {len(_content)} records')

        _mark = datetime.now()
        with console.status(f'[blue]Grouping {timeline_builder.source_codename}', spinner="bouncingBall"):
            _grouped = timeline_builder.build_timelines()
        print(f'[green]Payment stories {timeline_builder.source_codename} '
              f'calculated in {(datetime.now()-_mark).total_seconds():.1f} s, {len(_grouped)} records')

        _mark = datetime.now()
        with console.status(f'[blue]Storing {timeline_builder.source_codename} payment-stories', spinner="bouncingBall"):
            _grouped.to_parquet(payment_stories_file(_input_code, pd_source_file.codename()))
        print(f'[green]Payment stories for {timeline_builder.source_codename} '
              f'stored in parquet in {(datetime.now()-_mark).total_seconds():.1f} s')
