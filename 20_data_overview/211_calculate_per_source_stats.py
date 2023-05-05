import sys
from datetime import datetime

from rich import print
from rich.console import Console

from lib.input_const import PayDelayWithDebtsDirectory, DIR_PROCESSING, report_overview_file
from lib.util import report_processing
from lib.statistics import *


console = Console()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the group of files to be processed')
        exit(1)

    _input_code = sys.argv[1]
    _reports = list()

    for pd_source_file in PayDelayWithDebtsDirectory(DIR_PROCESSING).file_names():
        statistics = PayDelayStatistics(pd_source_file.file(basedir=DIR_PROCESSING), pd_source_file.codename())

        _mark = datetime.now()
        with console.status(f'[blue]Loading {statistics.source_codename}', spinner="bouncingBall"):
            _content = statistics.content()
        report_processing(f'File {pd_source_file.file_name()} loaded', _mark, _content)

        _mark = datetime.now()
        with console.status(f'[blue]Processing {statistics.source_codename}', spinner="bouncingBall"):
            _reports.append(statistics.report())
        print(f'[green]Source {statistics.source_codename} processed in {(datetime.now()-_mark).total_seconds():.1f} s')

    pd.concat(_reports).to_csv(report_overview_file(_input_code))

