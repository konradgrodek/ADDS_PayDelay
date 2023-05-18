import sys
sys.path.append('../')
from datetime import datetime

from rich import print
from rich.console import Console

from lib.input_const import PayDelayWithDebtsDirectory, PaymentsGroupedDirectory, DIR_PROCESSING
from lib.util import report_processing
from lib.paystories import *


console = Console()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the group of files to be processed')
        exit(1)

    _input_code = sys.argv[1]

    # remove files from previous execution(s)
    print(f'[green]Removing existing files with per-source grouped payments from {DIR_PROCESSING.absolute()}')
    for _pdf in PaymentsGroupedDirectory(DIR_PROCESSING).file_names():
        _pdf.file(DIR_PROCESSING).unlink()
        print(f'[red]{_pdf.codename()} deleted')

    for pd_source_file in PayDelayWithDebtsDirectory(DIR_PROCESSING).file_names():
        timelines_grouper = PaymentHistoryGrouper(pd_source_file.file(DIR_PROCESSING), pd_source_file.codename())

        _mark = datetime.now()
        with console.status(f'[blue]Loading {timelines_grouper.source_codename}', spinner="bouncingBall"):
            _content = timelines_grouper.content()
        report_processing(f"Source {timelines_grouper.source_codename} loaded", _mark, _content)

        _mark = datetime.now()
        with console.status(f'[blue]Detecting story-dividing events', spinner="bouncingBall"):
            _div = timelines_grouper.detect_dividers()
        report_processing(f"Dividing-events detected", _mark, _div)

        _mark = datetime.now()
        with console.status(f'[blue]Generating story-ids', spinner="bouncingBall"):
            _sids = timelines_grouper.calculate_story_ids()
        report_processing(f"Story-ids generated", _mark, _sids)

        _mark = datetime.now()
        with console.status(f'[blue]Preparing final results and writing to parquet', spinner="bouncingBall"):
            _final = timelines_grouper.combine_and_store(_input_code)
        report_processing(f"Final results put in shape and stored to parquet", _mark, _final)

