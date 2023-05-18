import sys
import sys
sys.path.append('../')

from datetime import datetime

from rich import print
from rich.console import Console

from lib.util import report_processing
from lib.paystories import *

console = Console()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the group of files to be processed')
        exit(1)

    _input_code = sys.argv[1]
    _single_source = None if len(sys.argv) < 3 else sys.argv[2]

    if _single_source is None:
        # remove files from previous execution(s)
        print(f'[green]Removing existing files with payment stories from {DIR_PROCESSING.absolute()}')
        for _psf in PaymentStoriesDirectory(DIR_PROCESSING).file_names():
            _psf.file(DIR_PROCESSING).unlink()
            print(f'[red]{_psf.codename()} deleted')

    for grouped_payments in PaymentsGroupedDirectory(DIR_PROCESSING).file_names():
        if _single_source is not None and _single_source != grouped_payments.codename():
            continue

        stories_builder = PaymentStoriesBuilder(grouped_payments.file(DIR_PROCESSING), grouped_payments.codename())

        _mark = datetime.now()
        with console.status(f'[blue]Loading {stories_builder.source_codename}', spinner="bouncingBall"):
            _content = stories_builder.payments()
        report_processing(f"Grouped payments for source {stories_builder.source_codename} loaded", _mark, _content)

        print(f'<{grouped_payments.codename()}> '
              f'Delay: mean: {stories_builder.delay_mean()}, stddev: {stories_builder.delay_stddev()} | '
              f'Amount: median: {stories_builder.amount_median()}, IQR: {stories_builder.amount_quantile_range()}')

        _mark = datetime.now()
        with console.status(f'[blue]Scaling delay and amount, calculating severity', spinner="bouncingBall"):
            stories_builder.scaled_delays()
            stories_builder.scaled_amount()
            _content = stories_builder.severity()
        report_processing(f"Delay and amount scaled, severity calculated", _mark, _content)

        _mark = datetime.now()
        with console.status(f'[blue]Calculating timeline of the stories', spinner="bouncingBall"):
            _content = stories_builder.story_timeline()
        report_processing(f"Story timeline calculated", _mark, _content)

        _mark = datetime.now()
        with console.status(f'[blue]Building up stories', spinner="bouncingBall"):
            _content = stories_builder.stories()
        report_processing(f"Stories built up", _mark, _content)

        _mark = datetime.now()
        with console.status(f'[blue]Discovering tendencies', spinner="bouncingBall"):
            _content = stories_builder.tendencies()
        report_processing(f"Tendencies found and evaluated", _mark, _content)

        _mark = datetime.now()
        with console.status(f'[blue]Writing stories', spinner="bouncingBall"):
            _file = stories_builder.write_stories(_input_code)
        print(f'[green]Stories wrote to '
              f'{_file} in {(datetime.now() - _mark).total_seconds():.1f} s')

        _mark = datetime.now()
        with console.status(f'[blue]Updating payment groups', spinner="bouncingBall"):
            _file = stories_builder.update_payment_groups(_input_code)
        print(f'[green]Payment groups updated, wrote to '
              f'{_file} in {(datetime.now() - _mark).total_seconds():.1f} s')

    print('[green]DONE')
