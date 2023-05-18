import sys
sys.path.append('../')
from lib.input_const import *
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd

import matplotlib as mpl
import matplotlib.pyplot as plt

from rich import print

mpl.use("pgf")
mpl.rcParams.update({
    "pgf.texsystem": "pdflatex",
    'font.family': 'serif',
    'text.usetex': True,
    'pgf.rcfonts': False,
})


def _correct_tab(tex: str) -> str:
    tex = tex[:len(r"\begin{table}")] + r'[htbp!] \centering' + tex[len(r"\begin{table}"):]
    tex = tex.replace(r"\toprule", r"\\[-1.8ex]\hline\hline \\[-1.8ex]")
    return tex


def fig_severity_shown(input_code: str, name: str, source_codename: str, story_id: int) -> Path:
    stories = pq.read_table(payment_stories_file(input_code, source_codename))
    groups = pq.read_table(payments_grouped_by_stories_file(input_code, source_codename))

    story = stories.filter(pc.field(PaymentStoriesColumns.StoryId.name) == story_id).to_pylist()[0]
    payments = groups.filter(pc.field(PaymentGroupsColumns.StoryId.name) == story_id).select([
        PaymentGroupsColumns.StoryTimeline.name,
        PaymentGroupsColumns.DelayDaysScaled.name,
        PaymentGroupsColumns.Severity.name
    ]).to_pydict()

    fig, ax = plt.subplots(figsize=(6.5, 3.6))

    for _tl, _d, _s in zip(payments[PaymentGroupsColumns.StoryTimeline.name],
                           payments[PaymentGroupsColumns.DelayDaysScaled.name],
                           payments[PaymentGroupsColumns.Severity.name]):
        if abs(_s - _d) > 0.1:
            ax.arrow(x=_tl, y=_d, dx=0, dy=_s - _d, color='red', length_includes_head=True, head_width=2.5,
                     head_length=0.1, lw=0.1)

    ax.scatter(
        x=payments[PaymentGroupsColumns.StoryTimeline.name],
        y=payments[PaymentGroupsColumns.DelayDaysScaled.name],
        marker=".", color='gray', label='delay'
    )
    ax.scatter(
        x=payments[PaymentGroupsColumns.StoryTimeline.name],
        y=payments[PaymentGroupsColumns.Severity.name],
        marker=".", color='green', label='severity'
    )

    ax.grid(axis='y', which='major', lw=0.25)
    ax.set_ylabel("Delay / Severity (scaled)")
    ax.set_xlabel("Days since the story begins")
    ax.legend(fontsize='xx-small')

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


def fig_h1_delay_mean(input_code: str, name: str, source_codename: str, story_id: int) -> Path:
    stories = pq.read_table(payment_stories_file(input_code, source_codename))
    groups = pq.read_table(payments_grouped_by_stories_file(input_code, source_codename))

    story = stories.filter(pc.field(PaymentStoriesColumns.StoryId.name) == story_id).to_pylist()[0]
    payments = groups.filter(pc.field(PaymentGroupsColumns.StoryId.name) == story_id).select([
        PaymentGroupsColumns.StoryTimeline.name,
        PaymentGroupsColumns.DelayDaysScaled.name
    ]).to_pydict()

    duration = story[PaymentStoriesColumns.Duration.name]
    mean = story[PaymentStoriesColumns.ScaledDelayMean.name]

    fig, ax = plt.subplots(figsize=(6.5, 3.6))
    ax.scatter(
        x=payments[PaymentGroupsColumns.StoryTimeline.name],
        y=payments[PaymentGroupsColumns.DelayDaysScaled.name],
        marker=".", color='gray', label='$d_i$'
    )
    ax.plot(
        [0, duration],
        [mean, mean],
        color='gray', linestyle='--', lw=0.75, label='$D_{\mu}$'
    )
    ax.arrow(
        x=1.05 * max(payments[PaymentGroupsColumns.StoryTimeline.name]),
        y=0,
        dx=0, dy=mean,
        color='red', length_includes_head=True,
        head_width=2.5, head_length=0.1, lw=0.5
    )

    if story[PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name] > 0:
        debt_x = max(payments[PaymentGroupsColumns.StoryTimeline.name]) + story[
            PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name]
        ax.plot(
            [debt_x, debt_x],
            [min(payments[PaymentGroupsColumns.DelayDaysScaled.name]),
             max(payments[PaymentGroupsColumns.DelayDaysScaled.name])],
            color='orange', lw=0.5, linestyle='-.', label='debt'
        )

    ax.grid(axis='y', which='major', lw=0.25)
    ax.set_yticks([0])
    ax.set_ylabel("Delay")
    ax.set_xlabel("Days since the story begins")
    ax.legend(fontsize='xx-small')

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


def fig_h3_delay_tendency(input_code: str, name: str, source_codename: str, story_id: int) -> Path:
    stories = pq.read_table(payment_stories_file(input_code, source_codename))
    groups = pq.read_table(payments_grouped_by_stories_file(input_code, source_codename))

    story = stories.filter(pc.field(PaymentStoriesColumns.StoryId.name) == story_id).to_pylist()[0]
    payments = groups.filter(pc.field(PaymentGroupsColumns.StoryId.name) == story_id).select([
        PaymentGroupsColumns.StoryTimeline.name,
        PaymentGroupsColumns.DelayDaysScaled.name
    ]).to_pydict()

    a0 = story[PaymentStoriesColumns.TendencyConstant_ForDelay.name]
    a1 = story[PaymentStoriesColumns.TendencyCoefficient_ForDelay.name]
    duration = story[PaymentStoriesColumns.Duration.name]
    mean = story[PaymentStoriesColumns.ScaledDelayMean.name]

    fig, ax = plt.subplots(figsize=(6.5, 3.6))
    ax.scatter(
        x=payments[PaymentGroupsColumns.StoryTimeline.name],
        y=payments[PaymentGroupsColumns.DelayDaysScaled.name],
        marker=".", color='gray', label='$d_i$'
    )
    ax.plot(
        [0, duration],
        [a0, a0 + a1 * duration],
        color='black', linestyle='dotted', lw=0.75, label='regression line'
    )
    if story[PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name] > 0:
        debt_x = max(payments[PaymentGroupsColumns.StoryTimeline.name]) + story[
            PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name]
        ax.plot(
            [debt_x, debt_x],
            [min(payments[PaymentGroupsColumns.DelayDaysScaled.name]),
             max(payments[PaymentGroupsColumns.DelayDaysScaled.name])],
            color='orange', lw=0.5, linestyle='-.', label='debt'
        )

    ax.grid(axis='y', which='major', lw=0.25)
    ax.set_yticks([0])
    ax.set_ylabel("Delay")
    ax.set_xlabel("Days since the story begins")
    ax.legend(fontsize='xx-small')

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


def fig_h5_tendency_value_explained(input_code: str, name: str, source_codename: str, story_id: int) -> Path:
    stories = pq.read_table(payment_stories_file(input_code, source_codename))
    groups = pq.read_table(payments_grouped_by_stories_file(input_code, source_codename))

    story = stories.filter(pc.field(PaymentStoriesColumns.StoryId.name) == story_id).to_pylist()[0]
    payments = groups.filter(pc.field(PaymentGroupsColumns.StoryId.name) == story_id).select([
        PaymentGroupsColumns.StoryTimeline.name,
        PaymentGroupsColumns.DelayDaysScaled.name
    ]).to_pydict()

    a0 = story[PaymentStoriesColumns.TendencyConstant_ForDelay.name]
    a1 = story[PaymentStoriesColumns.TendencyCoefficient_ForDelay.name]
    duration = story[PaymentStoriesColumns.Duration.name]
    mean = story[PaymentStoriesColumns.ScaledDelayMean.name]

    fig, ax = plt.subplots(figsize=(6.5, 3.6))
    ax.scatter(
        x=payments[PaymentGroupsColumns.StoryTimeline.name],
        y=payments[PaymentGroupsColumns.DelayDaysScaled.name],
        marker=".", color='gray', label='$d_i$'
    )
    ax.plot(
        [0, duration],
        [a0, a0 + a1 * duration],
        color='black', linestyle='dotted', lw=0.75, label='regression line'
    )

    ax.arrow(
        x=max(payments[PaymentGroupsColumns.StoryTimeline.name]),
        y=0,
        dx=0, dy=a0 + a1 * duration,
        color='red', length_includes_head=True,
        head_width=5, head_length=0.1, lw=0.5
    )

    if story[PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name] > 0:
        debt_x = max(payments[PaymentGroupsColumns.StoryTimeline.name]) + story[
            PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name]
        ax.plot(
            [debt_x, debt_x],
            [min(payments[PaymentGroupsColumns.DelayDaysScaled.name]),
             max(payments[PaymentGroupsColumns.DelayDaysScaled.name])],
            color='red', lw=0.5, linestyle='-.', label='debt'
        )

    ax.grid(axis='y', which='major', lw=0.25)
    ax.set_yticks([0])
    ax.set_ylabel("Delay")
    ax.set_xlabel("Days since the story begins")
    ax.legend(fontsize='xx-small')

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the group of files to be processed')
        exit(1)

    _input_code = sys.argv[1]

    # print('[blue]Preparing explanation of severity')
    # _f = fig_severity_shown(_input_code, "301_severity_explained", 'Mentdis', 116855050)
    # print(f'[green]Explanation of severity stored in {_f.absolute()}')

    # print('[blue]Preparing explanation of H1')
    # _f = fig_h1_delay_mean(_input_code, "302_h1_delay_mean_explained", 'Mentdis', 116854026)
    # print(f'[green]Explanation of hypothesis H1 (delay mean) stored in {_f.absolute()}')

    # print('[blue]Preparing explanation of H3')
    # _f = fig_h3_delay_tendency(_input_code, "303_h3_tendency_coefficient_explained", 'Mentdis', 116861844)
    # print(f'[green]Explanation of hypothesis H3 (tendency coefficient) stored in {_f.absolute()}')

    print('[blue]Preparing explanation of H5')
    _f = fig_h5_tendency_value_explained(_input_code, "304_h5_tendency_value_explained", 'Mentdis', 116545831)
    print(f'[green]Explanation of hypothesis H5 (tendency value) stored in {_f.absolute()}')
