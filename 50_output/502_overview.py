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


def tab_all_paydelay_overview(input_code: str, name: str) -> Path:
    pdelay_full = pq.read_table(pay_delay_with_debts_file(input_code))

    count_all = pdelay_full.num_rows
    count_entities = pc.count_distinct(pdelay_full.column(PayDelayColumns.EntityId.name)).as_py()
    avg_delay = pc.mean(pdelay_full.column(PayDelayColumns.DelayDays.name)).as_py()
    count_non_empty_amount = pdelay_full.filter(pc.field(PayDelayColumns.InvoicedAmount.name).is_valid()).num_rows
    count_delay_0 = pdelay_full.filter(pc.field(PayDelayColumns.DelayDays.name) == 0).num_rows

    _out_file_path = tex_tab_file(name)

    with open(_out_file_path, 'w', newline='') as f:
        f.write(
            _correct_tab(
                pd.DataFrame({
                    "All records count": [count_all],
                    "Entities count": [count_entities],
                    "Avg delay (days)": [avg_delay],
                    r"Delay = 0 (\%)": [100.0*count_delay_0/count_all],
                    r"Non-empty amount (\%)": [100.0*count_non_empty_amount/count_all]
                }).to_latex(float_format="%.2f", label=f"tab:{name}", index=False, caption="Whole data set overview")
            )
        )
    return _out_file_path


def tab_sources_overview(input_code: str, name: str) -> Path:
    report = pd.read_csv(report_overview_file(input_code), index_col=[0])

    COL_COUNT_ALL = 'Records (k)'
    COL_OUTLIERS = 'Outliers (\%)'
    COL_PER_ENTITY = 'Per entity'
    # COL_RISK_RATE = 'Mild risk rate'
    COL_AVG_DELAY = r'Delay $\mi$'
    COL_STDDEV_DELAY = r'Delay $\sigma$'
    COL_AMOUNT_MISSING = 'Miss. amount (\%)'

    report[COL_COUNT_ALL] = report[OverviewReportColNames.RecordsCountAll] / 1000.0
    report[COL_OUTLIERS] = 100.0 * (report[OverviewReportColNames.RecordsCountAll] - report[
        OverviewReportColNames.RecordsCountWithoutOutliers]) / report[OverviewReportColNames.RecordsCountAll]
    report[COL_PER_ENTITY] = report[OverviewReportColNames.RecordsCountWithoutOutliers] / report[
        OverviewReportColNames.EntitiesCount]
    report[COL_AVG_DELAY] = report[OverviewReportColNames.PaymentDaysMean]
    report[COL_STDDEV_DELAY] = report[OverviewReportColNames.PaymentDaysStddev]
    report[COL_AMOUNT_MISSING] = 100.0 * report[OverviewReportColNames.AmountUnknownCount] / report[
        OverviewReportColNames.RecordsCountWithoutOutliers]

    report = report[[
        COL_COUNT_ALL,
        COL_OUTLIERS,
        COL_PER_ENTITY,
        COL_AVG_DELAY,
        COL_STDDEV_DELAY,
        COL_AMOUNT_MISSING]
    ].sort_values([COL_COUNT_ALL], ascending=False).head(50)

    _out_file_path = tex_tab_file(name)

    with open(_out_file_path, 'w', newline='', encoding='UTF-8') as f:
        f.write(
            _correct_tab(
                report.to_latex(
                    float_format="%.2f",
                    label=f"tab:{name}",
                    index=True,
                    caption="Sources overview (sorted: more active sources come first)")
            )
        )
    return _out_file_path


def fig_histogram_all_delays(input_code: str, name: str) -> Path:
    pdelay_full = pq.read_table(pay_delay_with_debts_file(input_code))
    _bins_lowers = range(-33, 31)
    _bins = [
        (pdelay_full.filter(
            (pc.field(PayDelayColumns.DelayDays.name) > _from) & (pc.field(PayDelayColumns.DelayDays.name) <= _to)
        ).num_rows, _to)
        for _from, _to in zip(_bins_lowers[:-1], _bins_lowers[1:])
    ]
    _first = pdelay_full.filter(pc.field(PayDelayColumns.DelayDays.name) <= min(_bins_lowers)).num_rows
    _last = pdelay_full.filter(pc.field(PayDelayColumns.DelayDays.name) > max(_bins_lowers)).num_rows
    _bins = [(_first, min(_bins_lowers))] + _bins + [(_last, max(_bins_lowers) + 1)]

    fig, ax = plt.subplots(figsize=(6.5, 4))
    ax.bar(
        [_b[1] for _b in _bins],
        [100 * _b[0] / pdelay_full.num_rows for _b in _bins],
        width=0.7, color='gray', edgecolor='white'
    )
    ax.set_ylim(0, 10)
    ax.set_xlim(-32, 30)
    ax.set_xlabel("Delay (days)")
    ax.set_ylabel("%")

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


def fig_sources_rel_typical_buyer(input_code: str, name: str) -> Path:
    report = pd.read_csv(report_overview_file(input_code), index_col=[0])
    having_amount = report.loc[
        (report[OverviewReportColNames.AmountUnknownCount] / report[OverviewReportColNames.RecordsCountWithoutOutliers])
        < 0.1]
    fig, axs = plt.subplots(1, 2, figsize=(6.5, 3), gridspec_kw={"wspace": 0.05})
    for ax in axs:
        ax.scatter(
            x=having_amount[OverviewReportColNames.PaymentDaysMean],
            y=having_amount[OverviewReportColNames.AmountMean],
            s=(having_amount[OverviewReportColNames.RecordsCountWithoutOutliers]) / 20000,
            c='gray',
            alpha=0.5
        )
        ax.grid(True, color='silver')
        # ax.set_xlabel("Average delay (days)")

    axs[0].set_ylabel("Average amount (CHF)")
    axs[1].set_ylim(0, 500)
    axs[1].set_xlim(-20, 10)
    axs[1].yaxis.tick_right()
    axs[1].set_xlabel(f"Zoomed-in", fontsize='xx-small')
    fig.text(0.5, 0.02, 'Average delay (days)', ha='center', va='center')

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


def fig_sources_rel_variability(input_code: str, name: str):
    report = pd.read_csv(report_overview_file(input_code), index_col=[0])
    having_amount = report.loc[
        (report[OverviewReportColNames.AmountUnknownCount] / report[OverviewReportColNames.RecordsCountWithoutOutliers])
        < 0.1]
    fig, axs = plt.subplots(1, 2, figsize=(6.5, 3.5), gridspec_kw={"wspace": 0.05})
    axs[0].scatter(
        x=having_amount[OverviewReportColNames.PaymentDaysStddev],
        y=having_amount[OverviewReportColNames.AmountStandardDeviation],
        s=(having_amount[OverviewReportColNames.RecordsCountWithoutOutliers]) / 20000,
        c='gray',
        alpha=0.5
    )
    axs[0].set_ylabel("Amount $\sigma$ (CHF)")
    axs[0].set_xlabel("Delay $\sigma$ (days)")
    axs[0].grid(True, color='silver')

    axs[1].scatter(
        x=report[OverviewReportColNames.PaymentDaysMean],
        y=report[OverviewReportColNames.PaymentDaysStddev],
        s=(report[OverviewReportColNames.RecordsCountWithoutOutliers]) / 20000,
        c='gray',
        alpha=0.5
    )
    axs[1].tick_params(axis='y', right=True, labelright=True, labelleft=False, left=False)
    axs[1].yaxis.set_label_position("right")
    axs[1].set_ylabel("Delay $\sigma$ (days)")
    axs[1].set_xlabel("Delay $\mu$ (days)")
    axs[1].grid(True, color='silver')

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


def fig_sources_rel_risk_rate(input_code: str, name: str):
    report = pd.read_csv(report_overview_file(input_code), index_col=[0])
    report = report.loc[report[OverviewReportColNames.EntitiesCount] > 1000]

    fig, axs = plt.subplots(1, 2, figsize=(6.5, 3.5), gridspec_kw={"wspace": 0.05})
    axs[0].scatter(
        x=report[OverviewReportColNames.PaymentDaysMean],
        y=100.0 * report[OverviewReportColNames.EntitiesWithLaterDebt] / report[OverviewReportColNames.EntitiesCount],
        s=(report[OverviewReportColNames.RecordsCountWithoutOutliers]) / 20000,
        c='gray',
        alpha=0.5
    )
    axs[0].set_ylabel("Mild risk rate")
    axs[0].set_xlabel("Delay $\sigma$ (days)")
    axs[0].grid(True, color='silver')

    axs[1].scatter(
        x=report[OverviewReportColNames.PaymentDaysMean],
        y=100.0 * report[OverviewReportColNames.EntitiesWithLaterSevereDebt] / report[
            OverviewReportColNames.EntitiesCount],
        s=(report[OverviewReportColNames.RecordsCountWithoutOutliers]) / 20000,
        c='gray',
        alpha=0.5
    )
    axs[1].tick_params(axis='y', right=True, labelright=True, labelleft=False, left=False)
    axs[1].yaxis.set_label_position("right")
    axs[1].set_ylabel("Significant risk rate")
    axs[1].set_xlabel("Delay $\sigma$ (days)")
    axs[1].grid(True, color='silver')

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


def fig_story_example(input_code: str, name: str, source_codename: str, story_id: int) -> Path:
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
        marker=".", color='gray', label='payments'
    )
    ax.plot(
        [0, duration],
        [mean, mean],
        color='gray', linestyle='--', lw=0.75, label='mean'
    )
    ax.plot(
        [0, duration],
        [a0, a0 + a1*duration],
        color='black', linestyle='dotted', lw=0.75, label='regression line (trend)'
    )
    if story[PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name] is not None \
            and story[PaymentStoriesColumns.LaterDebtMinDaysToValidFrom.name] > 0:
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

    # print('[blue]Calculating whole dataset overview table')
    # _f = tab_all_paydelay_overview(_input_code, "000_overview_whole_dataset")
    # print(f'[green]Whole dataset overview table written to {_f.absolute()}')

    print('[blue]Printing histogram of delays')
    _f = fig_histogram_all_delays(_input_code, "001_delay_histogram")
    print(f'[green]Histogram of delay-days written to {_f.absolute()}')

    # print('[blue]Calculating sources overview table')
    # _f = tab_sources_overview(_input_code, "100_overview_sources")
    # print(f'[green]Sources overview table written to {_f.absolute()}')

    # print('[blue]Preparing sources relations on typical payer')
    # _f = fig_sources_rel_typical_buyer(_input_code, "101_src_rel_typical_payer")
    # print(f'[green]Typical payers of sources stored in {_f.absolute()}')

    # print('[blue]Preparing sources relations on typical payer')
    # _f = fig_sources_rel_variability(_input_code, "102_src_rel_variability")
    # print(f'[green]Typical payers of sources stored in {_f.absolute()}')
    #
    # print('[blue]Preparing sources relations on risk rate')
    # _f = fig_sources_rel_risk_rate(_input_code, "103_src_rel_risk_rate")
    # print(f'[green]Risk rates in function of average delay of sources stored in {_f.absolute()}')

    # print('[blue]Preparing examples of stories')
    # _f = fig_story_example(_input_code, "104_story_example_positive", 'Mentdis', 116805594)
    # print(f'[green]Positive example of story wrote to file {_f.absolute()}')
    # _f = fig_story_example(_input_code, "105_story_example_negative", 'Mentdis', 116806281)
    # print(f'[green]Negative example of story wrote to file {_f.absolute()}')

