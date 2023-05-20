import sys
sys.path.append('../')
from lib.input_const import *
from lib.perfeval import PaymentStoriesPerformanceEvaluator
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
        color='red', linestyle='dotted', lw=0.75, label='regression line'
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


def tab_sources_rocauc_overview(input_code: str, name: str) -> Path:
    report = pd.read_csv(report_predictors(input_code), index_col=[0]).merge(
        pd.read_csv(report_overview_file(input_code), index_col=[0]), left_index=True, right_index=True)
    _out_file_path = tex_tab_file(name)

    report[OverviewReportColNames.RecordsCountWithoutOutliers] = report[OverviewReportColNames.RecordsCountWithoutOutliers] / 1000
    report[StoriesPerformanceReportColNames.StoriesCount] = report[StoriesPerformanceReportColNames.StoriesCount] / 1000

    tab = report.sort_values(
        [OverviewReportColNames.RecordsCountWithoutOutliers],
        ascending=False
    )[[
            OverviewReportColNames.RecordsCountWithoutOutliers,
            StoriesPerformanceReportColNames.StoriesCount,
            StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.ScaledDelayMean, PaymentStoriesColumns.DenotesAnyRisk),
            StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.ScaledDelayMean, PaymentStoriesColumns.DenotesSignificantRisk),
            StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.SeverityMean, PaymentStoriesColumns.DenotesAnyRisk),
            StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.SeverityMean, PaymentStoriesColumns.DenotesSignificantRisk),
            StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.Tendency_ForDelay, PaymentStoriesColumns.DenotesAnyRisk),
            StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.Tendency_ForDelay, PaymentStoriesColumns.DenotesSignificantRisk),
            StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.Tendency_ForSeverity, PaymentStoriesColumns.DenotesAnyRisk),
            StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.Tendency_ForSeverity, PaymentStoriesColumns.DenotesSignificantRisk),
    ]].rename(
            columns={
                OverviewReportColNames.RecordsCountWithoutOutliers: 'Payments (k)',
                StoriesPerformanceReportColNames.StoriesCount: 'Stories (k)',
                StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.ScaledDelayMean, PaymentStoriesColumns.DenotesAnyRisk): r'\makecell{$D_{\mu}$\\mild}',
                StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.ScaledDelayMean,
                                                                            PaymentStoriesColumns.DenotesSignificantRisk): r'\makecell{$D_{\mu}$\\sev.}',
                StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.SeverityMean, PaymentStoriesColumns.DenotesAnyRisk): r'\makecell{$S_{\mu}$\\mild}',
                StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.SeverityMean,
                                                                            PaymentStoriesColumns.DenotesSignificantRisk): r'\makecell{$S_{\mu}$\\sev.}',
                StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.Tendency_ForDelay, PaymentStoriesColumns.DenotesAnyRisk): r'\makecell{$\hat{D}$\\mild}',
                StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.Tendency_ForDelay,
                                                                            PaymentStoriesColumns.DenotesSignificantRisk): r'\makecell{$\hat{D}$\\sev.}',
                StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.Tendency_ForSeverity, PaymentStoriesColumns.DenotesAnyRisk): r'\makecell{$\hat{S}$\\mild}',
                StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(PaymentStoriesColumns.Tendency_ForSeverity,
                                                                            PaymentStoriesColumns.DenotesSignificantRisk): r'\makecell{$\hat{S}$\\sev.}',
            }
        ).head(25)

    with open(_out_file_path, 'w', newline='') as f:
        f.write(_correct_tab(
            tab.to_latex(
                float_format="%.2f",
                label=f"tab:{name}",
                index=True,
                caption=fr"$ROC AUC$ for the biggest sources. "
                        r"Predictors: $D_{\mu}$ scaled delay mean, $S_{\mu}$ severity mean, "
                        r"$\hat{D}$ value of delay tendency, $\hat{S}$ value of severity tendency. "
                        r"Mild and severe risk assessed")
        ))
    return _out_file_path


def tab_sources_f1_overview(input_code: str, name: str) -> Path:

    report = pd.read_csv(report_predictors(input_code), index_col=[0]).merge(
        pd.read_csv(report_overview_file(input_code), index_col=[0]), left_index=True, right_index=True)
    _out_file_path = tex_tab_file(name)

    H1 = StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(PaymentStoriesColumns.ScaledDelayMean, PaymentStoriesColumns.DenotesAnyRisk)
    H2 = StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(PaymentStoriesColumns.SeverityMean, PaymentStoriesColumns.DenotesAnyRisk)
    H3 = StoriesPerformanceReportColNames.PredictorPerformanceF1_00(PaymentStoriesColumns.TendencyCoefficient_ForDelay, PaymentStoriesColumns.DenotesAnyRisk)
    H4 = StoriesPerformanceReportColNames.PredictorPerformanceF1_00(PaymentStoriesColumns.TendencyCoefficient_ForSeverity, PaymentStoriesColumns.DenotesAnyRisk)
    H5 = StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(PaymentStoriesColumns.Tendency_ForDelay, PaymentStoriesColumns.DenotesAnyRisk)
    H6 = StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(PaymentStoriesColumns.Tendency_ForSeverity, PaymentStoriesColumns.DenotesAnyRisk)
    CNT = StoriesPerformanceReportColNames.PredictorCountValid(PaymentStoriesColumns.ScaledDelayMean)

    H = [H1, H2, H3, H4, H5, H6]

    report_best_F1 = report.loc[report[CNT] > 2000].filter(
        items=H + [CNT], axis='columns')
    report_best_F1[CNT] = report_best_F1[CNT] / 1000

    sources = []
    for col in H:
        _best = report_best_F1.sort_values(col, ascending=False).head(15).index
        for _src in _best:
            if _src not in sources:
                sources.append(_src)

    report_best_F1 = report_best_F1.filter(items=sources, axis='index')
    report_best_F1['F1_sum'] = report_best_F1.filter(items=H, axis='columns').sum(axis=1)
    report_best_F1 = report_best_F1.sort_values(['F1_sum'], ascending=False).filter(items=[CNT] + H, axis='columns').rename(columns={
        CNT: 'Valid stories (k)',
        H1: r'\makecell{\textbf{$H_1$}\\$F_1^{max}$\\$D_{\mu}$}',
        H2: r'\makecell{\textbf{$H_2$}\\$F_1^{max}$\\$S_{\mu}$}',
        H3: r'\makecell{\textbf{$H_3$}\\$F_1$\\$D_{a_1}$}',
        H4: r'\makecell{\textbf{$H_4$}\\$F_1$\\$S_{a_1}$}',
        H5: r'\makecell{\textbf{$H_5$}\\$F_1^{max}$\\$\hat{D}$}',
        H6: r'\makecell{\textbf{$H_6$}\\$F_1^{max}$\\$\hat{S}$}'
    })

    _out_file_path = tex_tab_file(name)
    with open(_out_file_path, 'w', newline='') as f:
        f.write(_correct_tab(
            report_best_F1.to_latex(
                float_format="%.2f",
                label=f"tab:{name}",
                index=True,
                caption=fr"$F_1$ score for sources providing best results (mild risk considered). "
                        r"Predictors: $D_{\mu}$ scaled delay mean, $S_{\mu}$ severity mean, "
                        r"$\hat{D}$ value of delay tendency, $\hat{S}$ value of severity tendency")
        ))
    return _out_file_path


def fig_roc_curves(input_code: str, name: str, source_codename: str):
    evaluator = PaymentStoriesPerformanceEvaluator(payment_stories_file(input_code, source_codename), source_codename)
    _fpr = 'False Positive Rate'
    _tpr = 'True Positive Rate'
    _actual_col = PaymentStoriesColumns.DenotesAnyRisk.name
    _steps = 200
    roc_h1 = evaluator.roc_curve(
        predictor_col=PaymentStoriesColumns.ScaledDelayMean.name,
        steps=_steps,
        actual_col=_actual_col
    )
    roc_h2 = evaluator.roc_curve(
        predictor_col=PaymentStoriesColumns.SeverityMean.name,
        steps=_steps,
        actual_col=_actual_col
    )
    roc_h5 = evaluator.roc_curve(
        predictor_col=PaymentStoriesColumns.Tendency_ForDelay.name,
        steps=_steps,
        actual_col=_actual_col
    )
    roc_h6 = evaluator.roc_curve(
        predictor_col=PaymentStoriesColumns.Tendency_ForSeverity.name,
        steps=_steps,
        actual_col=_actual_col
    )

    colormap = mpl.colormaps['summer']
    fig, axs = plt.subplots(1, 2, figsize=(6.5, 3.6), gridspec_kw={"wspace": 0.05})

    _diag_lw = 0.25
    axs[0].plot([0, 1], [0, 1], lw=_diag_lw, color='gray', linestyle='dotted')
    axs[1].plot([0, 1], [0, 1], lw=_diag_lw, color='gray', linestyle='dotted')

    _lw_roc = 0.7
    axs[0].plot(roc_h1[_fpr], roc_h1[_tpr], lw=_lw_roc, color=colormap(0.1), label=r'$H_1$: $D_{\mu}$')
    axs[0].plot(roc_h5[_fpr], roc_h5[_tpr], lw=_lw_roc, color=colormap(0.8), label=r'$H_5$: $\hat{D}$')
    axs[1].plot(roc_h2[_fpr], roc_h2[_tpr], lw=_lw_roc, color=colormap(0.1), label=r'$H_2$: $S_{\mu}$')
    axs[1].plot(roc_h6[_fpr], roc_h6[_tpr], lw=_lw_roc, color=colormap(0.8), label=r'$H_6$: $\hat{S}$')

    axs[0].set_ylabel(_tpr)
    axs[1].yaxis.tick_right()

    axs[0].legend(fontsize='x-small', loc='lower right')
    axs[1].legend(fontsize='x-small', loc='lower right')

    fig.text(0.5, 0.02, _fpr, ha='center', va='center')

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


def fig_f1_curves(input_code: str, name: str, source_codename: str):
    evaluator = PaymentStoriesPerformanceEvaluator(payment_stories_file(input_code, source_codename), source_codename)
    report = pd.read_csv(report_predictors(input_code), index_col=[0]).merge(
        pd.read_csv(report_overview_file(input_code), index_col=[0]), left_index=True, right_index=True).filter(
        items=[source_codename], axis='index')
    _actual_col = PaymentStoriesColumns.DenotesAnyRisk

    f1c_h1 = evaluator.f1_curve(
        predictor_col=PaymentStoriesColumns.ScaledDelayMean.name,
        steps=200,
        actual_col=_actual_col.name
    )

    f1c_h2 = evaluator.f1_curve(
        predictor_col=PaymentStoriesColumns.SeverityMean.name,
        steps=200,
        actual_col=_actual_col.name
    )

    f1c_h5 = evaluator.f1_curve(
        predictor_col=PaymentStoriesColumns.Tendency_ForDelay.name,
        steps=200,
        actual_col=_actual_col.name
    )

    f1c_h6 = evaluator.f1_curve(
        predictor_col=PaymentStoriesColumns.Tendency_ForSeverity.name,
        steps=200,
        actual_col=_actual_col.name
    )

    fig, axs = plt.subplots(1, 2, figsize=(6.5, 3.5), gridspec_kw={"wspace": 0.05})

    colormap = mpl.colormaps['summer']

    _hpt = [r'$H_1$: $D_{\mu}$', r'$H_5$: $\hat{D}$', r'$H_2$: $S_{\mu}$', r'$H_6$: $\hat{S}$']

    _lw_roc = 0.7
    axs[0].plot(f1c_h1.index, f1c_h1[f1c_h1.columns[0]], lw=_lw_roc, color=colormap(0.1), label=_hpt[0])
    axs[0].plot(f1c_h5.index, f1c_h5[f1c_h5.columns[0]], lw=_lw_roc, color=colormap(0.8), label=_hpt[1])
    axs[1].plot(f1c_h2.index, f1c_h2[f1c_h2.columns[0]], lw=_lw_roc, color=colormap(0.1), label=_hpt[2])
    axs[1].plot(f1c_h6.index, f1c_h6[f1c_h6.columns[0]], lw=_lw_roc, color=colormap(0.8), label=_hpt[3])

    _f1mx_size = 10
    axs[0].scatter(
        [report[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMaxTh(PaymentStoriesColumns.ScaledDelayMean, _actual_col)][0]],
        [report[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(PaymentStoriesColumns.ScaledDelayMean, _actual_col)][0]],
        label='$F_1^{max}$, $H_1$', color='red', s=[_f1mx_size])
    axs[0].scatter(
        [report[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMaxTh(PaymentStoriesColumns.Tendency_ForDelay, _actual_col)][0]],
        [report[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(PaymentStoriesColumns.Tendency_ForDelay, _actual_col)][0]],
        label='$F_1^{max}$, $H_5$', color='orange', s=[_f1mx_size])
    axs[1].scatter(
        [report[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMaxTh(PaymentStoriesColumns.SeverityMean, _actual_col)][0]],
        [report[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(PaymentStoriesColumns.SeverityMean, _actual_col)][0]],
        label='$F_1^{max}$, $H_2$', color='red', s=[_f1mx_size])
    axs[1].scatter(
        [report[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMaxTh(PaymentStoriesColumns.Tendency_ForSeverity, _actual_col)][0]],
        [report[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(PaymentStoriesColumns.Tendency_ForSeverity, _actual_col)][0]],
        label='$F_1^{max}$, $H_6$', color='orange', s=[_f1mx_size])

    axs[0].set_ylabel('$F_1$ score')
    axs[0].set_xlabel('Delay (scaled)')
    axs[1].set_xlabel('Severity (scaled)')

    axs[1].yaxis.tick_right()

    axs[0].legend(fontsize='x-small', loc='upper left')
    axs[1].legend(fontsize='x-small', loc='upper left')

    _out_file_path = tex_figure_file(name)
    plt.savefig(_out_file_path)
    return _out_file_path


def tab_binary_classifier_metrics(input_code: str, name: str, source_codename: str):
    evaluator = PaymentStoriesPerformanceEvaluator(payment_stories_file(input_code, source_codename), source_codename)
    report = pd.read_csv(report_predictors(input_code), index_col=[0]).merge(
        pd.read_csv(report_overview_file(input_code), index_col=[0]), left_index=True, right_index=True).filter(
        items=[source_codename], axis='index')

    _predictor_columns = [
        PaymentStoriesColumns.ScaledDelayMean,
        PaymentStoriesColumns.Tendency_ForDelay,
        PaymentStoriesColumns.SeverityMean,
        PaymentStoriesColumns.Tendency_ForSeverity
    ]
    _actual_column = PaymentStoriesColumns.DenotesAnyRisk
    tab = pd.DataFrame({
        'ROC AUC': [report[StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(_pc, _actual_column)][0] for _pc in
                _predictor_columns],
        '$F_1^{max}$': [report[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(_pc, _actual_column)][0] for
                  _pc in _predictor_columns],
        'Recall': [evaluator.true_positive_rate(_pc.name, report[
            StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMaxTh(_pc, _actual_column)][0],
                                             _actual_column.name) for _pc in _predictor_columns],
        'Precision': [evaluator.precision(_pc.name, report[
            StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMaxTh(_pc, _actual_column)][0],
                                    _actual_column.name) for _pc in _predictor_columns],
        'Accuracy': [evaluator.accuracy(_pc.name, report[
            StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMaxTh(_pc, _actual_column)][0],
                                   _actual_column.name) for _pc in _predictor_columns],
    }, index=['$H_1$', '$H_5$', '$H_2$', '$H_6$'])

    _out_file_path = tex_tab_file(name)
    with open(_out_file_path, 'w', newline='') as f:
        f.write(_correct_tab(
            tab.to_latex(
                float_format="%.2f",
                label=f"tab:{name}",
                index=True,
                caption=r"Summary of binary classifier metrics for source "
                        r"\textit{"+source_codename+"} for each hypothesis")
        ))
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

    # print('[blue]Preparing explanation of H5')
    # _f = fig_h5_tendency_value_explained(_input_code, "304_h5_tendency_value_explained", 'Mentdis', 116545831)
    # print(f'[green]Explanation of hypothesis H5 (tendency value) stored in {_f.absolute()}')

    # print('[blue]Creating tabs with ROC AUC for biggest columns')
    # _f = tab_sources_rocauc_overview(_input_code, "311_ROCAUC_biggest_sources")
    # print(f'[green]ROC AUC of biggest sources stored in {_f.absolute()}')

    print('[blue]Creating tabs with F1 for the best sources')
    _f = tab_sources_f1_overview(_input_code, "312_F1_best_sources_mild_risk")
    print(f'[green]F1 of best sources stored in {_f.absolute()}')

    case_studies_srcs = ['Blebout', 'Estry', 'Sirient', 'Addis']

    print('[blue]Generating summary tables')
    for _i, _c in enumerate(case_studies_srcs):
        print(f'[blue]Generate summary table for {_c}')
        _f = tab_binary_classifier_metrics(_input_code, f"4{_i}1_metrics_summary_{_c}", _c)
        print(f'[green]Metrics summary for {_c} stored in {_f.absolute()}')

    print('[blue]Creating ROC curves')
    for _i, _c in enumerate(case_studies_srcs):
        print(f'[blue]Creating ROC curves for {_c}')
        _f = fig_roc_curves(_input_code, f"4{_i}2_roc_curves_{_c}", _c)
        print(f'[green]ROC curve for {_c} was written to {_f.absolute()}')

    print('[blue]Creating F1 curves')
    for _i, _c in enumerate(case_studies_srcs):
        print(f'[blue]Creating F1 curves for {_c}')
        _f = fig_f1_curves(_input_code, f"4{_i}3_f1_curves_{_c}", _c)
        print(f'[green]F1 curve for {_c} was written to {_f.absolute()}')
