import sys
sys.path.append('../')
from lib.input_const import *
import seaborn as sb
import pandas as pd
import tikzplotlib


if __name__ == "__main__":
    report = pd.read_csv(report_overview_file("202212"), index_col=[0])
    # COL_BAD_PAYERS_RATIO = 'bad-payers-percentage'
    # report[COL_BAD_PAYERS_RATIO] = 100.0 * report[OverviewReportColNames.EntitiesWithLaterDebt] / report[
    #     OverviewReportColNames.EntitiesCount]
    # axs = sb.scatterplot(
    #     report.loc[report[OverviewReportColNames.RecordsCountAll] > 1000],
    #     x=OverviewReportColNames.PaymentDaysMean,
    #     y=COL_BAD_PAYERS_RATIO, size=OverviewReportColNames.PaymentDaysStddev, legend=False)
    # tikzplotlib.save(tex_file("overview_sources_delay"))

    with open(tex_tab_file("report"), 'w') as f:
        report.sort_values(
            OverviewReportColNames.RecordsCountAll, ascending=False).head(10)[
            [OverviewReportColNames.RecordsCountWithoutOutliers,
             OverviewReportColNames.EntitiesWithLaterDebt,
             OverviewReportColNames.GendersRatio,
             OverviewReportColNames.AgeMean,
             OverviewReportColNames.PaymentDaysMean,
             OverviewReportColNames.AmountMean]
        ].to_latex(f, float_format="%.2f")
