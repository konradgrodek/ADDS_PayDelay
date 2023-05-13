import sys
sys.path.append('../')

from datetime import datetime

from rich import print
from rich.console import Console

from lib.perfeval import *
from lib.input_const import PaymentStoriesColumns

console = Console()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('[red]Missing required parameter: input code that identifies the group of files to be processed')
        exit(1)

    _input_code = sys.argv[1]

    predictor_cols = [
        PaymentStoriesColumns.ScaledDelayMean,
        PaymentStoriesColumns.SeverityMean,
        PaymentStoriesColumns.TendencyCoefficient_ForDelay,
        PaymentStoriesColumns.TendencyCoefficient_ForSeverity
    ]

    actual_cols = [
        PaymentStoriesColumns.DenotesAnyRisk,
        PaymentStoriesColumns.DenotesSignificantRisk
    ]

    statistics = {
        StoriesPerformanceReportColNames.StoriesCount: [],
        StoriesPerformanceReportColNames.StoryLengthMean: [],
        StoriesPerformanceReportColNames.StoryDurationMean: [],
        StoriesPerformanceReportColNames.StoriesPerEntity: [],
        StoriesPerformanceReportColNames.RiskRate: [],
        StoriesPerformanceReportColNames.SignificantRiskRate: []
    }

    for _predictor_col in predictor_cols:
        statistics.update({
            StoriesPerformanceReportColNames.PredictorMean(_predictor_col): [],
            StoriesPerformanceReportColNames.PredictorMedian(_predictor_col): [],
            StoriesPerformanceReportColNames.PredictorStddev(_predictor_col): [],
            StoriesPerformanceReportColNames.PredictorCountValid(_predictor_col): [],
        })
        for _actual_col in actual_cols:
            statistics.update({
                StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(_predictor_col, _actual_col): [],
                StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(_predictor_col, _actual_col): [],
                StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMaxTh(_predictor_col, _actual_col): [],
            })

    sources = []

    for payment_stories in PaymentStoriesDirectory(DIR_PROCESSING).file_names():
        evaluator = PaymentStoriesPerformanceEvaluator(payment_stories.file(DIR_PROCESSING), payment_stories.codename())

        if evaluator.count_stories() < 100:
            print(f'[red]The source <{evaluator.source_codename}> '
                  f'contains less than 100 stories ({evaluator.count_stories()}), it is skipped')
            continue

        # if evaluator.count_stories() > 50000:
        #     continue

        sources.append(payment_stories.codename())

        _mark = datetime.now()
        with console.status(f'[blue]Calculating basic stats for {payment_stories.codename()}', spinner="bouncingBall"):
            statistics[StoriesPerformanceReportColNames.StoriesCount].append(evaluator.count_stories())
            statistics[StoriesPerformanceReportColNames.StoryLengthMean].append(evaluator.story_length_mean())
            statistics[StoriesPerformanceReportColNames.StoryDurationMean].append(evaluator.story_duration_mean())
            statistics[StoriesPerformanceReportColNames.StoriesPerEntity].append(evaluator.stories_per_legal_entity())
            statistics[StoriesPerformanceReportColNames.RiskRate].append(
                evaluator.risk_rate(PaymentStoriesColumns.DenotesAnyRisk.name))
            statistics[StoriesPerformanceReportColNames.SignificantRiskRate].append(
                evaluator.risk_rate(PaymentStoriesColumns.DenotesSignificantRisk.name))
        print(f'[green]<{payment_stories.codename()}> '
              f'Basic stats done in {(datetime.now() - _mark).total_seconds():.1f} s. '
              f'Size: {evaluator.count_stories()} stories')

        for _predictor_col in predictor_cols:
            _mark = datetime.now()
            with console.status(f'[blue]Calculating statistics of {_predictor_col.name}', spinner="bouncingBall"):
                statistics[StoriesPerformanceReportColNames.PredictorMean(_predictor_col)].append(
                    evaluator.predictor_mean(_predictor_col.name))
                statistics[StoriesPerformanceReportColNames.PredictorMedian(_predictor_col)].append(
                    evaluator.predictor_median(_predictor_col.name))
                statistics[StoriesPerformanceReportColNames.PredictorStddev(_predictor_col)].append(
                    evaluator.predictor_stddev(_predictor_col.name))
                statistics[StoriesPerformanceReportColNames.PredictorCountValid(_predictor_col)].append(
                    evaluator.predictor_vcount(_predictor_col.name))
            print(f'[green]Stats for {_predictor_col.name} '
                  f'calculated in {(datetime.now() - _mark).total_seconds():.1f} s')

            for _actual_col in actual_cols:
                with console.status(f'[blue]Evaluating performance of {_predictor_col.name} for {_actual_col.name}',
                                    spinner="bouncingBall"):
                    rocauc = evaluator.roc_auc(_predictor_col.name, _actual_col.name)
                    statistics[StoriesPerformanceReportColNames.PredictorPerformanceROCAUC(
                        _predictor_col, _actual_col)].append(rocauc)
                    f1 = evaluator.f1_max(_predictor_col.name, _actual_col.name)
                    statistics[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMax(
                        _predictor_col, _actual_col)].append(f1[0])
                    statistics[StoriesPerformanceReportColNames.PredictorPerformanceF1ScoreMaxTh(
                        _predictor_col, _actual_col)].append(f1[1])
                print(f'[green]Performance of {_predictor_col.name} for {_actual_col.name} evaluated '
                      f'in {(datetime.now() - _mark).total_seconds():.1f} s. '
                      f'F1: {"N/A" if f1[0] is None else f"{f1[0]:.3f}"}, '
                      f'ROCAUC: {"N/A" if rocauc is None else f"{rocauc:.3f}"}')

    report = pd.DataFrame(statistics, index=sources)
    report.to_csv(report_predictors(_input_code))
    print('[green]DONE')
