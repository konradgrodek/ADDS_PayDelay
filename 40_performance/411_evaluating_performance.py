import sys
import pandas as pd

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

    source_codename = 'Adbouti'
    input_code = 202212
    evaluator = PaymentStoriesPerformanceEvaluator(payment_stories_file(input_code, source_codename), source_codename)
    evaluator.roc_curve(predictor_col=PaymentStoriesColumns.ScaledDelayMean.name, threshold_min=-3, threshold_max=3,
                        steps=100, actual_col=PaymentStoriesColumns.DenotesAnyRisk.name)


    predictor_cols = [
        PaymentStoriesColumns.ScaledDelayMean,
        PaymentStoriesColumns.SeverityMean,
        PaymentStoriesColumns.TendencyCoefficient_ForDelay,
        PaymentStoriesColumns.TendencyCoefficient_ForSeverity
    ]

    statistics = {}
    sources = []

    for payment_stories in PaymentStoriesDirectory(DIR_PROCESSING).file_names():
        evaluator = PaymentStoriesPerformanceEvaluator(payment_stories.file(DIR_PROCESSING), payment_stories.codename())

        sources.append(payment_stories.codename())

        aggregations = [
            (evaluator.predictor_vcount, "count_valid"),
            (evaluator.predictor_min, "min"),
            (evaluator.predictor_mean, "mean"),
            (evaluator.predictor_median, "median"),
            (evaluator.predictor_max, "max")
        ]

        for col in predictor_cols:
            for a in aggregations:
                _n = f"{col.name}_{a[1]}"
                if _n not in statistics:
                    statistics[_n] = []
                statistics[_n].append(a[0](col.name))

        # print(f"<{payment_stories.codename()}><{PaymentStoriesColumns.ScaledDelayMean.name}> "
        #       f"min: {evaluator.predictor_min(PaymentStoriesColumns.ScaledDelayMean.name):.1f} "
        #       f"mean: {evaluator.predictor_mean(PaymentStoriesColumns.ScaledDelayMean.name):.1f} "
        #       f"median: {evaluator.predictor_median(PaymentStoriesColumns.ScaledDelayMean.name):.1f} "
        #       f"max: {evaluator.predictor_max(PaymentStoriesColumns.ScaledDelayMean.name):.1f}")
        # print(f"<{payment_stories.codename()}><{PaymentStoriesColumns.SeverityMean.name}> "
        #       f"min: {evaluator.predictor_min(PaymentStoriesColumns.SeverityMean.name):.1f} "
        #       f"mean: {evaluator.predictor_mean(PaymentStoriesColumns.SeverityMean.name):.1f} "
        #       f"median: {evaluator.predictor_median(PaymentStoriesColumns.SeverityMean.name):.1f} "
        #       f"max: {evaluator.predictor_max(PaymentStoriesColumns.SeverityMean.name):.1f}")
        # print(f"<{payment_stories.codename()}><{PaymentStoriesColumns.TendencyCoefficient_ForDelay.name}> "
        #       f"min: {evaluator.predictor_min(PaymentStoriesColumns.TendencyCoefficient_ForDelay.name):.1f} "
        #       f"mean: {evaluator.predictor_mean(PaymentStoriesColumns.TendencyCoefficient_ForDelay.name):.1f} "
        #       f"median: {evaluator.predictor_median(PaymentStoriesColumns.TendencyCoefficient_ForDelay.name):.1f} "
        #       f"max: {evaluator.predictor_max(PaymentStoriesColumns.TendencyCoefficient_ForDelay.name):.1f}")
        # print(f"<{payment_stories.codename()}><{PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name}> "
        #       f"min: {evaluator.predictor_min(PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name):.1f} "
        #       f"mean: {evaluator.predictor_mean(PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name):.1f} "
        #       f"median: {evaluator.predictor_median(PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name):.1f} "
        #       f"max: {evaluator.predictor_max(PaymentStoriesColumns.TendencyCoefficient_ForSeverity.name):.1f}")
        # print(evaluator.confusion_matrix(
        #     predictor_col=PaymentStoriesColumns.ScaledDelayMean.name,
        #     threshold=0.0,
        #     actual_col=PaymentStoriesColumns.DenotesAnyRisk.name)
        # )
        # print(evaluator.recall(
        #     predictor_col=PaymentStoriesColumns.ScaledDelayMean.name,
        #     threshold=0.0,
        #     actual_col=PaymentStoriesColumns.DenotesAnyRisk.name))

    report = pd.DataFrame(statistics, index=sources)
    report.to_csv(DIR_ANALYSIS, f"predictors_{_input_code}.csv")
