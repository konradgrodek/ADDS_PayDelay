"""
Contains the static or semi-static configuration: names of columns, files, dirs, etc
"""
import pyarrow as pa
from pathlib import Path
from collections import namedtuple
import re


DIR_INPUT = Path('../_in')
DIR_PROCESSING = Path('../_proc')
DIR_ANALYSIS = Path('../_analysis')

PREFIX_PAY_DELAY = 'pay_delay'
PREFIX_PAYMENTS_WITH_DEBTS = 'pay_delay_w_debts'
PREFIX_PAYMENTS_GROUPED = 'payments_grouped'
PREFIX_PAYMENT_STORIES = 'payment_stories'
PREFIX_DEBTS = 'debts'

EXTENSION_PARQUET = '.parquet'

MALE = "MALE"
FEMALE = "FEMALE"

OUTLIER__MIN_DELAY = -90
OUTLIER__MAX_DELAY = 365

Column = namedtuple('Column', ['name', 'otype'])


class DebtColumns:

    LiabilityOwner = Column('liability_owner', pa.uint32())
    EntityId = Column('legal_entity_id', pa.uint32())
    InfoType = Column('info_type', pa.string())
    CreditStatus = Column('credit_status', pa.uint8())
    ValidFrom = Column('valid_from', pa.date32())
    ValidTo = Column('valid_to', pa.date32())

    InputColumnTypes = {
        _col.name: _col.otype
        for _col in [LiabilityOwner, EntityId, InfoType, CreditStatus, ValidFrom, ValidTo]
    }

    Id = Column('debt_id', pa.uint32())


class PayDelayColumns:

    EntityId = Column('legal_entity_id', pa.uint32())
    DueDate = Column("due_date", pa.date32())
    DelayDays = Column("delay_days", pa.int32())
    InvoicedAmount = Column("invoiced_amount", pa.int32())
    Industry = Column("industry", pa.string())
    DataSource = Column('data_source_id', pa.uint16())
    BirthDateInt = Column("birth_date_int", pa.uint32())
    Sex = Column("sex", pa.string())

    InputColumnTypes = {
        _col.name: _col.otype
        for _col in [EntityId, DueDate, DelayDays, InvoicedAmount, Industry, DataSource, BirthDateInt, Sex]
    }

    Id = Column('pd_id', pa.uint32())

    Age = Column("age", pa.int16())

    IsOutlier = Column("is_outlier", pa.bool_())

    PriorDebtsMaxCreditStatus = Column(f"prior_{DebtColumns.CreditStatus.name}_max", DebtColumns.CreditStatus.otype)
    LaterDebtsCount = Column("later_debts_count", "undefined")
    LaterDebtsMaxCreditStatus = Column(f"later_{DebtColumns.CreditStatus.name}_max", DebtColumns.CreditStatus.otype)

    @staticmethod
    def LaterDebtsMinDaysToValidFrom(cs: int) -> Column:
        return Column(f"later_{DebtColumns.CreditStatus.name}_{cs}_min_days_to_valid_from", pa.uint16())


def pay_delay_ori_file(input_code: str) -> Path:
    """
    Returns the path to original file (csv) with payment-delay data
    :param input_code: the input-code
    :return: the path to the csv file with all payment delays
    """
    return DIR_INPUT / f'{PREFIX_PAY_DELAY}_{input_code}.csv'


def pay_delay_file(input_code: str) -> Path:
    """
    Use to get the path to the pay-delay parquet file
    :param input_code: the input-code
    :return: the path to parquet file containing ALL payment delays
    """
    return DIR_INPUT / f'{PREFIX_PAY_DELAY}_{input_code}{EXTENSION_PARQUET}'


def pay_delay_with_debts_file(input_code: str) -> Path:
    """
    Provides path to file containing ALL payment delays with debt information. NOTE: the file is not verified if exists
    :param input_code: the input-code of interest (e.g. '202212' or 'SAMPLE', etc)
    :return: the Path pointing to the file
    """
    return DIR_PROCESSING / f'{PREFIX_PAYMENTS_WITH_DEBTS}_{input_code}{EXTENSION_PARQUET}'


class PerSourceFileName:
    """
    Encapsulates creating and 'parsing' file name
    """

    def __init__(self, prefix: str, file: Path = None, input_code: str = None, codename: str = None):
        """
        There are two ways to define the object:
        (i) by providing valid path to the file
        (ii) by providing input-code (e.g. '202212' or 'SAMPLE') and the code-name of the source
        :param file: the path to the file containing payment delay and debt
        :param input_code: the name of the input-code
        :param codename: the code-name of the source
        """
        self._prefix = prefix
        self._pattern = re.compile(f"{self._prefix}_([A-Z][a-z]+)_(.*){EXTENSION_PARQUET}")
        if file is not None:
            if input_code is not None or codename is not None:
                raise ValueError('Provide either the path or input-code and source codename')
        else:
            if input_code is None or codename is None:
                raise ValueError('If file path is not provided, both input-code and source codename are required')
        self._file = file
        self._file_name = None if file is None else file.name
        self._input_code = input_code
        self._codename = codename

    def file_name(self) -> str:
        """
        Returns the name of the file. Note: it may happen that if the object was created with incorrect file name,
        this will simply return the value that does not follow the desired pattern
        :return: the file name
        """
        if self._file_name is None:
            self._file_name = f"{self._prefix}_{self._codename}_{self._input_code}{EXTENSION_PARQUET}"
        return self._file_name

    def is_name_valid(self) -> bool:
        """
        Only returns true if the object contains name, which follows the established pattern
        for pay-delay-with-debts-by-source
        :return: True if the name follows the pay-delay-with-debes-by-source pattern
        """
        return re.fullmatch(self._pattern, self.file_name()) is not None

    def input_code(self) -> str:
        """
        Returns the input-code for the file. If the file does not follow correct pattern, ValueError is thrown
        :return: The inuput code (e.g. '202212' or 'SAMPLE', etc)
        """
        if self._input_code is not None:
            return self._input_code
        if not self.is_name_valid():
            raise ValueError("The provided name is not valid pay-delay-with-debts-per-source name")
        self._input_code = self._pattern.match(self._file_name).group(2)
        return self._input_code

    def codename(self) -> str:
        """
        Returns the code-name generated by source, which is isolated in this pay-delay-with-debts-per-source file.
        If the file does not follow correct pattern, ValueError is thrown
        :return: THe code-name (e.g. 'Foresecu', 'Alparal', etc)
        """
        if self._codename is not None:
            return self._codename
        if not self.is_name_valid():
            raise ValueError("The provided name is not valid pay-delay-with-debts-per-source name")
        self._codename = self._pattern.match(self._file_name).group(1)
        return self._codename

    def file(self, basedir: Path = Path('.'), validate=True) -> Path:
        """
        Appends the file name to the base-dir and returns Path object.
        Raises value error if the file does not exist unless validate=False
        :param basedir: the basedir where the file is expected to be.
        :param validate: if True, the file will be checked if exists
        :return: Path object for the file
        """
        if self._file is not None:
            if validate and not self._file.exists():
                raise ValueError(f'The file {self._file.absolute()} does not exist')
            return self._file

        self._file = basedir / self.file_name()
        return self.file(basedir, validate)


def PayDelayWithDebtsFileName(file: Path = None, input_code: str = None, codename: str = None):
    return PerSourceFileName(PREFIX_PAYMENTS_WITH_DEBTS, file=file, input_code=input_code, codename=codename)


class PerSourceDirectory:
    """
    Class is designed to ease filtering the provided type of files from given directory for all sources
    """

    def __init__(self, prefix: str, _dir: Path):
        """
        Creates the object which can be used to provide all files that met given files pattern
        :param prefix: the prefix of files
        :param _dir: the directory to scan
        """
        self._prefix = prefix
        self._dir = _dir
        if not self._dir.is_dir():
            raise ValueError(f"The path {self._dir} does not point to a directory")

    def file_names(self) -> list[PerSourceFileName]:
        """
        Provides list of file-names objects for all files within the directory that meet given pattern
        :return: list of PayDelayWithDebtsFileName
        """
        return [
            _pdf
            for _pdf in [PerSourceFileName(prefix=self._prefix, file=_fle) for _fle in self._dir.iterdir()]
            if _pdf.is_name_valid()
        ]

    def files(self) -> list[Path]:
        """
        Provides list of files for all pay-delay-with-debts-per-source files within the directory
        :return: list of Path
        """
        return [_pdf.file(self._dir) for _pdf in self.file_names()]


def PayDelayWithDebtsDirectory(_dir: Path):
    return PerSourceDirectory(PREFIX_PAYMENTS_WITH_DEBTS, _dir)


def PaymentsGroupedDirectory(_dir: Path):
    return PerSourceDirectory(PREFIX_PAYMENTS_GROUPED, _dir)


def PaymentStoriesDirectory(_dir: Path):
    return PerSourceDirectory(PREFIX_PAYMENT_STORIES, _dir)


def report_overview_file(input_code: str) -> Path:
    """
    Provides path to file with stored DataFrame containing overview report on the sources
    :param input_code: the input-code of interest
    :return: the path to the file with overview report
    """
    return DIR_ANALYSIS / f'overview_report_{input_code}.csv'


def payments_grouped_by_stories_file(input_code: str, source_codename: str) -> Path:
    """
    Returns path to file containing partial, intermediate processing data:
    payment information with the grouping information and - if exists - the "ending debt" info
    :param input_code: the input-code
    :param source_codename: the source identifier
    :return: path to a parquet file
    """
    return DIR_PROCESSING / f'{PREFIX_PAYMENTS_GROUPED}_{source_codename}_{input_code}{EXTENSION_PARQUET}'


def payment_stories_file(input_code: str, source_codename: str) -> Path:
    """
    Returns path to file containing "payment stories", payments grouped by entity and ordered in time-lines
    :param input_code: the input-code
    :param source_codename: the identification of source
    :return: file path
    """
    return DIR_PROCESSING / f'{PREFIX_PAYMENT_STORIES}_{source_codename}_{input_code}{EXTENSION_PARQUET}'


class PaymentGroupsColumns:

    Id = PayDelayColumns.Id
    EntityId = PayDelayColumns.EntityId
    DueDate = PayDelayColumns.DueDate
    DelayDays = PayDelayColumns.DelayDays
    InvoicedAmount = PayDelayColumns.InvoicedAmount
    PriorCreditStatusMax = PayDelayColumns.PriorDebtsMaxCreditStatus
    StoryId = Column("story_id", PayDelayColumns.Id.otype)
    DividingCreditStatus = Column("dividing_credit_status", PayDelayColumns.LaterDebtsMaxCreditStatus.otype)
    DividingDaysToDebt = Column("dividing_days_to_debt", PayDelayColumns.LaterDebtsMinDaysToValidFrom(1).otype)

    InvoicedAmountScaled = Column(InvoicedAmount.name + '_scaled', pa.float64())
    DelayDaysScaled = Column(DelayDays.name + '_scaled', pa.float64())
    StoryTimeline = Column('days_since_story_begins', pa.uint16())
    Severity = Column('severity', pa.float64())


class PaymentStoriesColumns:

    StoryId = PaymentGroupsColumns.StoryId
    FirstPaymentId = Column(PayDelayColumns.Id.name+"_min", PayDelayColumns.Id.otype)
    EntityId = PayDelayColumns.EntityId
    BeginsAt = Column(PayDelayColumns.DueDate.name+"_first", PayDelayColumns.DueDate.otype)
    EndsAt = Column(PayDelayColumns.DueDate.name+"_last", PayDelayColumns.DueDate.otype)
    Duration = Column("story_duration_days", PayDelayColumns.DelayDays.otype)
    BeginsWithCreditStatus = Column(DebtColumns.CreditStatus.name+"_start", DebtColumns.CreditStatus.otype)
    EndsWithCreditStatus = Column(DebtColumns.CreditStatus.name+"_stop", DebtColumns.CreditStatus.otype)
    LaterDebtMinDaysToValidFrom = Column(f"later_debts_min_days_to_valid_from",
                                         PaymentGroupsColumns.DividingDaysToDebt.otype)
    PaymentsCount = Column('payments_count', pa.uint16())
    ScaledDelaySum = Column(PayDelayColumns.DelayDays.name+'_scaled_sum', pa.float32())
    ScaledDelayMean = Column(PayDelayColumns.DelayDays.name+'_scaled_mean', pa.float32())
    ScaledAmountMean = Column(PayDelayColumns.InvoicedAmount.name+'_scaled_mean', pa.float32())
    SeveritySum = Column('severity_sum', ScaledDelaySum.otype)
    SeverityMean = Column('severity_mean', ScaledDelaySum.otype)
    DaysSinceBeginMean = Column('days_since_begin_mean', None)
    TendencyCoefficient_ForDelay = Column('regression_line_a1_for_delay', pa.float64())
    TendencyConstant_ForDelay = Column('regression_line_a0_for_delay', pa.float64())
    TendencyError_ForDelay = Column('regression_line_rsquare_for_delay', pa.float64())
    TendencyCoefficient_ForSeverity = Column('regression_line_a1_for_severity', pa.float64())
    TendencyConstant_ForSeverity = Column('regression_line_a0_for_severity', pa.float64())
    TendencyError_ForSeverity = Column('regression_line_rsquare_for_severity', pa.float64())
