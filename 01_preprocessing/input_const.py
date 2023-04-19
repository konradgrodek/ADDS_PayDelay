"""
Contains the static configuration: names of columns, dirs, etc
"""
import pyarrow as pa
import pathlib

from collections import namedtuple


DIR_INPUT = pathlib.Path('./_in')
DIR_PROCESSING = pathlib.Path('./_proc')

PREFIX_PAY_DELAY = 'pay_delay'
PREFIX_DEBTS = 'debts'

EXTENSION_PARQUET = '.parquet'

Column = namedtuple('Column', ['name', 'otype'])


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


class DebtColumns:

    LiabilityOwner = Column('liability_owner', pa.uint32())
    EntityId = Column('legal_entity_id', pa.uint32())
    InfoType = Column('info_type', pa.string())
    CreditStatus = Column('credit_status', pa.string())
    ValidFrom = Column('valid_from', pa.date32())
    ValidTo = Column('valid_to', pa.date32())

    InputColumnTypes = {
        _col.name: _col.otype
        for _col in [LiabilityOwner, EntityId, InfoType, CreditStatus, ValidFrom, ValidTo]
    }

    Id = Column('debt_id', pa.uint32())
