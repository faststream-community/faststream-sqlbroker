from typing import Annotated

from faststream._internal.context import Context

from faststream_sqlbroker.sqlbroker.broker.broker import SqlBroker as SB
from faststream_sqlbroker.sqlbroker.message import (
    SqlBrokerBatchMessage as SBM,  # noqa: N814
    SqlBrokerMessage as SM,  # noqa: N814
)

__all__ = (
    "SqlBroker",
    "SqlBrokerBatchMessage",
    "SqlBrokerMessage",
)

SqlBrokerMessage = Annotated[SM, Context("message")]
SqlBrokerBatchMessage = Annotated[SBM, Context("message")]
SqlBroker = Annotated[SB, Context("broker")]
