from typing import Annotated

from faststream._internal.context import Context

from faststream_sqlbroker.sqlbroker.broker.broker import SqlBroker as SB
from faststream_sqlbroker.sqlbroker.message import SqlBrokerMessage as SM  # noqa: N814

__all__ = (
    "SqlBroker",
    "SqlBrokerMessage",
)

SqlBrokerMessage = Annotated[SM, Context("message")]
SqlBroker = Annotated[SB, Context("broker")]
