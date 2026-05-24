from typing import Annotated

from faststream._internal.context import Context

from faststream_sqlbroker.sqla.broker.broker import SqlaBroker as SB
from faststream_sqlbroker.sqla.message import SqlaMessage as SM

__all__ = (
    "SqlaBroker",
    "SqlaMessage",
)

SqlaMessage = Annotated[SM, Context("message")]
SqlaBroker = Annotated[SB, Context("broker")]
