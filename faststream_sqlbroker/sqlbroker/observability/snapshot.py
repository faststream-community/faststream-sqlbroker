from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection

from faststream_sqlbroker.sqlbroker.message import SqlBrokerMessageState
from faststream_sqlbroker.sqlbroker.observability.queries import (
    archived_message_state_summary_query,
    message_state_summary_query,
)
from faststream_sqlbroker.sqlbroker.schema import (
    SqlBrokerSchemaDefinition,
    SqlBrokerSchemaType,
)


@dataclass(frozen=True)
class MessageStateSummary:
    queue: str
    state: SqlBrokerMessageState
    message_count: int
    oldest_next_attempt_at: datetime


@dataclass(frozen=True)
class ArchivedMessageStateSummary:
    queue: str
    state: SqlBrokerMessageState
    message_count: int


@dataclass(frozen=True)
class SqlBrokerStateSnapshot:
    collected_at: datetime
    messages: tuple[MessageStateSummary, ...]
    archived_messages: tuple[ArchivedMessageStateSummary, ...] = ()


def _state(value: Any) -> SqlBrokerMessageState:
    if isinstance(value, SqlBrokerMessageState):
        return value
    try:
        return SqlBrokerMessageState(value)
    except ValueError:
        return SqlBrokerMessageState[value]


def state_snapshot_from_rows(
    rows: list[RowMapping],
    *,
    collected_at: datetime | None = None,
) -> SqlBrokerStateSnapshot:
    now = collected_at or datetime.now(timezone.utc).replace(tzinfo=None)
    return SqlBrokerStateSnapshot(
        collected_at=now,
        messages=tuple(
            MessageStateSummary(
                queue=row["queue"],
                state=_state(row["state"]),
                message_count=row["message_count"],
                oldest_next_attempt_at=row["oldest_next_attempt_at"],
            )
            for row in rows
        ),
    )


async def load_state_snapshot(
    connection: AsyncConnection,
    *,
    schema: SqlBrokerSchemaDefinition,
    queues: tuple[str, ...] | None = None,
) -> SqlBrokerStateSnapshot:
    table = schema.tables[SqlBrokerSchemaType.MESSAGE]
    result = await connection.execute(message_state_summary_query(table, queues=queues))
    snapshot = state_snapshot_from_rows(list(result.mappings()))

    archive_table = schema.get_table(SqlBrokerSchemaType.MESSAGE_ARCHIVE)
    if archive_table is None:
        return snapshot

    archive_result = await connection.execute(
        archived_message_state_summary_query(archive_table, queues=queues)
    )
    return SqlBrokerStateSnapshot(
        collected_at=snapshot.collected_at,
        messages=snapshot.messages,
        archived_messages=tuple(
            ArchivedMessageStateSummary(
                queue=row["queue"],
                state=_state(row["state"]),
                message_count=row["message_count"],
            )
            for row in archive_result.mappings()
        ),
    )
