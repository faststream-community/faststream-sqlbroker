from collections.abc import Collection
from typing import Any

from sqlalchemy import Select, Table, func, select


def message_state_summary_query(
    message_table: Table,
    *,
    queues: Collection[str] | None = None,
) -> Select[Any]:
    """Build a portable snapshot query for persisted message states."""
    stmt = select(
        message_table.c.queue.label("queue"),
        message_table.c.state.label("state"),
        func.count().label("message_count"),
        func.min(message_table.c.next_attempt_at).label("oldest_next_attempt_at"),
    ).group_by(message_table.c.queue, message_table.c.state)

    if queues is not None:
        stmt = stmt.where(message_table.c.queue.in_(queues))

    return stmt


def archived_message_state_summary_query(
    message_archive_table: Table,
    *,
    queues: Collection[str] | None = None,
) -> Select[Any]:
    """Build a portable count query for archived message states."""
    stmt = select(
        message_archive_table.c.queue.label("queue"),
        message_archive_table.c.state.label("state"),
        func.count().label("message_count"),
    ).group_by(message_archive_table.c.queue, message_archive_table.c.state)

    if queues is not None:
        stmt = stmt.where(message_archive_table.c.queue.in_(queues))

    return stmt
