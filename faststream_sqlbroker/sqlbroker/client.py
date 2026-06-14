from __future__ import annotations

import operator
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from faststream.exceptions import FeatureNotSupportedException, SetupError
from sqlalchemy import (
    ColumnElement,
    Table,
    bindparam,
    delete,
    insert,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.mysql import insert as insert_mysql
from sqlalchemy.dialects.postgresql import insert as insert_pg
from sqlalchemy.dialects.sqlite import insert as insert_sqlite

from faststream_sqlbroker.sqlbroker.message import (
    SqlBrokerInnerMessage,
    SqlBrokerMessageState,
)
from faststream_sqlbroker.sqlbroker.schema import (
    SqlBrokerSchemaConfig,
    SqlBrokerSchemaDefinition,
    SqlBrokerSchemaType,
    define_sqlbroker_schema,
)
from faststream_sqlbroker.sqlbroker.schema_validator import SchemaValidator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


def _get_message_select_columns(message: Table) -> tuple[ColumnElement[Any], ...]:
    return (
        message.c.id.label("id"),
        message.c.queue.label("queue"),
        message.c.headers.label("headers"),
        message.c.payload.label("payload"),
        message.c.state.label("state"),
        message.c.attempts_count.label("attempts_count"),
        message.c.deliveries_count.label("deliveries_count"),
        message.c.created_at.label("created_at"),
        message.c.first_attempt_at.label("first_attempt_at"),
        message.c.next_attempt_at.label("next_attempt_at"),
        message.c.last_attempt_at.label("last_attempt_at"),
        message.c.acquired_at.label("acquired_at"),
    )


class SqlBrokerBaseClient(ABC):
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        schema: SqlBrokerSchemaDefinition,
    ) -> None:
        self._engine = engine
        self._schema = schema
        self._message_table = schema.tables[SqlBrokerSchemaType.MESSAGE]
        self._message_archive_table = schema.get_table(
            SqlBrokerSchemaType.MESSAGE_ARCHIVE
        )
        self._message_select_columns = _get_message_select_columns(self._message_table)
        self._schema_validator = SchemaValidator(schema=schema)

    async def enqueue(
        self,
        payload: bytes,
        *,
        queue: str,
        headers: dict[str, str] | None = None,
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        if next_attempt_at:
            stmt = (
                insert(self._message_table)
                .values(
                    queue=queue,
                    payload=payload,
                    headers=headers,
                    next_attempt_at=next_attempt_at,
                )
            )  # fmt: skip
        else:
            stmt = (
                insert(self._message_table)
                .values(
                    queue=queue,
                    payload=payload,
                    headers=headers,
                )
            )  # fmt: skip

        if connection:
            await connection.execute(stmt)
        else:
            async with self._engine.begin() as conn:
                await conn.execute(stmt)

    async def enqueue_batch(
        self,
        items: Sequence[tuple[bytes, dict[str, str]]],
        *,
        queue: str,
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        if not items:
            return

        if next_attempt_at:
            values = [
                {
                    "queue": queue,
                    "payload": payload,
                    "headers": headers,
                    "next_attempt_at": next_attempt_at,
                }
                for payload, headers in items
            ]
        else:
            values = [
                {
                    "queue": queue,
                    "payload": payload,
                    "headers": headers,
                }
                for payload, headers in items
            ]

        stmt = insert(self._message_table).values(values)

        if connection:
            await connection.execute(stmt)
        else:
            async with self._engine.begin() as conn:
                await conn.execute(stmt)

    async def fetch(
        self,
        queues: list[str],
        *,
        limit: int,
    ) -> list[SqlBrokerInnerMessage]:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        ready = (
            select(*self._message_select_columns)
            .where(
                or_(
                    self._message_table.c.state == SqlBrokerMessageState.PENDING,
                    self._message_table.c.state == SqlBrokerMessageState.RETRYABLE,
                ),
                self._message_table.c.next_attempt_at <= now,
                or_(*(self._message_table.c.queue == queue for queue in queues)),
            )
            .order_by(self._message_table.c.next_attempt_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .cte("ready")
        )
        updated = (
            update(self._message_table)
            .where(self._message_table.c.id.in_(select(ready.c.id)))
            .values(
                state=SqlBrokerMessageState.PROCESSING,
                deliveries_count=self._message_table.c.deliveries_count + 1,
                acquired_at=now,
            )
            .returning(self._message_table)
            .cte("updated")
        )
        stmt = select(updated).order_by(updated.c.next_attempt_at)
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
            return [SqlBrokerInnerMessage(**row) for row in result.mappings()]

    async def retry(self, messages: Sequence[SqlBrokerInnerMessage]) -> None:
        if not messages:
            return
        params = [
            {
                "message_id": message.id,
                "state": message.state,
                "attempts_count": message.attempts_count,
                "deliveries_count": message.deliveries_count,
                "first_attempt_at": message.first_attempt_at,
                "next_attempt_at": message.next_attempt_at,
                "last_attempt_at": message.last_attempt_at,
            }
            for message in messages
        ]
        stmt = (
            update(self._message_table)
            .where(self._message_table.c.id == bindparam("message_id"))
            .values(
                state=bindparam("state"),
                attempts_count=bindparam("attempts_count"),
                deliveries_count=bindparam("deliveries_count"),
                first_attempt_at=bindparam("first_attempt_at"),
                next_attempt_at=bindparam("next_attempt_at"),
                last_attempt_at=bindparam("last_attempt_at"),
                acquired_at=None,
            )
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt, params)

    async def archive(
        self,
        messages_to_archive: Sequence[SqlBrokerInnerMessage],
        messages_to_delete_from_primary: Sequence[SqlBrokerInnerMessage],
    ) -> None:
        if not messages_to_archive and not messages_to_delete_from_primary:
            return
        async with self._engine.begin() as conn:
            if messages_to_archive:
                values = [
                    {
                        "id": msg.id,
                        "queue": msg.queue,
                        "payload": msg.payload,
                        "headers": msg.headers,
                        "state": msg.state,
                        "attempts_count": msg.attempts_count,
                        "deliveries_count": msg.deliveries_count,
                        "created_at": msg.created_at,
                        "first_attempt_at": msg.first_attempt_at,
                        "last_attempt_at": msg.last_attempt_at,
                    }
                    for msg in messages_to_archive
                ]
                stmt = self._build_archive_insert_stmt(values)
                await conn.execute(stmt)
            if messages_to_delete_from_primary:
                delete_stmt = (
                    delete(self._message_table)
                    .where(
                        self._message_table.c.id.in_([item.id for item in messages_to_delete_from_primary])
                    )
                )  # fmt: skip
                await conn.execute(delete_stmt)

    async def release_stuck(self, timeout: float) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        select_stuck = (
            select(self._message_table.c.id)
            .where(
                self._message_table.c.state == SqlBrokerMessageState.PROCESSING,
                self._message_table.c.acquired_at < now - timedelta(seconds=timeout),
            )
        )  # fmt: skip
        stmt = (
            update(self._message_table)
            .where(self._message_table.c.id.in_(select_stuck))
            .values(
                state=SqlBrokerMessageState.PENDING,
                next_attempt_at=now,
                acquired_at=None,
            )
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    async def validate_schema(self) -> None:
        async with self._engine.connect() as conn:
            errors = await conn.run_sync(self._schema_validator)
            if errors:
                msg = f"Schema validation failed: {'; '.join(errors)}"
                raise SetupError(msg)

    async def ping(self) -> bool:
        try:
            async with self._engine.connect() as conn:
                (await conn.execute(text("SELECT 1"))).scalar()  # nosemgrep
        except Exception:
            return False
        return True

    @abstractmethod
    def _build_archive_insert_stmt(self, values: list[dict[str, Any]]) -> Any:
        raise NotImplementedError


class SqlBrokerPostgresClient(SqlBrokerBaseClient):
    def _build_archive_insert_stmt(self, values: list[dict[str, Any]]) -> Any:
        return (
            insert_pg(cast("Table", self._message_archive_table))
            .values(values)
            .on_conflict_do_nothing()
        )


class SqlBrokerMySqlClient(SqlBrokerPostgresClient):
    async def fetch(
        self,
        queues: list[str],
        *,
        limit: int,
    ) -> list[SqlBrokerInnerMessage]:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        async with self._engine.begin() as conn:
            ready_stmt = (
                select(self._message_table.c.id.label("id"))
                .where(
                    or_(
                        self._message_table.c.state == SqlBrokerMessageState.PENDING,
                        self._message_table.c.state == SqlBrokerMessageState.RETRYABLE,
                    ),
                    self._message_table.c.next_attempt_at <= now,
                    or_(*(self._message_table.c.queue == queue for queue in queues)),
                )
                .order_by(self._message_table.c.next_attempt_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )

            ready_result = await conn.execute(ready_stmt)
            ready_ids = ready_result.scalars().all()
            if not ready_ids:
                return []

            update_stmt = (
                update(self._message_table)
                .where(self._message_table.c.id.in_(ready_ids))
                .values(
                    state=SqlBrokerMessageState.PROCESSING,
                    deliveries_count=self._message_table.c.deliveries_count + 1,
                    acquired_at=now,
                )
            )
            await conn.execute(update_stmt)

            fetch_stmt = (
                select(*self._message_select_columns)
                .where(self._message_table.c.id.in_(ready_ids))
            )  # fmt: skip
            fetched_result = await conn.execute(fetch_stmt)
            rows = fetched_result.mappings().all()

        rows_by_id = {row["id"]: row for row in rows}
        ordered_rows = [rows_by_id[id_] for id_ in ready_ids if id_ in rows_by_id]
        return [SqlBrokerInnerMessage(**row) for row in ordered_rows]

    async def release_stuck(self, timeout: float) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        select_stuck = (
            select(self._message_table.c.id)
            .where(
                self._message_table.c.state == SqlBrokerMessageState.PROCESSING,
                self._message_table.c.acquired_at < now - timedelta(seconds=timeout),
            )
            .subquery()
        )
        stmt = (
            update(self._message_table)
            .where(self._message_table.c.id.in_(select(select_stuck.c.id)))
            .values(
                state=SqlBrokerMessageState.PENDING,
                next_attempt_at=now,
                acquired_at=None,
            )
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    def _build_archive_insert_stmt(self, values: list[dict[str, Any]]) -> Any:
        return (
            insert_mysql(cast("Table", self._message_archive_table))
            .values(values)
            .prefix_with("IGNORE")
        )


class SqlBrokerSqliteClient(SqlBrokerBaseClient):
    async def fetch(
        self,
        queues: list[str],
        *,
        limit: int,
    ) -> list[SqlBrokerInnerMessage]:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        ready = (
            select(self._message_table.c.id)
            .where(
                or_(
                    self._message_table.c.state == SqlBrokerMessageState.PENDING,
                    self._message_table.c.state == SqlBrokerMessageState.RETRYABLE,
                ),
                self._message_table.c.next_attempt_at <= now,
                or_(*(self._message_table.c.queue == queue for queue in queues)),
            )
            .order_by(self._message_table.c.next_attempt_at)
            .limit(limit)
            .cte("ready")
        )
        claim_stmt = (
            update(self._message_table)
            .where(self._message_table.c.id.in_(select(ready.c.id)))
            .values(
                state=SqlBrokerMessageState.PROCESSING,
                deliveries_count=self._message_table.c.deliveries_count + 1,
                acquired_at=now,
            )
            .returning(*self._message_select_columns)
        )
        async with self._engine.begin() as conn:
            result = await conn.execute(claim_stmt)
            rows = result.mappings().all()

        rows = sorted(rows, key=operator.itemgetter("next_attempt_at"))
        return [SqlBrokerInnerMessage(**row) for row in rows]

    def _build_archive_insert_stmt(self, values: list[dict[str, Any]]) -> Any:
        return (
            insert_sqlite(cast("Table", self._message_archive_table))
            .values(values)
            .on_conflict_do_nothing()
        )


def create_sqlbroker_client(
    engine: AsyncEngine,
    *,
    schema: SqlBrokerSchemaConfig,
) -> SqlBrokerBaseClient:
    schema_definition = define_sqlbroker_schema(
        config=schema,
    )
    client_cls: type[SqlBrokerBaseClient]
    match engine.dialect.name.lower():
        case "mysql":
            client_cls = SqlBrokerMySqlClient
        case "sqlite":
            client_cls = SqlBrokerSqliteClient
        case "postgresql":
            client_cls = SqlBrokerPostgresClient
        case _:
            raise FeatureNotSupportedException
    return client_cls(
        engine,
        schema=schema_definition,
    )
