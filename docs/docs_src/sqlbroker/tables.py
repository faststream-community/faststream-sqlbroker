from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Enum,
    LargeBinary,
    MetaData,
    String,
    Table,
)

from faststream_sqlbroker.sqlbroker.message import SqlBrokerMessageState

metadata = MetaData()

message = Table(
    "message",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("queue", String(255), nullable=False, index=True),
    Column("headers", JSON, nullable=True),
    Column("payload", LargeBinary, nullable=False),
    Column(
        "state",
        Enum(SqlBrokerMessageState),
        nullable=False,
        index=True,
    ),
    Column("attempts_count", BigInteger, nullable=False),
    Column("deliveries_count", BigInteger, nullable=False),
    Column("created_at", DateTime, nullable=False),
    Column("first_attempt_at", DateTime),
    Column(
        "next_attempt_at",
        DateTime,
        nullable=False,
        index=True,
    ),
    Column("last_attempt_at", DateTime),
    Column("acquired_at", DateTime),
)


message_archive = Table(
    "message_archive",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("queue", String(255), nullable=False, index=True),
    Column("headers", JSON, nullable=True),
    Column("payload", LargeBinary, nullable=False),
    Column("state", Enum(SqlBrokerMessageState), nullable=False, index=True),
    Column("attempts_count", BigInteger, nullable=False),
    Column("deliveries_count", BigInteger, nullable=False),
    Column("created_at", DateTime, nullable=False),
    Column("first_attempt_at", DateTime),
    Column("last_attempt_at", DateTime),
    Column("archived_at", DateTime, nullable=False),
)
