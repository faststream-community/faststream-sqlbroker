from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, IntEnum

from faststream.exceptions import SetupError
from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Enum as SqlEnum,
    LargeBinary,
    MetaData,
    String,
    Table,
)

from faststream_sqlbroker.sqlbroker.message import SqlBrokerMessageState


class SqlBrokerSchemaVariant(str, Enum):
    COMPETING_CONSUMERS = "competing_consumers"


class SqlBrokerCompetingConsumersSchemaVersion(IntEnum):
    V1 = 1


class SqlBrokerSchemaType(str, Enum):
    MESSAGE = "message"
    MESSAGE_ARCHIVE = "message_archive"


@dataclass(frozen=True, kw_only=True)
class SqlBrokerSchemaConfig:
    message_table_name: str = "message"
    message_archive_table_name: str | None = "message_archive"
    variant: SqlBrokerSchemaVariant = SqlBrokerSchemaVariant.COMPETING_CONSUMERS
    version: SqlBrokerCompetingConsumersSchemaVersion = (
        SqlBrokerCompetingConsumersSchemaVersion.V1
    )


def _unsupported_schema_variant_error(
    variant: SqlBrokerSchemaVariant,
) -> SetupError:
    return SetupError(f"Unsupported SqlBroker schema variant: {variant}")


def _unsupported_schema_version_error(
    *,
    variant: SqlBrokerSchemaVariant,
    version: object,
) -> SetupError:
    return SetupError(
        "Unsupported SqlBroker schema version for "
        f"{variant.value}: {version}. Supported versions: "
        f"{SqlBrokerCompetingConsumersSchemaVersion.V1.value}"
    )


@dataclass(frozen=True)
class SqlBrokerSchemaDefinition:
    variant: SqlBrokerSchemaVariant
    version: SqlBrokerCompetingConsumersSchemaVersion
    tables: dict[SqlBrokerSchemaType, Table]

    def get_table(self, schema_type: SqlBrokerSchemaType) -> Table | None:
        return self.tables.get(schema_type)


def define_sqlbroker_schema(
    *,
    config: SqlBrokerSchemaConfig,
) -> SqlBrokerSchemaDefinition:
    variant = config.variant

    if variant is not SqlBrokerSchemaVariant.COMPETING_CONSUMERS:
        raise _unsupported_schema_variant_error(variant)

    version = config.version
    if not isinstance(version, SqlBrokerCompetingConsumersSchemaVersion):
        raise _unsupported_schema_version_error(
            variant=variant,
            version=version,
        )

    metadata = MetaData()
    tables: dict[SqlBrokerSchemaType, Table] = {
        SqlBrokerSchemaType.MESSAGE: Table(
            config.message_table_name,
            metadata,
            Column("id", BigInteger, primary_key=True),
            Column("queue", String(255), nullable=False, index=True),
            Column("headers", JSON, nullable=True),
            Column("payload", LargeBinary, nullable=False),
            Column(
                "state",
                SqlEnum(SqlBrokerMessageState),
                nullable=False,
                index=True,
                server_default=SqlBrokerMessageState.PENDING.name,
            ),
            Column("attempts_count", BigInteger, nullable=False, default=0),
            Column("deliveries_count", BigInteger, nullable=False, default=0),
            Column(
                "created_at",
                DateTime,
                nullable=False,
                default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
            ),
            Column("first_attempt_at", DateTime),
            Column(
                "next_attempt_at",
                DateTime,
                nullable=False,
                default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
                index=True,
            ),
            Column("last_attempt_at", DateTime),
            Column("acquired_at", DateTime),
        )
    }

    if config.message_archive_table_name is not None:
        tables[SqlBrokerSchemaType.MESSAGE_ARCHIVE] = Table(
            config.message_archive_table_name,
            metadata,
            Column("id", BigInteger, primary_key=True),
            Column("queue", String(255), nullable=False, index=True),
            Column("headers", JSON, nullable=True),
            Column("payload", LargeBinary, nullable=False),
            Column("state", SqlEnum(SqlBrokerMessageState), nullable=False, index=True),
            Column("attempts_count", BigInteger, nullable=False),
            Column("deliveries_count", BigInteger, nullable=False),
            Column("created_at", DateTime, nullable=False),
            Column("first_attempt_at", DateTime),
            Column("last_attempt_at", DateTime),
            Column(
                "archived_at",
                DateTime,
                nullable=False,
                default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
            ),
        )

    return SqlBrokerSchemaDefinition(
        variant=variant,
        version=version,
        tables=tables,
    )
