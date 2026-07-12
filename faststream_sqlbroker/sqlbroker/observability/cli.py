import asyncio
import logging
import signal
from enum import Enum
from typing import Annotated

import typer
from prometheus_client import CollectorRegistry, start_http_server
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_sqlbroker.sqlbroker.observability.sampler import (
    SqlBrokerStateMetricsConfig,
    SqlBrokerStateSampler,
)
from faststream_sqlbroker.sqlbroker.schema import SqlBrokerSchemaConfig

cli = typer.Typer(add_completion=False, pretty_exceptions_short=True)


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


async def serve(
    *,
    database_url: str,
    host: str,
    port: int,
    interval: float,
    namespace: str,
    message_table: str,
    archive_table: str | None,
    queues: list[str] | None,
) -> None:
    logger = logging.getLogger(__name__)
    engine = create_async_engine(database_url)
    registry = CollectorRegistry()
    sampler = SqlBrokerStateSampler(
        engine=engine,
        schema=SqlBrokerSchemaConfig(
            message_table_name=message_table,
            message_archive_table_name=archive_table,
        ),
        config=SqlBrokerStateMetricsConfig(
            registry=registry,
            interval=interval,
            namespace=namespace,
            queues=queues,
        ),
        logger=logger,
    )
    server, thread = start_http_server(port, addr=host, registry=registry)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stopped.set)

    logger.info("Serving SQLBroker state metrics on http://%s:%d/metrics", host, port)
    sampler.start()
    try:
        await stopped.wait()
    finally:
        await sampler.stop()
        server.shutdown()
        thread.join()
        await engine.dispose()


@cli.command()
def state_metrics(
    database_url: Annotated[
        str,
        typer.Option(
            help="SQLAlchemy async URL, for example postgresql+asyncpg://user:pass@host/db.",  # pragma: allowlist secret
        ),
    ],
    host: Annotated[str, typer.Option(help="Metrics listen address.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Metrics listen port.")] = 8000,
    interval: Annotated[
        float,
        typer.Option(help="Database sampling interval in seconds."),
    ] = 30.0,
    namespace: Annotated[
        str, typer.Option(help="Prometheus metric namespace.")
    ] = "sqlbroker",
    message_table: Annotated[
        str,
        typer.Option(help="SQLBroker primary message table name."),
    ] = "message",
    archive_table: Annotated[
        str,
        typer.Option(
            help="SQLBroker archive table name; use an empty value when disabled.",
        ),
    ] = "message_archive",
    queue: Annotated[
        list[str] | None,
        typer.Option(
            help="Queue to collect; repeat to select multiple queues. All queues by default.",
        ),
    ] = None,
    log_level: Annotated[LogLevel, typer.Option(case_sensitive=False)] = LogLevel.INFO,
) -> None:
    """Expose persisted FastStream SQLBroker state as Prometheus metrics."""
    logging.basicConfig(level=log_level.value)
    asyncio.run(
        serve(
            database_url=database_url,
            host=host,
            port=port,
            interval=interval,
            namespace=namespace,
            message_table=message_table,
            archive_table=archive_table or None,
            queues=queue or None,
        ),
    )


def main() -> None:
    cli(prog_name="sqlbroker-state-metrics")
