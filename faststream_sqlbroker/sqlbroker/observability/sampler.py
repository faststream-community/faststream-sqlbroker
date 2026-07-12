import asyncio
import logging
from collections.abc import Collection
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry

from faststream_sqlbroker.sqlbroker.observability.metrics import SqlBrokerStateMetrics
from faststream_sqlbroker.sqlbroker.observability.snapshot import load_state_snapshot
from faststream_sqlbroker.sqlbroker.schema import (
    SqlBrokerSchemaConfig,
    define_sqlbroker_schema,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


@dataclass(frozen=True, kw_only=True)
class SqlBrokerStateMetricsConfig:
    registry: CollectorRegistry
    interval: float = 30.0
    namespace: str = "sqlbroker"
    queues: Collection[str] | None = None

    def __post_init__(self) -> None:
        if self.interval <= 0:
            msg = "interval must be greater than zero"
            raise ValueError(msg)


class SqlBrokerStateSampler:
    """Periodically query persisted state and populate cached Prometheus metrics."""

    def __init__(
        self,
        *,
        engine: "AsyncEngine",
        schema: SqlBrokerSchemaConfig | None = None,
        config: SqlBrokerStateMetricsConfig,
        logger: logging.Logger | None = None,
    ) -> None:
        self._engine = engine
        self._schema = define_sqlbroker_schema(config=schema or SqlBrokerSchemaConfig())
        self._interval = config.interval
        self._queues = tuple(config.queues) if config.queues is not None else None
        self._metrics = SqlBrokerStateMetrics(
            registry=config.registry,
            namespace=config.namespace,
        )
        self._logger = logger or logging.getLogger(__name__)
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def metrics(self) -> SqlBrokerStateMetrics:
        return self._metrics

    async def collect(self) -> None:
        async with self._engine.connect() as connection:
            snapshot = await load_state_snapshot(
                connection,
                schema=self._schema,
                queues=self._queues,
            )
        self._metrics.apply(snapshot)
        self._metrics.record_success(collected_at=snapshot.collected_at)

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.collect()
            except Exception:
                self._logger.exception("SQLBroker state collection failed")

            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
