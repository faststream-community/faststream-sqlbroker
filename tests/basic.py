from collections.abc import AsyncGenerator
from typing import Any

import pytest_asyncio
from faststream import AckPolicy
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream_sqlbroker.sqlbroker.broker.broker import SqlBroker
from faststream_sqlbroker.sqlbroker.broker.router import SqlBrokerRouter
from faststream_sqlbroker.sqlbroker.retry import NoRetryStrategy
from tests.brokers.base.basic import BaseTestcaseConfig


class SqlBrokerTestcaseConfig(BaseTestcaseConfig):
    _engine: AsyncEngine | None = None

    @pytest_asyncio.fixture(autouse=True)
    async def setup_engine(self, engine: AsyncEngine, recreate_tables: None) -> None:
        self._engine = engine

    def get_broker(
        self,
        **kwargs: Any,
    ) -> SqlBroker:
        engine = kwargs.pop("engine", None) or self._engine
        return SqlBroker(engine=engine, **kwargs)

    @pytest_asyncio.fixture()
    async def broker(
        self, engine: AsyncEngine, recreate_tables: None
    ) -> AsyncGenerator[SqlBroker, None]:
        broker = self.get_broker(engine=engine)
        async with broker:
            yield broker

    def patch_broker(self, broker: SqlBroker, **kwargs: Any) -> SqlBroker:
        return broker

    def get_router(self, **kwargs: Any) -> SqlBrokerRouter:
        return SqlBrokerRouter(**kwargs)

    def get_subscriber_params(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if args:
            kwargs.setdefault("queues", [args[0]])
            args = args[1:]

        kwargs.setdefault("max_workers", 5)
        kwargs.setdefault("retry_strategy", NoRetryStrategy())
        kwargs.setdefault("max_fetch_interval", 0.1)
        kwargs.setdefault("min_fetch_interval", 0.01)
        kwargs.setdefault("fetch_batch_size", 5)
        kwargs.setdefault("overfetch_factor", 1.5)
        kwargs.setdefault("flush_interval", 0.01)
        kwargs.setdefault("release_stuck_interval", 60)
        kwargs.setdefault("release_stuck_timeout", 60 * 10)
        kwargs.setdefault("max_deliveries", None)
        kwargs.setdefault("ack_policy", AckPolicy.REJECT_ON_ERROR)

        return args, kwargs
