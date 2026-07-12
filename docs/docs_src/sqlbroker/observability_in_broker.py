from prometheus_client import CollectorRegistry, make_asgi_app
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_sqlbroker import SqlBroker
from faststream_sqlbroker.sqlbroker.observability import (
    SqlBrokerStateMetricsConfig,
)

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
registry = CollectorRegistry()

broker = SqlBroker(
    engine=engine,
    state_metrics_config=SqlBrokerStateMetricsConfig(
        registry=registry,
        interval=30,
    ),
)
metrics_app = make_asgi_app(registry=registry)
