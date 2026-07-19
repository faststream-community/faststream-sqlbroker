from __future__ import annotations

import importlib
import json
import sys
from typing import TYPE_CHECKING

import pytest
import sqlalchemy.ext.asyncio as sa_asyncio
from faststream import TestApp
from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.ext.asyncio import AsyncEngine

MODULE = "docs.docs_src.sqlbroker.publish_batch"


@pytest.fixture()
def publish_batch_module(
    engine: AsyncEngine,
    recreate_tables: None,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[object]:
    # The docs example builds its engine from a hard-coded Postgres URL at
    # import time. Swap in the real test engine (parametrised over
    # postgres/mysql/sqlite) so the documented code runs against a live DB.
    monkeypatch.setattr(sa_asyncio, "create_async_engine", lambda *a, **k: engine)
    sys.modules.pop(MODULE, None)
    try:
        yield importlib.import_module(MODULE)
    finally:
        sys.modules.pop(MODULE, None)


@pytest.mark.connected()
@pytest.mark.slow()
@pytest.mark.asyncio()
async def test_publish_batch(publish_batch_module: object, engine: AsyncEngine) -> None:
    # `publish_batch_examples` runs in `@app.after_startup`, so starting the
    # app exercises the documented `publish_batch(...)` calls.
    async with TestApp(publish_batch_module.app):
        pass

    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT queue, headers, next_attempt_at FROM message ORDER BY id")
        )
        rows = result.all()

    assert len(rows) == 7
    assert [row.queue for row in rows] == [
        "my_queue",
        "my_queue",
        "my_queue",
        "my_queue",
        "orders",
        "my_queue",
        "my_queue",
    ]

    def as_headers(value: object) -> dict[str, str]:
        if isinstance(value, dict):
            return value
        return json.loads(value)  # type: ignore[arg-type]

    assert as_headers(rows[4].headers)["correlation_id"] == "order-1"
    assert as_headers(rows[4].headers)["x-source"] == "checkout"
    assert rows[5].next_attempt_at is not None
    assert as_headers(rows[6].headers)["x-default"] == "batch"
