from __future__ import annotations

import importlib
import sys
from typing import TYPE_CHECKING

import pytest
import sqlalchemy.ext.asyncio as sa_asyncio
from faststream import TestApp
from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.ext.asyncio import AsyncEngine

MODULE = "docs.docs_src.sqlbroker.publish"


@pytest.fixture()
def publish_module(
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
async def test_publish(publish_module: object, engine: AsyncEngine) -> None:
    # `publish_examples` runs in `@app.after_startup`, so starting the app
    # exercises the three documented `publish(...)` calls.
    async with TestApp(publish_module.app):
        pass

    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT queue FROM message"))
        rows = result.all()

    assert len(rows) == 3
    assert all(row.queue == "my_queue" for row in rows)
