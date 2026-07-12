import asyncio
import signal
import socket
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncEngine
from typer.testing import CliRunner

from faststream_sqlbroker.sqlbroker.message import SqlBrokerMessageState
from faststream_sqlbroker.sqlbroker.observability import cli as cli_module
from faststream_sqlbroker.sqlbroker.schema import (
    SqlBrokerSchemaConfig,
    SqlBrokerSchemaType,
    define_sqlbroker_schema,
)

runner = CliRunner()


@pytest.fixture()
def served(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture the arguments the CLI would serve with, without starting the sampler."""
    calls: list[dict[str, Any]] = []

    async def fake_serve(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(cli_module, "serve", fake_serve)
    return calls


def test_cli_defaults(served: list[dict[str, Any]]) -> None:
    result = runner.invoke(
        cli_module.cli,
        ["--database-url", "sqlite+aiosqlite:///broker.db"],
    )

    assert result.exit_code == 0, result.output
    assert served == [
        {
            "database_url": "sqlite+aiosqlite:///broker.db",
            "host": "127.0.0.1",
            "port": 8000,
            "interval": 30.0,
            "namespace": "sqlbroker",
            "message_table": "message",
            "archive_table": "message_archive",
            "queues": None,
        },
    ]


def test_cli_queue_filters_are_repeatable(served: list[dict[str, Any]]) -> None:
    result = runner.invoke(
        cli_module.cli,
        [
            "--database-url",
            "sqlite+aiosqlite:///broker.db",
            "--queue",
            "orders",
            "--queue",
            "email",
        ],
    )

    assert result.exit_code == 0, result.output
    assert served[0]["queues"] == ["orders", "email"]


def test_cli_empty_archive_table_disables_archive_metrics(
    served: list[dict[str, Any]],
) -> None:
    result = runner.invoke(
        cli_module.cli,
        ["--database-url", "sqlite+aiosqlite:///broker.db", "--archive-table", ""],
    )

    assert result.exit_code == 0, result.output
    assert served[0]["archive_table"] is None


def test_cli_requires_database_url(served: list[dict[str, Any]]) -> None:
    result = runner.invoke(cli_module.cli, [])

    assert result.exit_code != 0
    assert not served


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _fetch(url: str) -> str:
    with urllib.request.urlopen(url, timeout=1) as response:
        return response.read().decode()


async def _wait_for_metrics(
    url: str, *, timeout: float, expected: str | None = None
) -> str:
    """Poll the metrics endpoint until it is reachable and (optionally) contains `expected`.

    The CLI starts the HTTP server before its first background sample completes, so an
    early scrape can observe an endpoint that is reachable but not yet populated.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_error: Exception | None = None
    body = ""
    while loop.time() < deadline:
        try:
            body = await asyncio.to_thread(_fetch, url)
            if expected is None or expected in body:
                return body
        except (urllib.error.URLError, ConnectionError) as exc:
            last_error = exc
        await asyncio.sleep(0.05)
    if last_error is not None and not body:
        msg = f"metrics endpoint at {url} never became ready: {last_error}"
        raise AssertionError(msg)
    return body


@pytest.mark.asyncio()
async def test_cli_serves_real_metrics_over_http_and_shuts_down_gracefully(
    engine: AsyncEngine, recreate_tables: None
) -> None:
    """Run the CLI as a real subprocess against a real database end to end."""
    schema = define_sqlbroker_schema(config=SqlBrokerSchemaConfig())
    table = schema.tables[SqlBrokerSchemaType.MESSAGE]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with engine.begin() as connection:
        await connection.execute(
            insert(table),
            {
                "queue": "orders",
                "payload": b"1",
                "state": SqlBrokerMessageState.PENDING,
                "created_at": now,
                "next_attempt_at": now,
                "attempts_count": 0,
                "deliveries_count": 0,
            },
        )

    database_url = engine.url.render_as_string(hide_password=False)
    port = _free_tcp_port()

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "from faststream_sqlbroker.sqlbroker.observability.cli import cli; cli()",
        "--database-url",
        database_url,
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--interval",
        "0.05",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        expected = 'sqlbroker_messages{queue="orders",state="pending"} 1.0'
        body = await _wait_for_metrics(
            f"http://127.0.0.1:{port}/metrics", timeout=10, expected=expected
        )
        assert expected in body

        process.send_signal(signal.SIGINT)
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.wait()
            msg = "CLI did not shut down gracefully after SIGINT"
            raise AssertionError(msg) from exc
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()

    output = (await process.stdout.read()).decode() if process.stdout else ""
    assert process.returncode == 0, output
