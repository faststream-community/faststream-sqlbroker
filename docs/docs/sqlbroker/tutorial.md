---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

!!! warning "Alpha"
    `faststream-sqlbroker` is currently in alpha.

# Tutorial

## Motivation

The primary benefit of a message queue built on top of a relational database is the ability to insert messages **transactionally**, atomically with other database operations, thus enabling the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html){.external-link target="_blank"}. Also, the relational database is usually the most readily available, already-provisioned piece of infrastructure for a given service.

Given a proper understanding of the trade-offs involved, a relational-database-based queue is an appropriate tool for many low-to-medium throughput, latency-tolerant uses, including as part of a larger messaging flow that involves a "proper" queue (e.g. as an outbox between a service and a queue).

## Installation

PostgreSQL, MySQL, and SQLite are currently supported.

```bash
pip install "faststream-sqlbroker"
```

## Database Tables

Currently the only supported DB schema variant is `WORK_QUEUE`, at version `1`, which uses up to two tables — `message` (active messages) and `message_archive` (completed/failed messages). These settings are grouped under the broker's `schema` parameter via `SqlBrokerSchemaConfig`. Set `message_archive_table_name=None` there to omit using archiving on success and [DLQ](../sqlbroker/design.md#dead-letter-queue){.internal-link}. You can customize the tables to your liking (partition them, add indices, specify more specific data types like `JSONB`, etc.) as long as they generally conform to the selected schema definition. Schema check is done on startup if the brokers's `validate_schema_on_start` is `True`.

```python linenums="1"
{!> docs_src/sqlbroker/tables.py !}
```

## Broker

```python linenums="1"
{!> docs_src/sqlbroker/broker.py !}
```

#### Broker parameters

- **`engine`** — SQLAlchemy `AsyncEngine` to use for requests to the database.
- **`schema`** — `SqlBrokerSchemaConfig` describing the broker tables and schema selection.
- **`schema.message_table_name`** — Name of the table containing active messages. Defaults to `message`.
- **`schema.message_archive_table_name`** — Name of the table containing completed/failed messages. Defaults to `message_archive`. Set to `None` to run without an archive table, in which case subscribers must set both `retain_in_archive_on_ack` and `retain_in_archive_on_reject` to `False`.
- **`schema.variant`** — Schema variant to use. Defaults to `WORK_QUEUE`.
- **`schema.version`** — Variant-specific schema version enum. For `WORK_QUEUE`, use `SqlBrokerWorkQueueSchemaVersion`, defaulting to `V1`.
- **`validate_schema_on_start`** — If `True` (default), validates that the configured tables exist and conform to the expected schema.
- **`graceful_timeout`** — Seconds to wait for in-flight messages to finish processing during shutdown.

## Publishing

```python linenums="1"
{!> docs_src/sqlbroker/publish.py [ln:1-16]!}
```

The broker's and publisher's (see [publishing](../getting-started/publishing/index.md){.internal-link}) `.publish()` methods accept:

- **`message`** — The message body.
- **`queue`** — The target queue name.
- **`headers`** — Optional `dict[str, str]` of message headers.
- **`next_attempt_at`** — Optional `datetime` (with timezone) for delayed delivery.
- **`connection`** — Optional SQLAlchemy `AsyncConnection` for transactional publishing.

### Delayed delivery

The message won't be fetched until `next_attempt_at` if it is provided.
```python linenums="1"
{!> docs_src/sqlbroker/publish.py [ln:18-22]!}
```

### Transactional publishing

When `connection` is provided, the message insert participates in the same database transaction as your other operations, enabling the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html){.external-link target="_blank"}.
```python linenums="1"
{!> docs_src/sqlbroker/publish.py [ln:24-30]!}
```

## Subscribing

```python linenums="1"
{!> docs_src/sqlbroker/subscribe.py !}
```

#### Subscriber parameters

- **`queues`** — List of queue names to consume from.
- **`max_workers`** (default: `1`) — Number of concurrent handler coroutines.
- **`retry_strategy`** (default: `NoRetryStrategy()`) — Called to determine if and how soon a Nack'ed message is retried.
- **`fetch_batch_size`** — Maximum number of messages to fetch in a single batch. A fetch's actual limit might be lower if the free capacity of the acquired-but-not-yet-processed messages set is smaller.
- **`overfetch_factor`** (default: `1.5`) — Multiplier for `fetch_batch_size` to size the maximum size of the set of acquired-but-not-yet-processed messages.
- **`min_fetch_interval`** — Minimum interval between consecutive fetches. If the last fetch was full (returned as many messages as the fetch's limit), the next fetch happens after both (i) minimum fetch interval has passed, and (ii) capacity equal to the fetch batch size has freed up in the set of acquired-but-not-yet-processed messages.
- **`max_fetch_interval`** — Maximum interval between consecutive fetches.
- **`flush_interval`** — Interval between flushes of processed message state to the database.
- **`release_stuck_interval`** (default: `60`) — Interval between checks for stuck [`PROCESSING`](../sqlbroker/design.md#message-lifecycle){.internal-link} messages.
- **`release_stuck_timeout`** (default: `60 * 10`) — Interval since `acquired_at` after which a [`PROCESSING`](../sqlbroker/design.md#message-lifecycle){.internal-link} message is considered stuck and is released back to [`PENDING`](../sqlbroker/design.md#message-lifecycle){.internal-link}.
- **`max_deliveries`** (default: `None`) — Maximum number of deliveries allowed for a message for [poison message protection](../sqlbroker/design.md#poison-message-protection){.internal-link}. If set, messages that have reached this limit are Reject'ed to [`FAILED`](../sqlbroker/design.md#message-lifecycle){.internal-link} without processing. Note that this might violate the at-least-once processing semantics.
- **`ack_policy`** (default: `REJECT_ON_ERROR`) — [`AckPolicy`](../getting-started/acknowledgement.md){.internal-link} that controls acknowledgement behavior.
- **`retain_in_archive_on_ack`** — If `True` (default), [`COMPLETED`](../sqlbroker/design.md#message-lifecycle){.internal-link} (Ack'ed) messages, in addition to being removed from the primary table, are also persisted in the archive table. Requires the broker to define an archive table (`message_archive_table_name`).
- **`retain_in_archive_on_reject`** — If `True` (default), [`FAILED`](../sqlbroker/design.md#message-lifecycle){.internal-link} (Reject'ed) messages, in addition to being removed from the primary table, are also persisted in the archive table, where they serve as a [dead-letter queue](../sqlbroker/design.md#dead-letter-queue){.internal-link}. Requires the broker to define an archive table (`message_archive_table_name`).

### Acknowledgements

#### Automatic via AckPolicy

Set `ack_policy` on the subscriber to control what happens after handler execution.

```python linenums="1"
{!> docs_src/sqlbroker/acknowledgements.py [ln:1-26]!}
```

- `AckPolicy.ACK` — Marks the message as [`COMPLETED`](../sqlbroker/design.md#message-lifecycle){.internal-link} after the handler attempt, even if the handler raises an exception.
- `AckPolicy.ACK_FIRST` — Has the same effect as `AckPolicy.ACK` for this broker.
- `AckPolicy.REJECT_ON_ERROR` — On success, acknowledges the message. If the handler raises, the message is Reject'ed and marked as [`FAILED`](../sqlbroker/design.md#message-lifecycle){.internal-link}. `retry_strategy` is ignored.
- `AckPolicy.NACK_ON_ERROR` — On success, acknowledges the message. If the handler raises, the message is Nack'ed. The `retry_strategy` is then called to determine whether the message should be scheduled for retry as [`RETRYABLE`](../sqlbroker/design.md#message-lifecycle){.internal-link} or Reject'ed and marked as [`FAILED`](../sqlbroker/design.md#message-lifecycle){.internal-link}. With `NoRetryStrategy()` or `None` in `retry_strategy`, this has the same effect as `REJECT_ON_ERROR`.
- `AckPolicy.MANUAL` — Requires explicit `msg.ack()`, `msg.nack()`, or `msg.reject()` in the handler. In the absence of explicit action, the message is Reject'ed as a safety precaution.

#### Manual

Use `AckPolicy.MANUAL` when the handler should decide the outcome explicitly.

```python linenums="1"
{!> docs_src/sqlbroker/acknowledgements.py [ln:29-39]!}
```

For any `ack_policy` manual acknowledgement overrides the automatic policy decision. If the handler calls `msg.ack()`, `msg.nack()`, or `msg.reject()`, that explicit action is used`.

### Retry strategies

When a message is Nack'ed (either manually or by `AckPolicy.NACK_ON_ERROR`), the `retry_strategy` determines if and when the message should be retried. By default, `NoRetryStrategy()` disables retries. All strategies accept `max_attempts` and `max_total_delay_seconds` as common parameters — if either limit is reached, the message is marked as [`FAILED`](../sqlbroker/design.md#message-lifecycle){.internal-link} instead of [`RETRYABLE`](../sqlbroker/design.md#message-lifecycle){.internal-link}. Otherwise, the strategy schedules the message for a retry determined by the returned `next_attempt_at`.

#### `ConstantRetryStrategy`

Retries after a fixed `delay_seconds` every time.

```python linenums="1"
{!> docs_src/sqlbroker/retry.py [ln:10-14]!}
```

#### `LinearRetryStrategy`

First retry after `initial_delay_seconds`, then the delay increases by `step_seconds` with each attempt.

```python linenums="1"
{!> docs_src/sqlbroker/retry.py [ln:16-21]!}
```

#### `ExponentialBackoffRetryStrategy`

Delay starts at `initial_delay_seconds` and is multiplied by `multiplier` on each attempt. `max_delay_seconds` caps the delay.

```python linenums="1"
{!> docs_src/sqlbroker/retry.py [ln:23-29]!}
```

#### `ExponentialBackoffWithJitterRetryStrategy`

Same as exponential backoff, but adds random jitter (up to `delay * jitter_factor`) to spread out retries and avoid thundering herds.

```python linenums="1"
{!> docs_src/sqlbroker/retry.py [ln:30-36]!}
```

#### `ConstantWithJitterRetryStrategy`

Retries after `base_delay_seconds` plus random jitter in the range `[-jitter_seconds, +jitter_seconds]`.

```python linenums="1"
{!> docs_src/sqlbroker/retry.py [ln:38-43]!}
```

#### `NoRetryStrategy`

No retries — the message is marked as [`FAILED`](../sqlbroker/design.md#message-lifecycle){.internal-link} on the first Nack.

```python linenums="1"
{!> docs_src/sqlbroker/retry.py [ln:45]!}
```

## Transactional outbox

Implementing the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html){.external-link target="_blank"} becomes as simple as the following.

Publish messages transactionally with your other database operations.
```python linenums="1"
{!> docs_src/sqlbroker/transactional_outbox.py [ln:1-27]!}
```

And relay the messages from the database to another broker.
```python linenums="1"
{!> docs_src/sqlbroker/transactional_outbox.py [ln:30-51]!}
```
