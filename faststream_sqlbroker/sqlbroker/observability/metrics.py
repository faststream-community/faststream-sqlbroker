from datetime import datetime, timezone

from prometheus_client import CollectorRegistry, Gauge

from faststream_sqlbroker.sqlbroker.observability.snapshot import (
    SqlBrokerStateSnapshot,
)


class SqlBrokerStateMetrics:
    """Cached Prometheus metrics populated from a persisted-state snapshot."""

    def __init__(
        self,
        *,
        registry: CollectorRegistry,
        namespace: str = "sqlbroker",
    ) -> None:
        self.messages = Gauge(
            "messages",
            "Current messages persisted in the SQLBroker primary table.",
            ("queue", "state"),
            namespace=namespace,
            registry=registry,
        )
        self.most_overdue_message_age_seconds = Gauge(
            "most_overdue_message_age_seconds",
            "Lag of the most overdue SQLBroker message by queue and state.",
            ("queue", "state"),
            namespace=namespace,
            registry=registry,
        )
        self.archived_messages = Gauge(
            "archived_messages",
            "Current messages persisted in the SQLBroker archive table.",
            ("queue", "state"),
            namespace=namespace,
            registry=registry,
        )
        self.last_success_timestamp_seconds = Gauge(
            "state_collection_last_success_timestamp_seconds",
            "Unix timestamp of the last successful SQLBroker state collection.",
            namespace=namespace,
            registry=registry,
        )
        self._labels: set[tuple[str, str]] = set()
        self._archive_labels: set[tuple[str, str]] = set()

    def apply(self, snapshot: SqlBrokerStateSnapshot) -> None:
        current: set[tuple[str, str]] = set()
        for item in snapshot.messages:
            labels = (item.queue, item.state.value)
            current.add(labels)
            self.messages.labels(*labels).set(item.message_count)
            age = max(
                0.0,
                (snapshot.collected_at - item.oldest_next_attempt_at).total_seconds(),
            )
            self.most_overdue_message_age_seconds.labels(*labels).set(age)

        for labels in self._labels - current:
            self.messages.remove(*labels)
            self.most_overdue_message_age_seconds.remove(*labels)
        self._labels = current

        archive_current: set[tuple[str, str]] = set()
        for archived_item in snapshot.archived_messages:
            labels = (archived_item.queue, archived_item.state.value)
            archive_current.add(labels)
            self.archived_messages.labels(*labels).set(archived_item.message_count)

        for labels in self._archive_labels - archive_current:
            self.archived_messages.remove(*labels)
        self._archive_labels = archive_current

    def record_success(self, *, collected_at: datetime) -> None:
        self.last_success_timestamp_seconds.set(
            collected_at.replace(tzinfo=timezone.utc).timestamp()
        )
