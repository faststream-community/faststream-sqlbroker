def test_tables() -> None:
    from docs.docs_src.sqlbroker.tables import message, message_archive, metadata

    assert message.name == "message"
    assert message_archive.name == "message_archive"
    assert {"message", "message_archive"} <= set(metadata.tables)

    assert {"id", "queue", "headers", "payload", "state", "next_attempt_at"} <= set(
        message.columns.keys()
    )
    assert {"id", "queue", "payload", "state", "archived_at"} <= set(
        message_archive.columns.keys()
    )
