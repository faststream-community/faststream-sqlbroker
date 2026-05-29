from faststream.exceptions import FastStreamException

INSTALL_FASTSTREAM_SqlBroker = """
To use SqlBroker with FastStream, please install dependencies:\n
pip install "faststream[sqlbroker]"\n
You'll also need an async driver for your database (e.g. asyncpg, asyncmy, aiosqlite).
"""


class DatetimeMissingTimezoneException(FastStreamException):
    def __str__(self) -> str:
        return "This requires a datetime with a non-None timezone"
