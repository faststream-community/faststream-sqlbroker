from sqlalchemy.ext.asyncio import create_async_engine

from faststream_sqlbroker.sqlbroker import SqlBroker

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
broker = SqlBroker(engine=engine)
