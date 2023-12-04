from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
import logging
from rich.logging import RichHandler
from typing import Annotated
import os
from sqlalchemy.pool import NullPool
from sqlalchemy import event, text, Engine
import asyncpg
import asyncio
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(filename)s:%(lineno)d - %(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
    force=True,
)

logger = logging.getLogger("skystore")
LOG_SQL = os.environ.get("LOG_SQL", "false").lower() == "1"

async def create_database(db_name: str):
    # Connect to the default database
    conn = await asyncpg.connect(user='postgres', password='skystore', database='postgres', host='127.0.0.1')
    
    # Check if the database already exists
    databases = await conn.fetch("SELECT datname FROM pg_database;")
    if db_name in {db['datname'] for db in databases}:
        print(f"Database {db_name} already exists. Perform cleaning and re-creating.")
        await conn.execute(f'DROP DATABASE {db_name}')
    # Create the new database
    await conn.execute(f'CREATE DATABASE {db_name}')
    print(f"Database {db_name} created successfully.")

    # Close the connection
    await conn.close()

# Define the name of the database you want to create
database_name = "skystore"

# create_db = asyncio.get_event_loop().run_until_complete(create_database(database_name))
# engine = create_async_engine(
#     "sqlite+aiosqlite:///skystore.db",
#     echo=LOG_SQL,
#     future=True,
# )

def run_create_database():
    asyncio.run(create_database(database_name))

# start a new event loop, create the database and close the loop
thread = Thread(target=run_create_database)
thread.start()
thread.join()

engine = create_async_engine(
    "postgresql+asyncpg://postgres:skystore@localhost:5432/skystore",
    echo=LOG_SQL,
    future=True,
    poolclass=NullPool
)



# @event.listens_for(Engine, "connect")
# def set_sqlite_pragma(dbapi_connection, connection_record):
#     cursor = dbapi_connection.cursor()
#     cursor.execute("PRAGMA synchronous = OFF")
#     cursor.execute("PRAGMA journal_mode=WAL;")
#     cursor.close()


async_session = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session


DBSession = Annotated[AsyncSession, Depends(get_session)]
