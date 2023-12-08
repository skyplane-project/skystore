from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
import logging
from rich.logging import RichHandler
from typing import Annotated
import os
from sqlalchemy.pool import NullPool
import asyncpg
import asyncio
from threading import Thread
from UltraDict import UltraDict

# filelock = FileLock("DATABASE_INIT_LOG.log.lock", timeout=10)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(filename)s:%(lineno)d - %(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
    force=True,
)

logger = logging.getLogger("skystore")
LOG_SQL = os.environ.get("LOG_SQL", "false").lower() == "1"


async def create_database(
    db_name: str,
    user: str = "postgres",
    password: str = "skystore",
    host: str = "127.0.0.1",
):
    try:
        conn = await asyncpg.connect(
            user=user, password=password, database=db_name, host=host
        )
        print(f"Database {db_name} already exists. Performing truncate...")
        result = await conn.fetch(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'"
        )
        table_names = [row["tablename"] for row in result]
        print("tablenames: ", table_names)

        # Iterate through the tables and truncate each one
        for table_name in table_names:
            await conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
        print(f"Database {db_name} truncated successfully.")
        await conn.close()
    except Exception as _:
        # the database does not exist
        # connect to the default database, and create the new database we designated
        print(f"Database {db_name} does not exist. Creating...")
        conn = await asyncpg.connect(
            user=user, password=password, database="postgres", host=host
        )
        await conn.execute(f"CREATE DATABASE {db_name}")
        print(f"Database {db_name} created successfully.")
        await conn.execute(
            f"ALTER DATABASE {db_name} SET DEFAULT_TRANSACTION_ISOLATION TO 'read committed';"
        )

        # NOTW: these are the optimizations can be applied in VM setup

        # await conn.execute(f"ALTER DATABASE {db_name} SET synchronous_commit TO off;")
        # await conn.execute(f"ALTER DATABASE {db_name} SET wal_level TO minimal;")
        await conn.execute(f"ALTER DATABASE {db_name} SET fsync TO off;")
        await conn.execute(f"ALTER DATABASE {db_name} SET full_page_writes TO off;")
        await conn.execute(f"ALTER DATABASE {db_name} SET autovacuum TO off;")
        await conn.execute(f"ALTER DATABASE {db_name} SET max_wal_size TO 1GB;")
        await conn.execute(f"ALTER DATABASE {db_name} SET wal_keep_segments TO 0;")
        #await conn.execute(f"ALTER DATABASE {db_name} SET checkpoint_timeout TO 3600;")
        #await conn.execute(f"ALTER DATABASE {db_name} SET checkpoint_completion_target TO 0.9;")
        #await conn.execute(f"ALTER DATABASE {db_name} SET checkpoint_warning TO 0;")
        #await conn.execute(f"ALTER DATABASE {db_name} SET max_wal_senders TO 0;")
        await conn.execute(f"ALTER DATABASE {db_name} SET max_replication_slots TO 0;")
        await conn.execute(f"ALTER DATABASE {db_name} SET default_statistics_target TO 100;")
        await conn.execute(f"ALTER DATABASE {db_name} SET random_page_cost TO 1.0;")
        await conn.execute(f"ALTER DATABASE {db_name} SET effective_cache_size TO 1GB;")
        await conn.execute(f"ALTER DATABASE {db_name} SET work_mem TO 1GB;")
        await conn.execute(f"ALTER DATABASE {db_name} SET maintenance_work_mem TO 1GB;")
        await conn.execute(f"ALTER DATABASE {db_name} SET max_worker_processes TO 32;")
        await conn.execute(f"ALTER DATABASE {db_name} SET max_parallel_workers_per_gather TO 32;")
        await conn.execute(f"ALTER DATABASE {db_name} SET max_parallel_workers TO 32;")
        await conn.execute(f"ALTER DATABASE {db_name} SET max_parallel_maintenance_workers TO 32;")
        await conn.execute(f"ALTER DATABASE {db_name} SET parallel_leader_participation TO off;")
        await conn.execute(f"ALTER DATABASE {db_name} SET max_connections TO 32;")
        # change temp buffers
        await conn.execute(f"ALTER DATABASE {db_name} SET temp_buffers TO 1GB;")
        # set shared buffer
        await conn.execute(f"ALTER DATABASE {db_name} SET shared_buffers TO 1GB;")

        await conn.close()


# Define the name of the database you want to create
database_name = "skystore"
user = "postgres"
password = "skystore"
# user = "shaopu"
# password = "monkeydog"


def run_create_database():
    asyncio.run(create_database(database_name, user, password))


# we will use the ultradict to store the pid of the process that is creating the database

db_init_log = None

try:
    db_init_log = UltraDict(name="db_init_log", create=True)
except:
    db_init_log = UltraDict(name="db_init_log", create=False)

with db_init_log.lock_pid_remote:
    if len(db_init_log) == 0:
        # read the first line of the file
        db_init_log["pid"] = os.getpid()
        thread = Thread(target=run_create_database)
        thread.start()
        thread.join()


engine = create_async_engine(
    # "postgresql+asyncpg://postgres:skystore@localhost:5432/skystore",
    f"postgresql+asyncpg://{user}:{password}@localhost:5432/skystore",
    echo=LOG_SQL,
    future=True,
    # NOTE: only enable this when you are testing
    # poolclass=NullPool,  # NOTE: this is to avoid the "database is being accessed by other users" error
)


# engine = create_async_engine(
#     "sqlite+aiosqlite:///skystore.db",
#     echo=LOG_SQL,
#     future=True,
# )

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
