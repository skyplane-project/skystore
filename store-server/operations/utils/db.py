from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
import logging
from rich.logging import RichHandler
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from typing import Annotated
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(filename)s:%(lineno)d - %(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
    force=True,
)

logger = logging.getLogger("skystore")
LOG_SQL = os.environ.get("LOG_SQL", "false").lower() == "1"

engine = create_async_engine(
    "sqlite+aiosqlite:///skystore.db",
    echo=LOG_SQL,
    future=True,
)
# engine = create_async_engine(
#     "postgresql+asyncpg://ubuntu:ubuntu@localhost:5432/skystore",
#     echo=LOG_SQL,
#     future=True,
# )
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session


DBSession = Annotated[AsyncSession, Depends(get_session)]
