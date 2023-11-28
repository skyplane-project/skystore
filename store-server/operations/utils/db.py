from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
import logging
from rich.logging import RichHandler
from typing import Annotated
import os
from sqlalchemy.pool import NullPool

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
#     "postgresql+asyncpg://shaopu:monkeydog@localhost:5432/skystore",
#     echo=LOG_SQL,
#     future=True,
#     poolclass=NullPool
# )
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session


DBSession = Annotated[AsyncSession, Depends(get_session)]
