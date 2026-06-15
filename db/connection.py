import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.sql import text
import os



DATABASE_URL = f"postgresql+asyncpg://{os.environ["DB_USER"]}:{os.environ["DB_PASSWORD"]}@{os.environ['DB_HOST']}/{os.environ["DB_NAME"]}"

engine = create_async_engine(DATABASE_URL)

async_session_local = async_sessionmaker(
    bind=engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)