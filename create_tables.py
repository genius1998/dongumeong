import asyncio
from database import engine, Base
from models import User, GmailAnalysis

async def create_tables():
    async with engine.begin() as conn:
        # This will create tables if they do not exist
        await conn.run_sync(Base.metadata.create_all)

if __name__ == "__main__":
    asyncio.run(create_tables())
    print("Database tables created successfully.")
