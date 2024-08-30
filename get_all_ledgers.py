import asyncpg
import os
from fastapi import HTTPException

DATABASE_URL = os.getenv("DATABASE_URL")

async def fetch_ledger_names():
    try:
        connection = await asyncpg.connect(DATABASE_URL)
        query = """SELECT name FROM mst_ledger"""
        result = await connection.fetch(query)

        if not result:
            raise HTTPException(status_code=404, detail="User or company not found")
        
        return [record['name'] for record in result]
        
    finally:
        await connection.close()
