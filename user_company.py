import asyncpg
import os
from fastapi import HTTPException

DATABASE_URL = os.getenv("DATABASE_URL")

async def get_company_by_email(email: str):
    try:
        connection = await asyncpg.connect(DATABASE_URL)
        query = """
        SELECT cd.company_id, cd.company_name
        FROM users_profile up
        JOIN company_detail cd ON up.id = cd.uuid
        WHERE up.email = $1
        """
        result = await connection.fetch(query, email)
        
        if not result:
            raise HTTPException(status_code=404, detail="User or company not found")
        
        return result
    finally:
        await connection.close()