# app/db.py
import asyncpg
import os
import json
from fastapi import HTTPException

# Load environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

async def get_database_connection():
    return await asyncpg.connect(DATABASE_URL)

async def fetch_user_companies(email: str):
    conn = await get_database_connection()
    try:
        query = "SELECT companies FROM auth.users WHERE email = $1"
        row = await conn.fetchrow(query, email)

        # for returning list of company IDs
        '''
        if row:
            companies_json = row['companies']
            companies = json.loads(companies_json) if isinstance(companies_json, str) else companies_json
            company_ids = list(companies.values())
            return company_ids
        else:
            raise HTTPException(status_code=404, detail="User not found")
        '''

        # for returning list of objects with company name, company IDs
        if row:
            companies_json = row['companies']
            companies = json.loads(companies_json) if isinstance(companies_json, str) else companies_json
            result = [{company_name: company_id} for company_name, company_id in companies.items()]
            return result
        else:
            raise HTTPException(status_code=404, detail="User not found")
    finally:
        await conn.close()
