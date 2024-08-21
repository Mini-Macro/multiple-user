# Code 1
import asyncpg
import asyncio
from datetime import datetime
from collections import defaultdict
import json
import time

async def fetch_data(pool, query, *args):
    async with pool.acquire() as connection:
        return await connection.fetch(query, *args)

async def get_monthly_totals(pool, start_date, end_date, company_id):
    query = """
        SELECT DATE_TRUNC('month', date) AS month,
               SUM(CASE WHEN voucher_type = 'Sales IPD' THEN ABS(amount) ELSE 0 END) AS ipd_total,
               SUM(CASE WHEN voucher_type = 'Sales OPD' THEN ABS(amount) ELSE 0 END) AS opd_total
        FROM trn_voucher
        WHERE voucher_type IN ('Sales IPD', 'Sales OPD')
          AND company_id = $1
          AND date BETWEEN $2 AND $3
        GROUP BY DATE_TRUNC('month', date)
        ORDER BY month;
    """
    results = await fetch_data(pool, query, company_id, start_date, end_date)
    
    return [
        {
            "xAxis": result['month'].strftime("%b-%y").lower(),
            "IPD": float(result['ipd_total']),
            "OPD": float(result['opd_total'])
        }
        for result in results
    ]

async def main(start_date, end_date, company_id):
    DATABASE_URL = "postgresql://postgres.catelzlfmdwiyqapheoh:FinKeepSahil@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"
    
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    async with asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=10) as pool:
        result = await get_monthly_totals(pool, start_date, end_date, company_id)
        print(json.dumps(result, indent=4))
        return result

if __name__ == "__main__":
    start_time = time.time()
    
    # Example usage
    start_date_str = "2023-04-01"
    end_date_str = "2024-03-31"
    company_id = 1  # Replace with actual company ID
    
    asyncio.run(main(start_date_str, end_date_str, company_id))
    
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")


# Code 2

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import date
import asyncpg
import asyncio
from datetime import datetime
from collections import defaultdict
import json
import time

app = FastAPI()

# Pydantic model for request validation
class DateRange(BaseModel):
    start_date: date
    end_date: date
    company_id: int

# Database connection parameters
DATABASE_URL = "postgresql://postgres.catelzlfmdwiyqapheoh:FinKeepSahil@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"

# Create a global connection pool
pool = None

async def fetch_data(query, *args):
    async with pool.acquire() as connection:
        return await connection.fetch(query, *args)

async def get_monthly_totals(start_date, end_date, company_id):
    query = """
        SELECT DATE_TRUNC('month', date) AS month,
               SUM(CASE WHEN voucher_type = 'Sales IPD' THEN ABS(amount) ELSE 0 END) AS ipd_total,
               SUM(CASE WHEN voucher_type = 'Sales OPD' THEN ABS(amount) ELSE 0 END) AS opd_total
        FROM trn_voucher
        WHERE voucher_type IN ('Sales IPD', 'Sales OPD')
          AND company_id = $1
          AND date BETWEEN $2 AND $3
        GROUP BY DATE_TRUNC('month', date)
        ORDER BY month;
    """
    results = await fetch_data(query, company_id, start_date, end_date)
    
    return [
        {
            "xAxis": result['month'].strftime("%b-%y").lower(),
            "IPD": float(result['ipd_total']),
            "OPD": float(result['opd_total'])
        }
        for result in results
    ]

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=10)

@app.on_event("shutdown")
async def shutdown():
    global pool
    await pool.close()

@app.post("/ipd_opd_revenue/")
async def monthly_totals(date_range: DateRange):
    try:
        start_time = time.time()
        result = await get_monthly_totals(date_range.start_date, date_range.end_date, date_range.company_id)
        end_time = time.time()
        execution_time = end_time - start_time
        return {"data": result, "execution_time": f"{execution_time:.2f} seconds"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)