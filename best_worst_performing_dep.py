import asyncpg
from datetime import datetime

async def get_total_voucher_amount(company_id:int, start_date:str, end_date:str):
    DATABASE_URL = "postgresql://postgres.catelzlfmdwiyqapheoh:FinKeepSahil@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"

    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    query = """
    SELECT voucher_type, SUM(amount) AS total_amount,  COUNT(*) AS voucher_count
    FROM trn_voucher
    WHERE trn_voucher.company_id = $1
    AND trn_voucher.date >= $2 AND trn_voucher.date <= $3
    GROUP BY voucher_type;
    """
    async def fetch_data(pool, query, *args):
        async with pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async with asyncpg.create_pool(DATABASE_URL, statement_cache_size=0) as pool:
            result = await fetch_data(pool, query, company_id, start_date, end_date)
    
    if not result:
        return {
            "highest_amount": "None", "highest_voucher_type": "None", "highest_count": "None",
            "lowest_amount": "None", "lowest_voucher_type": "None", "lowest_count": "None"
        }

    # Extract amounts and voucher types from the result
    amounts = [float(row['total_amount']) for row in result]
    voucher_types = [row['voucher_type'] for row in result]
    counts = [int(row['voucher_count']) for row in result]

    # Determine the highest and lowest amounts and their voucher types
    max_amount_index = amounts.index(max(amounts))
    min_amount_index = amounts.index(min(amounts))

    highest_amount = amounts[max_amount_index]
    lowest_amount = amounts[min_amount_index]

    highest_voucher_type = voucher_types[max_amount_index]
    lowest_voucher_type = voucher_types[min_amount_index]

    highest_count = counts[max_amount_index]
    lowest_count = counts[min_amount_index]

    return {
        "best_performing_department_amount": highest_amount,
        "best_performing_department_name": highest_voucher_type,
        "best_performing_department_patient_count": highest_count,
        "worst_performing_department_amount": lowest_amount,
        "worst_performing_department_name": lowest_voucher_type,
        "worst_performing_department_patient_count": lowest_count
    }
