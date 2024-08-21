import asyncpg
import asyncio
from datetime import datetime, date
from collections import defaultdict
import json

# Fetch data from the database
async def fetch_data(pool, query, *args):
    async with pool.acquire() as connection:
        return await connection.fetch(query, *args)

# Get all vouchers
async def get_all_vouchers(pool, company_id):
    query = """
        SELECT date, amount, voucher_type
        FROM trn_voucher
        WHERE voucher_type IN ('Sales IPD', 'Sales OPD') and company_id = $1;
    """
    return await fetch_data(pool, query, company_id)

# Calculate monthly totals
async def get_monthly_totals(pool, start_date, end_date, company_id):
    vouchers = await get_all_vouchers(pool, company_id)

    # Convert start_date and end_date to datetime objects for consistent comparison
    start_datetime = datetime.combine(start_date, datetime.min.time()).date()
    end_datetime = datetime.combine(end_date, datetime.max.time()).date()

    # Filter vouchers within the date range
    filtered_vouchers_ipd = [
        voucher for voucher in vouchers
        if voucher['voucher_type'] == 'Sales IPD' and start_datetime <= voucher['date'] <= end_datetime
    ]

    filtered_vouchers_opd = [
        voucher for voucher in vouchers
        if voucher['voucher_type'] == 'Sales OPD' and start_datetime <= voucher['date'] <= end_datetime
    ]

    # Calculate monthly totals for Sales IPD
    monthly_totals_ipd = defaultdict(float)
    for voucher in filtered_vouchers_ipd:
        month = voucher['date'].strftime("%b-%y").lower()
        monthly_totals_ipd[month] += float(voucher['amount'])
        # monthly_totals_ipd[month] += abs(float(voucher['amount']))  # Convert to positive using abs()

    # Calculate monthly totals for Sales OPD
    monthly_totals_opd = defaultdict(float)
    for voucher in filtered_vouchers_opd:
        month = voucher['date'].strftime("%b-%y").lower()
        monthly_totals_opd[month] += float(voucher['amount'])
        # monthly_totals_opd[month] += abs(float(voucher['amount']))  # Convert to positive using abs()

    # Calculate total sum of total_amount across all months -- for ensuring there is no loss of data 
    # total_sum = sum(monthly_totals.values())

    # Convert to a list of dictionaries for the response
    result = []
    for month in sorted(set(monthly_totals_ipd.keys()) | set(monthly_totals_opd.keys())):
        entry = {
            "xAxis": month,
            "IPD": abs(monthly_totals_ipd[month]),
            "OPD": abs(monthly_totals_opd[month])
        }
        result.append(entry)

    # return result, total_sum
    return result



# Main function to execute the async operations
async def main(start_date, end_date, company_id):
    DATABASE_URL = "postgresql://postgres.catelzlfmdwiyqapheoh:FinKeepSahil@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"
    
    # Define the date range
    # start_date_str = "2023-04-01"
    # end_date_str = "2024-03-31"
    
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    async with asyncpg.create_pool(DATABASE_URL, statement_cache_size=0) as pool:
        result = await get_monthly_totals(pool, start_date, end_date, company_id)
        # for entry in result:
            # entry['IPD'] = abs(entry['IPD'])
            # entry['OPD'] = abs(entry['OPD'])

        print(json.dumps(result, indent=4))
        return result

        
# Run the main function
if __name__ == "__main__":
    # start_date_str = "2023-04-01"
    # end_date_str = "2024-03-31"
    # asyncio.run(main(start_date_str, end_date_str ))
    asyncio.run(main())