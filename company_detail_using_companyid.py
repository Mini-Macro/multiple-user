import asyncpg
import asyncio
import time
from datetime import datetime
from statistics import mean, median
import pandas as pd

async def fetch_data(pool, query, *args):
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)

async def get_values_using_company_id(start_date: str, end_date: str, company_id: int):
    DATABASE_URL = "postgres://postgres.catelzlfmdwiyqapheoh:FinKeepSahil@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"

    result = []

    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    async with asyncpg.create_pool(DATABASE_URL, statement_cache_size=0, max_size=20, min_size=5) as pool:
        start_time = time.time()

        tasks = [
            # Combined query for Sales voucher count, debit amount, and particulars
            fetch_data(pool, """
                SELECT COUNT(*) AS count, 
                       array_agg(v.amount) AS amounts, 
                       array_agg(v.party_name) AS particulars
                FROM trn_voucher v
                INNER JOIN mst_vouchertype vt ON v.voucher_type = vt.name
                WHERE vt.parent = 'Sales' AND vt.company_id = $1 AND v.company_id = $1 AND v.date >= $2 AND v.date <= $3;
            """, company_id, start_date, end_date),
            # Direct expenses group debit amount
            fetch_data(pool, """
                SELECT SUM(trn_accounting.amount) AS total_debit_amount_direct_expenses
                FROM trn_voucher 
                JOIN trn_accounting ON trn_voucher.guid = trn_accounting.guid
                JOIN mst_ledger ON trn_accounting.ledger = mst_ledger.name
                JOIN mst_group ON mst_ledger.parent = mst_group.name
                WHERE mst_group.company_id = $1 AND mst_ledger.company_id = $1 AND mst_group.primary_group = 'Direct Expenses' 
                AND trn_voucher.voucher_type NOT IN ('Attendance', 'Delivery Note', 'Job Work In Order', 'Job Work Out Order', 'Material In', 'Material Out', 'Memorandum', 'Purchase Order','Receipt Note', 'Rejections In', 'Rejections Out', 'Reversing Journal', 'Sales Order') AND trn_voucher.date >= $2 AND trn_voucher.date <= $3;
            """, company_id, start_date, end_date),
            # Net profit from P&L statement
            fetch_data(pool, """
                SELECT amount
                FROM profit_loss_statement
                WHERE "Particulars" = 'Nett Profit' AND "company_id" = $1;
            """, company_id),
            # Debit from Interest & Financial group
            fetch_data(pool, """
                SELECT SUM(ta.amount) AS total_debit_amount
                FROM trn_accounting ta
                JOIN mst_ledger ml ON ta.ledger = ml.name
                JOIN mst_group mg ON ml.parent = mg.name
                JOIN trn_voucher tv ON ta.guid = tv.guid
                WHERE mg.company_id = $1 AND ml.company_id = $1 AND (mg.name = 'Interest & Finacial Charges Exp.' OR mg.parent = 'Interest & Finacial Charges Exp.') AND tv.date >= $2 AND tv.date <= $3;
            """, company_id, start_date, end_date),
        ]

        results = await asyncio.gather(*tasks)
        end_time = time.time()
        total_time = end_time - start_time

        # Card 1: Total footfall value
        sales_voucher_count = results[0][0]['count']
        debit_amount_sales_voucher = [abs(float(amount)) for amount in results[0][0]['amounts']]
        particulars_list = results[0][0]['particulars']
        result.append({"Label": 'Total Footfall', "Value": sales_voucher_count})

        # Card 2 & 3 : Average & Median Revenue
        total_revenue = sum(debit_amount_sales_voucher)
        average_debit_amount = mean(debit_amount_sales_voucher) if debit_amount_sales_voucher else 0
        median_debit_amount = median(debit_amount_sales_voucher) if debit_amount_sales_voucher else 0
        result.append({"Label": 'Average Revenue Per Patient', "Value": average_debit_amount})
        result.append({"Label": 'Median Revenue Per Patient', "Value": median_debit_amount})

        # Card 4: Operating cost
        total_debit_amount_direct_expenses = float(results[1][0]['total_debit_amount_direct_expenses']) if results[1][0]['total_debit_amount_direct_expenses'] is not None else 0.0
        result.append({"Label": 'Operating Cost', "Value": abs(total_debit_amount_direct_expenses)})

        # Card 5: EBIDTA
        amount_str = results[2][0]['amount'] if results[2] else '0'
        net_profit = float(amount_str.replace(',', '')) if amount_str is not None else 0.0
        debit_interest_financial = abs(float(results[3][0]['total_debit_amount']) if results[3][0]['total_debit_amount'] is not None else 0.0)
        ebidta = net_profit + debit_interest_financial
        result.append({"Label": 'EBIDTA', "Value": ebidta})

        # Card 6: Total Re-visits
        particulars_counts = pd.Series(particulars_list).value_counts()
        revisits_count = sum(count for count in particulars_counts if count > 1)
        result.append({"Label": 'Total Revisits', "Value": revisits_count})

        # Card 7: Daily Running Cost
        days_in_range = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1
        daily_running_cost = abs(total_debit_amount_direct_expenses) / days_in_range if days_in_range > 0 else 0
        result.append({"Label": 'Daily Running Cost', "Value": daily_running_cost})

        # Card 8: Operating Profits
        operating_profits = total_revenue - abs(total_debit_amount_direct_expenses)
        result.append({"Label": 'Operating Profits', "Value": operating_profits})

        return result

if __name__ == "__main__":
    company_id = 1  # Replace with the actual user ID
    start_date = "2023-01-01"  # Replace with the desired start date
    end_date = "2023-12-31"  # Replace with the desired end date
    asyncio.run(get_values_using_company_id(start_date, end_date, company_id))