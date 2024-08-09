import asyncpg
import asyncio
import time 
from statistics import mean, median
from decimal import Decimal
import pandas as pd

async def fetch_data(conn, query, *args):
    return await conn.fetch(query, *args)

async def get_values_from_db(username):
    # For local pgadmin database
    # DATABASE_URL = "postgresql://postgres:Keeper04@localhost:5432/MultipleUserDB"

    # For remote supabase database
    DATABASE_URL = "postgres://postgres.catelzlfmdwiyqapheoh:FinKeepSahil@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"

    result = []

    async with asyncpg.create_pool(DATABASE_URL, statement_cache_size=0) as pool:
        start_time = time.time()
        tasks = [
            # Sales voucher count
            fetch_data(pool, """
                SELECT COUNT(*) as count
                FROM trn_voucher v
                INNER JOIN mst_vouchertype vt ON v.voucher_type = vt.name
                WHERE vt.parent = 'Sales' AND vt.username = $1 AND v.username = $2;
            """,username, username),
            # Sales voucher debit amount
            fetch_data(pool, """
                SELECT v.amount
                FROM trn_voucher v
                INNER JOIN mst_vouchertype vt ON v.voucher_type = vt.name
                WHERE vt.parent = 'Sales' AND vt.username = $1 AND v.username = $2;
            """,username, username),
            # Direct expenses group debit amount (Zero entries for direct expenses group in SP industries data)
            fetch_data(pool, """
                SELECT  SUM(trn_accounting.amount) AS total_debit_amount_direct_expenses
                FROM trn_voucher 
                JOIN trn_accounting ON trn_voucher.guid = trn_accounting.guid
                JOIN mst_ledger ON trn_accounting.ledger = mst_ledger.name
                JOIN mst_group ON mst_ledger.parent = mst_group.name
                WHERE (mst_group.username = $1) AND (mst_ledger.username = $2) AND (mst_group.primary_group = 'Direct Expenses') 
                AND trn_voucher.voucher_type NOT IN ('Attendance', 'Delivery Note', 'Job Work In Order', 'Job Work Out Order', 'Material In', 'Material Out', 'Memorandum', 'Purchase Order','Receipt Note', 'Rejections In', 'Rejections Out', 'Reversing Journal', 'Sales Order');
            """,username, username),
            # Particulars from sales voucher
            fetch_data(pool, """
                SELECT party_name AS particulars 
                FROM trn_voucher v
                INNER JOIN mst_vouchertype vt ON v.voucher_type = vt.name
                WHERE (vt.parent = 'Sales') AND (vt.username = $1) AND (v.username= $2);
            """,username, username),
            # Net profit from p&l statement
            fetch_data(pool, """
                SELECT amount
                FROM profit_loss_statement
                WHERE "Particulars" = 'Nett Profit' and "username" = $1;
            """,username),
            # Debit from Interest & Financial group
            fetch_data(pool, """
                SELECT SUM(ta.amount) AS total_debit_amount
                FROM trn_accounting ta
                JOIN mst_ledger ml ON ta.ledger = ml.name
                JOIN mst_group mg ON ml.parent = mg.name
                WHERE (mg.username = $1 AND ml.username = $2) AND (mg.name = 'Interest & Finacial Charges Exp.' or mg.parent = 'Interest & Finacial Charges Exp.');
            """,username, username),

        ]

        results = await asyncio.gather(*tasks)
        end_time = time.time()
        total_time = end_time - start_time

        # Card 1: Total footfall value
        

        sales_voucher_count = results[0][0]['count']
        result.append({"Label": 'Total Footfall', "Value": sales_voucher_count, "IconName": 'MdDashboard'})
        

        # Card 2 & 3 : Average & Median Revenue

        # total_debit_amount = results[1][0]['total_debit_amount'] if results[1][0]['total_debit_amount'] is not None else 0
        # debit_amount_sales_voucher = [float(row['amount']) for row in results[1]] if results[1] else []
        debit_amount_sales_voucher = [abs(float(row['amount'])) for row in results[1]] if results[1] else []

        total_revenue = sum(debit_amount_sales_voucher)

        # print("debit_amount_sales_voucher: ", total_revenue)

        # total_debit_amount = sum(debit_amount_sales_voucher)
        average_debit_amount = mean(debit_amount_sales_voucher) if debit_amount_sales_voucher else 0
        median_debit_amount = median(debit_amount_sales_voucher) if debit_amount_sales_voucher else 0

        # result.append({"Label": 'debit_amount_sales_voucher: ', "Value": total_revenue, "IconName": 'MdBarChart'}) -- for confirming return value with actual tally value 
        result.append({"Label": 'Average Revenue Per Patient', "Value": average_debit_amount, "IconName": 'MdBarChart'})
        result.append({"Label": 'Median Revenue Per Patient', "Value": median_debit_amount, "IconName": 'MdCloudUpload'})

        # Card 4: Operating cost = Debit amount from direct expenses group 

        total_debit_amount_direct_expenses = float(results[2][0]['total_debit_amount_direct_expenses']) if results[2][0]['total_debit_amount_direct_expenses'] is not None else 0.0
        result.append({"Label": 'Operating Cost', "Value": abs(total_debit_amount_direct_expenses), "IconName": 'MdHome'})

        # Card 5: EBIDTA

        amount_str = results[4][0]['amount']
        if amount_str is not None:
            amount_str = amount_str.replace(',', '')
            net_profit = float(amount_str)
        else:
            net_profit = 0.0
        # print("net_profit: ", net_profit)

        debit_interest_financial = abs(float(results[5][0]['total_debit_amount']) if results[5][0]['total_debit_amount'] is not None else 0.0)
        # print("debit_interest_financial: ", debit_interest_financial)
        
        ebidta = net_profit + debit_interest_financial
        # print("ebidta: ", ebidta)
        result.append({"Label": 'EBIDTA', "Value": ebidta, "IconName": 'MdLocalGroceryStore'})

        # Card 6: Total Re-visits

        # total_revisits = sales_voucher_count - len(debit_amount_sales_voucher)
        # result.append({"Label": 'Total Re-visits', "Value": total_revisits, "IconName": 'MdRepeat'})
        particulars_list = [row['particulars'] for row in results[3]]
        # print("particulars_list: ", particulars_list)
        particulars_counts = pd.Series(particulars_list).value_counts()
        # revisits_count = sum(particulars_counts // 2)
        revisits_count = sum(count for count in particulars_counts if count > 1)
        # print( "revisits_count: ", revisits_count)

        result.append({"Label": 'Total Revisits', "Value": revisits_count, "IconName": 'MdDashboard'})

        # Card 7: daily_running_cost
        # total_working_days = 268 right now hardcoding we need to find a way to calculate total number of working days
        daily_running_cost = abs(total_debit_amount_direct_expenses) / 268 # 268 will change for sp industries
        result.append({"Label": 'Daily Running Cost', "Value":  daily_running_cost, "IconName": 'MdDirectionsCar'})

        # Card 8: Operating Profits
        if (total_debit_amount_direct_expenses > 0):
            operating_profits = total_revenue + abs(total_debit_amount_direct_expenses)
        else:
            operating_profits = total_revenue - abs(total_debit_amount_direct_expenses)

        result.append({"Label": 'Operating Profits', "Value":   operating_profits, "IconName": 'MdLocalGroceryStore'})

        return result
    
if __name__ == "__main__":
    # username = 'asha'  # Replace with the actual username
    # asyncio.run(get_values_from_db(username))
    asyncio.run(get_values_from_db())






