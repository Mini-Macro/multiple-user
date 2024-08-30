import asyncpg
import os

DATABASE_URL = os.getenv("DATABASE_URL")

async def fetch_ledger_transactions(ledger_name: str):
    try:
        connection = await asyncpg.connect(DATABASE_URL)
        queries = {
            "vouchers": """SELECT * FROM trn_voucher WHERE ledger_name = $1""",
            "accounting_entries": """SELECT * FROM trn_accounting WHERE ledger = $1""",
            "inventory_entries": """SELECT * FROM trn_inventory WHERE item IN (SELECT guid FROM mst_stock_item WHERE name IN (SELECT name FROM mst_ledger WHERE guid = $1))""",
            "cost_centres": """SELECT * FROM trn_cost_centre WHERE ledger = $1""",
            "bills": """SELECT * FROM trn_bill WHERE ledger = $1""",
            "bank_transactions": """SELECT * FROM trn_bank WHERE ledger = $1""",
            "batches": """SELECT * FROM trn_batch WHERE item IN (SELECT guid FROM mst_stock_item WHERE name IN (SELECT name FROM mst_ledger WHERE guid = $1))"""
        }
        
        transactions = {}
        for key, query in queries.items():
            transactions[key] = await connection.fetch(query, ledger_name)

        return transactions
    finally:
        await connection.close()