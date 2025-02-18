import pandas as pd
import sqlite3

# Connect to SQLite database (or create one)
conn = sqlite3.connect("fetch_data.db")
cursor = conn.cursor()

# Number of transactions with valid user_id in users table -> 261
query1 = """
SELECT 
    count(*) as 'Count of transactions'
FROM users u
INNER JOIN transactions t on u.id = t.user_id
;
"""
print("\n\nNumber of transactions with valid user_id in users table\n")
result1 = pd.read_sql(query1, conn)
print(result1)

# Out of 261 the transactions with valid barcode in products table -> 144
query2 = """
SELECT 
    count(*) as 'Count of transactions'
FROM users u
INNER JOIN transactions t on u.id = t.user_id
INNER JOIN products p on p.barcode = t.barcode
;
"""
print("\n\nNumber of transactions with valid barcode in products table\n")
result2 = pd.read_sql(query2, conn)
print(result2)

# Out of 144 transactions, the valid ones where final_sale is not zero and quantity is also not zero -> 72
query3 = """
SELECT 
    count(user_id) as 'Count of transactions'
FROM users u
INNER JOIN transactions t on u.id = t.user_id
INNER JOIN products p on p.barcode = t.barcode
WHERE final_sale != 0 and final_quantity != 0
;
"""
print("\n\nNumber of valid transactions  where final_sale is not zero and quantity is also not zero\n")
result3 = pd.read_sql(query3, conn)
print(result3)

# Below query confirms that for each receipt_id there is only 1 user_id associated with it
query5 = """
select 
    count(DISTINCT user_id)
    receipt_id  
from transactions
group by receipt_id
having count(DISTINCT user_id) > 1
;
"""

r3 = pd.read_sql(query5, conn)
print(r3)

