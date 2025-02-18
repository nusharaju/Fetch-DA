import pandas as pd
import sqlite3

# Increase display options for better visibility
pd.set_option('display.max_rows', 1000)  
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)

# Connect to SQLite database (or create one)
conn = sqlite3.connect("fetch_data.db")
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode=WAL;")
cursor.execute("PRAGMA busy_timeout = 5000;")

# ========================================== CLOSED ENDED QUESTIONS ==========================================

# Q. What are the top 5 brands by receipts scanned among users 21 and over?

# Assumptions:
# 1. Not considering records where the brand name is unknown or 'nan'
# Reasoning: We don't know if the brands which are marked as 'nan' are same or different brands
# Grouping it as 1 brand and showing it on top 5 does not make sense

query1 = """
WITH valid_users as (SELECT 
    receipt_id,
    strftime('%Y', 'now') - strftime('%Y', birth_date) - (strftime('%m-%d', 'now') < strftime('%m-%d', birth_date)) AS Age,
    brand
FROM users u
INNER JOIN transactions t on t.user_id = u.id
INNER JOIN products p on p.barcode = t.barcode)

SELECT brand as "Brand name", count(receipt_id) as "Number of receipts"
FROM valid_users
WHERE age >= 21 AND brand != 'nan'
GROUP BY brand
ORDER BY count(receipt_id) DESC
LIMIT 5
;
"""
print("\n\n Top 5 brands by receipts scanned among users 21 and over: \n")
result1 = pd.read_sql(query1, conn)
print(result1)


# What are the top 5 brands by sales among users that have had their account for at least six months?

# Assumptions: 
# Not considering brands with unknown value or 'nan' value
# Included the rows where final_quantity is 0, because in many rows even though quantity is 0 the sale value is there

query2 = """

WITH eligible_users as (
   SELECT
       id,
       (strftime('%Y', 'now') - strftime('%Y', created_date)) * 12 + 
       (strftime('%m', 'now') - strftime('%m', created_date)) AS account_lifetime_months
   FROM users
),

sales as (
   SELECT
       id,
       final_sale,
       brand
   FROM eligible_users u
   INNER JOIN transactions t on u.id = t.user_id
   INNER JOIN products p on p.barcode = t.barcode
   WHERE account_lifetime_months >= 6 and brand != 'nan'
)

SELECT brand, sum(final_sale) as "Total sales"
FROM sales
GROUP BY brand
ORDER BY sum(final_sale) DESC
LIMIT 5
;
"""
print("\n\n Top 5 brands by sales among users that have had their account for at least six months: \n")
result2 = pd.read_sql(query2, conn)
print(result2)


# Q. What is the percentage of sales in the Health & Wellness category by generation?

# https://www.parents.com/parenting/better-parenting/style/generation-names-and-years-a-cheat-sheet-for-parents/
# Taking reference for generation from above link

query3 = """
WITH valid_transactions as (
    SELECT 
        *
    FROM products p
    INNER JOIN transactions t on t.barcode = p.barcode
    INNER JOIN users u on u.id = t.user_id
    WHERE category_1 = 'Health & Wellness'
),

gen_users as (
    SELECT 
        id,
        CASE 
            when strftime('%Y', birth_date) BETWEEN '1900' AND '1927' then 'Greatest Generation'
            when strftime('%Y', birth_date) BETWEEN '1928' AND '1945' then 'Silent Generation'
            when strftime('%Y', birth_date) BETWEEN '1946' AND '1964' then 'Baby Boom Generation'
            when strftime('%Y', birth_date) BETWEEN '1965' AND '1980' then 'Generation X'
            when strftime('%Y', birth_date) BETWEEN '1981' AND '1996' then 'Millennial'
            when strftime('%Y', birth_date) BETWEEN '1997' AND '2010' then 'Generation Z'
            when strftime('%Y', birth_date) BETWEEN '2011' AND '2024' then 'Generation Alpha'
            END Generation
    FROM users
),

total_sales as (
    SELECT
        SUM(final_sale) as total_health_wellness_sales
    FROM valid_transactions
)

SELECT 
    generation,
    ROUND((SUM(final_sale) / (select total_health_wellness_sales from total_sales)) * 100.0, 2) as "Health & Wellness Sales %"
FROM valid_transactions v
INNER JOIN gen_users g on v.user_id = g.id
GROUP BY generation
ORDER BY "Health & Wellness Sales %" DESC
;

"""

print("\n\n Percentage of sales in the Health & Wellness category by generation \n")
result3 = pd.read_sql(query3, conn)
print(result3)


# ========================================== OPEN ENDED QUESTIONS ==========================================

# Who are Fetch’s Power Users?

# Assumptions:
# A power user in Fetch is defined as a user who:
# 1. Has a transaction count above the average -> shows high engagement.
# 2. Generates sales more than the average -> indicates high spending behavior.
# 3. Has been active on Fetch longer than the average user -> indicated loyalty & retention.
# Not including product table here because there are some barcodes which are not present in the products table. 
# This might be because it's just a sample data provided here instead of the actual entire products table

# For reference avg_transactions = 1.373626,  avg_sales = 6.62967  avg_lifetime = 36.131868 months

query4 = """
WITH valid_users as (select 
    id,
    COUNT(distinct receipt_id) as number_transactions,
    SUM(final_sale) as total_sales,
    (strftime('%Y', 'now') - strftime('%Y', created_date)) * 12 +
       (strftime('%m', 'now') - strftime('%m', created_date)) AS account_lifetime_in_months
FROM transactions t
INNER JOIN users u on u.id = t.user_id
WHERE final_sale != 0 and final_quantity != 0
GROUP BY id, account_lifetime_in_months
),

cte_avg as (
    SELECT 
        id,
        number_transactions,
        total_sales,
        account_lifetime_in_months,
        AVG(number_transactions) OVER () AS avg_transactions,
        AVG(total_sales) OVER () AS avg_sales,
        AVG(account_lifetime_in_months) OVER () AS avg_lifetime
    FROM valid_users
)

SELECT 
    id,
    number_transactions,
    total_sales,
    account_lifetime_in_months
FROM cte_avg
WHERE 
    number_transactions > avg_transactions 
    AND total_sales > avg_sales 
    AND account_lifetime_in_months > avg_lifetime
ORDER BY number_transactions DESC, total_sales DESC, account_lifetime_in_months DESC;
"""

print("\n\n Fetch’s Power Users \n")
result4 = pd.read_sql(query4, conn)
print(result4)

# Q. Which is the leading brand in the Dips & Salsa category?
# Assumption: The brand which had the most sales can be considered a leading brand

query5 = """
SELECT 
    brand,
    COUNT(t.barcode) as number_of_products_sold,
    SUM(final_sale) as total_sales
FROM products p 
INNER JOIN transactions t on p.barcode = t.barcode
WHERE category_2 = 'Dips & Salsa' and final_sale != 0 and final_quantity != 0
GROUP BY brand
ORDER BY total_sales desc
LIMIT 1
"""

print("\n\n Leading brand in the Dips & Salsa category \n")
result5 = pd.read_sql(query5, conn)
print(result5)


# Q. At what percent has Fetch grown year over year?
# Assumptions:
# Fetch's growth can be considered as follows:
# 1. By what percentage the customer base is growing each year

query6 = """
WITH user_growth as (
    SELECT 
        strftime('%Y', created_date) as Year,
        LAG (count(id), 1, 0) OVER (ORDER BY  strftime('%Y', created_date)) as previousTotal,
        COUNT(id) as currentTotal
    FROM users
    GROUP BY Year
)
SELECT 
    Year,
    previousTotal,
    currentTotal,
    ROUND(((currentTotal - previousTotal)/(previousTotal * 1.0)) * 100.00,2) as customer_growth_yoy_percent
FROM user_growth
WHERE previousTotal != 0
"""

print("\n\n Fetch's growth year over year \n")
result6 = pd.read_sql(query6, conn)
print(result6)