import pandas as pd
import sqlite3

# Connect to SQLite database (or create one)
conn = sqlite3.connect("fetch_data.db")
cursor = conn.cursor()

# Load CSV files
transactions_df = pd.read_csv("Transaction.csv")

# Display first few rows of each dataset
print("\n\nTransactions Data:\n")
print(transactions_df.head())

print("\n\nMissing Values before dropping duplicate records:")
print(transactions_df.isnull().sum())

print("\n\nNumber of Duplicate records:\n")
print(transactions_df.duplicated().sum())

# Dropping duplicate rows from transactions table
transactions_df = transactions_df.drop_duplicates()

# Convert date columns to datetime format:
transactions_df['PURCHASE_DATE'] = pd.to_datetime(transactions_df['PURCHASE_DATE'], errors='coerce')
transactions_df['SCAN_DATE'] = pd.to_datetime(transactions_df['SCAN_DATE'], errors='coerce')

# ============================= FINAL_QUANTITY ================================

# Creating new field 'FINAL_QUANTITY2'
transactions_df['FINAL_QUANTITY2'] = pd.to_numeric(transactions_df['FINAL_QUANTITY'], errors='coerce')

# To check what is the actual value for FINAL_QUANTITY where the error is occuring, I am taking the data where FINAL_QUANTITY2 = 'NaN'
filtered_df = transactions_df[transactions_df['FINAL_QUANTITY2'].isna()]

# Getting unique values from the column which are not getting converted properly
print(f"\n\nUnique values in FINAL_QUANTITY where it did not get converted properly: \n{filtered_df['FINAL_QUANTITY'].unique()}")

# Replace the 'zero' with integer value '0' and convert the column to numeric
transactions_df['FINAL_QUANTITY'] = transactions_df['FINAL_QUANTITY'].replace('zero', '0')
transactions_df['FINAL_QUANTITY'] = pd.to_numeric(transactions_df['FINAL_QUANTITY'], errors='coerce')

# Remove the column 'FINAL_QUANTITY2'
transactions_df.drop(columns=['FINAL_QUANTITY2'], inplace=True)

# ============================= FINAL_SALE ================================

# Repeating same steps for FINAL_SALE column
transactions_df['FINAL_SALE2'] = pd.to_numeric(transactions_df['FINAL_SALE'], errors='coerce')
filtered_df = transactions_df[transactions_df['FINAL_SALE2'].isna()]

# Getting unique values from the column which are not getting converted properly
print(f"\n\nUnique values in FINAL SALE where it did not get converted properly: \n{filtered_df['FINAL_SALE'].unique()}")

# # Replace the ' ' with integer value '0' and convert the column to numeric
transactions_df['FINAL_SALE'] = transactions_df['FINAL_SALE'].replace(' ', '0')
transactions_df['FINAL_SALE'] = pd.to_numeric(transactions_df['FINAL_SALE'], errors='coerce')

# #Remove the column 'FINAL_SALE2'
transactions_df.drop(columns=['FINAL_SALE2'], inplace=True)

# Rechecking if all null values are taken care of or not
print("\n\nTransactions:\n", transactions_df.isnull().sum())

# ============================= BARCODE ================================

# Convert BARCODE to string
transactions_df['BARCODE'] = transactions_df['BARCODE'].astype(str)

# Remove any trailing '.0'
transactions_df['BARCODE'] = transactions_df['BARCODE'].str.replace(r'\.0$', '', regex=True)

# Convert back to numeric (Int64), setting non-convertible values to NaN
transactions_df['BARCODE'] = pd.to_numeric(transactions_df['BARCODE'], errors='coerce').astype('Int64')

# Replace <NA> with 0
transactions_df['BARCODE'] = transactions_df['BARCODE'].fillna(0).astype('Int64')

# ============================= TRANSACTION DURATION ================================

# Find start and end dates
start_date = transactions_df['SCAN_DATE'].min()
end_date = transactions_df['SCAN_DATE'].max()

# Calculate the total duration
duration = end_date - start_date

# Print duration of transaction
print(f"\n\nTotal duration of transaction data: {duration.days} days\n")

# ============================= FINAL_SALE & FINAL_QUANTITY INCONSISTENCY ================================

# Count rows where FINAL_SALE has values but FINAL_QUANTITY is zero
final_sale_nonzero_quantity_zero = transactions_df[
    (transactions_df['FINAL_SALE'] > 0) & (transactions_df['FINAL_QUANTITY'] == 0)
].shape[0]

# Count rows where FINAL_QUANTITY has values but FINAL_SALE is zero
final_quantity_nonzero_sale_zero = transactions_df[
    (transactions_df['FINAL_QUANTITY'] > 0) & (transactions_df['FINAL_SALE'] == 0)
].shape[0]

# Display results
print(f"Rows where FINAL_SALE > 0 but FINAL_QUANTITY == 0: {final_sale_nonzero_quantity_zero}")
print(f"Rows where FINAL_QUANTITY > 0 but FINAL_SALE == 0: {final_quantity_nonzero_sale_zero}")


# ============================= SCAN_DATE & PURCHASE_DATE INCONSISTENCY ================================

# Create new columns without the timestamp
transactions_df['SCAN_DATE_ONLY'] = transactions_df['SCAN_DATE'].dt.date
transactions_df['PURCHASE_DATE_ONLY'] = transactions_df['PURCHASE_DATE'].dt.date

# Check where SCAN_DATE_ONLY is before PURCHASE_DATE_ONLY
scan_before_purchase = transactions_df[transactions_df['SCAN_DATE_ONLY'] < transactions_df['PURCHASE_DATE_ONLY']]

# Count occurrences
count_scan_before_purchase = scan_before_purchase.shape[0]

# Display results
print(f"\n\nNumber of rows where SCAN_DATE_ONLY is before PURCHASE_DATE_ONLY: {count_scan_before_purchase}\n\n")

# Display some of these rows
print(scan_before_purchase[['PURCHASE_DATE_ONLY', 'SCAN_DATE_ONLY']].head(10))

# ======================= SAVE TO DB =============================================================

# Save data to SQLite
transactions_df.to_sql("transactions", conn, if_exists="replace", index=False)

# Commit changes and close connection
conn.commit()  
conn.close()  
