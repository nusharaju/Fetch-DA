import pandas as pd
import sqlite3

# Connect to SQLite database (or create one)
conn = sqlite3.connect("fetch_data.db")
cursor = conn.cursor()

# Load CSV files
products_df = pd.read_csv("Products.csv")

# Display first few rows of each dataset
print("\Products Data:") 
print(products_df.head(10))

print("\nMissing Values before dropping duplicate records:")
print(products_df.isnull().sum())

# Duplicate records in products table
print("\nNumber of Duplicate records:")
print(products_df.duplicated().sum())

# Dropping duplicate rows from products table
products_df = products_df.drop_duplicates()

print("\nMissing Values after dropping duplicate records:")
print(products_df.isnull().sum())

# Convert BARCODE to string
products_df['BARCODE'] = products_df['BARCODE'].astype(str)

# Remove any trailing '.0'
products_df['BARCODE'] = products_df['BARCODE'].str.replace(r'\.0$', '', regex=True)

# Convert back to numeric (Int64), setting non-convertible values to NaN
products_df['BARCODE'] = pd.to_numeric(products_df['BARCODE'], errors='coerce').astype('Int64')

# Filter the dataframe to include only duplicate barcodes
duplicate_products_df = products_df[products_df['BARCODE'].duplicated(keep=False)]

# Number of duplicate barcodes
print("\nNumber of duplicate barcode rows is : ")
print(duplicate_products_df.shape[0])

# The percentage of duplicate barcode rows out of the total rows
print("\nPercentage of duplicate barcode rows out of total rows is: ")
print((duplicate_products_df.shape[0] / products_df.shape[0]) * 100)

# Drop all duplicate barcode values rows from the products table
products_df = products_df.drop_duplicates(subset=['BARCODE'], keep=False)

print(products_df.isnull().sum())

# Convert all columns except 'BARCODE' to string (VARCHAR equivalent)
for col in products_df.columns:
    if col != 'BARCODE':
        products_df[col] = products_df[col].astype(str)

# Verify data types
print(products_df.dtypes)

# Get the total number of unique items in CATEGORY_1 along with their counts
category1_counts = products_df["CATEGORY_1"].value_counts().reset_index()
category1_counts.columns = ["CATEGORY_1", "COUNT"]
print(category1_counts)

# Get the total number of unique items in MANUFACTURER along with their counts
manufacturers_count = products_df["MANUFACTURER"].value_counts().reset_index()
manufacturers_count.columns = ["MANUFACTURER", "COUNT"]
print(manufacturers_count.head(10))

# Get the total number of unique items in BRAND along with their counts
brand_count = products_df["BRAND"].value_counts().reset_index()
brand_count.columns = ["BRAND", "COUNT"]
print(brand_count.head(10))

# Number of unique brands associated with each manufacturer
manufacturer_brand_count = products_df.groupby("MANUFACTURER")["BRAND"].nunique().reset_index()
manufacturer_brand_count.columns = ["MANUFACTURER", "BRAND_COUNT"]

# Sort by BRAND_COUNT in descending order
manufacturer_brand_count = manufacturer_brand_count.sort_values(by="BRAND_COUNT", ascending=False)
print(manufacturer_brand_count.head(10))

# Category dependency

# If CATEGORY_1 is 'nan' then CATEGORY_2 is also 'nan'
category1_nan = products_df[products_df['CATEGORY_1'] == 'nan']
print(f"CATEGORY_1 -> {category1_nan['CATEGORY_1'].count()} , CATEGORY_2 -> {category1_nan['CATEGORY_2'].count()}\n")

# If CATEGORY_2 is 'nan' then CATEGORY_3 is also 'nan'
category2_nan = products_df[products_df['CATEGORY_2'] == 'nan']
print(f"CATEGORY_2 -> {category2_nan['CATEGORY_2'].count()} , CATEGORY_3 -> {category2_nan['CATEGORY_3'].count()}\n")

# If CATEGORY_3 is 'nan' then CATEGORY_4 is also 'nan'
category3_nan = products_df[products_df['CATEGORY_3'] == 'nan']
print(f"CATEGORY_3 -> {category3_nan['CATEGORY_3'].count()} , CATEGORY_4 -> {category3_nan['CATEGORY_4'].count()}\n")

# ======================= SAVE TO DB =============================================================

# Save data to SQLite
products_df.to_sql("products", conn, if_exists="replace", index=False)

# Commit changes and close connection
conn.commit()  
conn.close()  