import pandas as pd
import sqlite3

# Connect to SQLite database (or create one)
conn = sqlite3.connect("fetch_data.db")
cursor = conn.cursor()

# Load CSV files
users_df = pd.read_csv("USER.csv")

# Display first few rows of each dataset
print("\n\nUsers Data:\n") 
print(users_df.head(10))

print("\n\nMissing Values before dropping duplicate records:\n")
print(users_df.isnull().sum())

# Duplicate records in users table
print("\n\nNumber of Duplicate records:\n")
print(users_df.duplicated().sum())

# =============================== CONVERT TO PROPER DATATYPE =======================================

# Convert date columns to datetime format:
users_df['CREATED_DATE'] = pd.to_datetime(users_df['CREATED_DATE'], errors='coerce')
users_df['BIRTH_DATE'] = pd.to_datetime(users_df['BIRTH_DATE'], errors='coerce')

# Convert ID, STATE, LANGUAGE, GENDER to varchar
users_df['ID'] = users_df['ID'].astype(str)
users_df['STATE'] = users_df['STATE'].astype(str)
users_df['LANGUAGE'] = users_df['LANGUAGE'].astype(str)
users_df['GENDER'] = users_df['GENDER'].astype(str)

# Check the data types
print(users_df.dtypes)

# Check the unique values in each column
print("\n\nUnique STATE values\n",users_df['STATE'].unique())
print("\n\nUnique LANGUAGE values\n",users_df['LANGUAGE'].unique())
print("\n\nUnique GENDER values\n",users_df['GENDER'].unique())

# Filter the dataframe to include only duplicate users IDs
duplicate_users_df = users_df[users_df['ID'].duplicated(keep=False)]

# The percentage of duplicate ID rows out of the total rows
print("\n\nNumber of duplicate user IDs out of total rows is: \n")
print(duplicate_users_df.shape[0])


# ============================= CREATED_DATE & BIRTH_DATE INCONSISTENCY ================================

# Check where CREATED_DATE is before BIRTH_DATE
created_before_birthdate = users_df[users_df['CREATED_DATE'] < users_df['BIRTH_DATE']]

# Count occurrences
count_created_before_birthdate = created_before_birthdate.shape[0]

# Display results
print(f"\n\nNumber of rows where CREATED_DATE is before BIRTH_DATE: {count_created_before_birthdate}\n\n")

# Display some of these rows
print(created_before_birthdate[['ID','BIRTH_DATE', 'CREATED_DATE']].head(10))


# ======================= SAVE TO DB =============================================================

# Save data to SQLite
users_df.to_sql("users", conn, if_exists="replace", index=False)

# Commit changes and close connection
conn.commit()  
conn.close()  
