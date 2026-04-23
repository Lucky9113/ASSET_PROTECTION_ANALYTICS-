import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta

# Set random seed for reproducibility
np.random.seed(42)

print("Generating 500,000 baseline transactions...")
n_transactions = 500000
employee_ids = [f"EMP_{str(i).zfill(3)}" for i in range(1, 201)] # 200 employees
item_skus = [f"SKU_{str(i).zfill(3)}" for i in range(1, 501)]    # 500 products

# Generate base data arrays
tx_ids = np.arange(1000000, 1000000 + n_transactions)
emp_ids = np.random.choice(employee_ids, n_transactions)
skus = np.random.choice(item_skus, n_transactions)
tx_types = np.random.choice(['Sale', 'Return'], n_transactions, p=[0.9, 0.1])
gross_amts = np.round(np.random.uniform(5.0, 500.0, n_transactions), 2)
disc_types = np.random.choice(['None', 'Standard', 'Employee', 'Contractor'], n_transactions, p=[0.75, 0.15, 0.05, 0.05])
overrides = np.random.choice([0, 1], n_transactions, p=[0.98, 0.02])
voids = np.zeros(n_transactions, dtype=int)
scan_times = np.round(np.random.uniform(2.0, 15.0, n_transactions), 1)
presence = np.round(np.random.uniform(0.7, 1.0, n_transactions), 2)
fraud = np.zeros(n_transactions, dtype=int)

# Create Timestamps (Starting from 30 days ago)
start_date = datetime.now() - timedelta(days=30)
timestamps = [start_date + timedelta(seconds=i * 5) for i in range(n_transactions)]

# Create DataFrame
df = pd.DataFrame({
    'Transaction_ID': tx_ids,
    'Timestamp': timestamps,
    'Employee_ID': emp_ids,
    'Item_SKU': skus,
    'Transaction_Type': tx_types,
    'Gross_Amount': gross_amts,
    'Discount_Type': disc_types,
    'Override_Flag': overrides,
    'Post_Payment_Void': voids,
    'Time_To_Scan_Sec': scan_times,
    'Presence_Score': presence,
    'Fraud_Label': fraud
})

# Apply standard legitimate discounts to Net Amount
df['Net_Amount'] = df['Gross_Amount'].copy()
df.loc[df['Discount_Type'] == 'Standard', 'Net_Amount'] *= 0.90
df.loc[df['Discount_Type'] == 'Employee', 'Net_Amount'] *= 0.80
df.loc[df['Discount_Type'] == 'Contractor', 'Net_Amount'] *= 0.85
df['Net_Amount'] = df['Net_Amount'].round(2)

print("Injecting internal theft patterns...")

# ---------------------------------------------------------
# INJECT PATTERN 1: CASH REGISTER THEFT (VOIDS)
# ---------------------------------------------------------
sale_indices = df[df['Transaction_Type'] == 'Sale'].index
fraud_idx_1 = np.random.choice(sale_indices, 300, replace=False)
df.loc[fraud_idx_1, 'Post_Payment_Void'] = 1
df.loc[fraud_idx_1, 'Net_Amount'] = 0 
df.loc[fraud_idx_1, 'Fraud_Label'] = 1

# ---------------------------------------------------------
# INJECT PATTERN 2: DISCOUNT ABUSE
# ---------------------------------------------------------
emp_discount_indices = df[df['Discount_Type'] == 'Employee'].index
fraud_idx_2 = np.random.choice(emp_discount_indices, 450, replace=False)
df.loc[fraud_idx_2, 'Net_Amount'] = (df.loc[fraud_idx_2, 'Gross_Amount'] * 0.10).round(2)
df.loc[fraud_idx_2, 'Fraud_Label'] = 1

# ---------------------------------------------------------
# INJECT PATTERN 3: SWEETHEARTING / PRICE MANIPULATION
# ---------------------------------------------------------
fraud_idx_3 = np.random.choice(df.index, 350, replace=False)
df.loc[fraud_idx_3, 'Override_Flag'] = 1
df.loc[fraud_idx_3, 'Time_To_Scan_Sec'] = np.round(np.random.uniform(0.1, 0.9, 350), 1) 
df.loc[fraud_idx_3, 'Net_Amount'] = (df.loc[fraud_idx_3, 'Gross_Amount'] * 0.05).round(2)
df.loc[fraud_idx_3, 'Fraud_Label'] = 1

# ---------------------------------------------------------
# INJECT PATTERN 4: GHOST RETURNS
# ---------------------------------------------------------
return_indices = df[df['Transaction_Type'] == 'Return'].index
fraud_idx_4 = np.random.choice(return_indices, 400, replace=False)
df.loc[fraud_idx_4, 'Presence_Score'] = np.round(np.random.uniform(0.0, 0.15, 400), 2)
df.loc[fraud_idx_4, 'Fraud_Label'] = 1

# ---------------------------------------------------------
# SAVE TO SQLITE DATABASE
# ---------------------------------------------------------
print("Saving data to SQLite database...")
conn = sqlite3.connect('pos_data.db')
# Convert Timestamp objects to string for SQLite compatibility
df['Timestamp'] = df['Timestamp'].astype(str)
df.to_sql('POS_Transactions', conn, if_exists='replace', index=False)
conn.close()

print("Success! Database 'pos_data.db' has been created.")
