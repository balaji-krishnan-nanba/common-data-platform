#!/usr/bin/env python3
"""Create new test files in Databricks."""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64
from datetime import datetime

# Check required environment variables
required_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID']
for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} environment variable not set")

w = WorkspaceClient()

print("🧪 Creating New Test Files for Dynamic Processing")
print("=" * 60)

# Create notebook to generate new test files
test_notebook = '''# Databricks notebook source
import pandas as pd
from datetime import datetime
import random
import os

print("📝 Creating NEW test files to verify dynamic processing...")
print("=" * 60)

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# 1. Create a new category of products - "Gaming"
print("\\n1️⃣ Creating Gaming Products CSV...")
gaming_products = pd.DataFrame({{
    'product_id': [f'GAM{{i:03d}}' for i in range(1, 21)],
    'product_name': [f'Gaming Product {{i}}' for i in range(1, 21)],
    'category': ['Gaming'] * 20,
    'sub_category': ['Console', 'PC', 'Mobile', 'Accessories'] * 5,
    'brand': ['GameBrandX', 'GameBrandY', 'GameBrandZ'] * 6 + ['GameBrandW'] * 2,
    'price': [round(random.uniform(50, 500), 2) for _ in range(20)],
    'cost': [round(random.uniform(25, 250), 2) for _ in range(20)],
    'status': ['ACTIVE'] * 18 + ['INACTIVE'] * 2,
    'created_date': datetime.now().strftime('%Y-%m-%d'),
    'modified_date': datetime.now().strftime('%Y-%m-%d')
}})

local_csv = "/tmp/gaming_products.csv"
gaming_products.to_csv(local_csv, index=False)
csv_path = f"abfss://raw-data@agentstge.dfs.core.windows.net/products/gaming/gaming_catalog_{{timestamp}}.csv"
dbutils.fs.cp(f"file:{{local_csv}}", csv_path)
print(f"✅ Created: {{csv_path}}")
os.remove(local_csv)

# 2. Create a new category - "Food & Beverage"
print("\\n2️⃣ Creating Food & Beverage Products CSV...")
food_products = pd.DataFrame({{
    'product_id': [f'FOOD{{i:03d}}' for i in range(1, 16)],
    'product_name': [f'Food Item {{i}}' for i in range(1, 16)],
    'category': ['Food & Beverage'] * 15,
    'sub_category': ['Snacks', 'Beverages', 'Frozen', 'Fresh', 'Packaged'] * 3,
    'brand': ['FoodBrandA', 'FoodBrandB', 'FoodBrandC'] * 5,
    'price': [round(random.uniform(5, 50), 2) for _ in range(15)],
    'cost': [round(random.uniform(2.5, 25), 2) for _ in range(15)],
    'status': ['ACTIVE'] * 15,
    'created_date': datetime.now().strftime('%Y-%m-%d'),
    'modified_date': datetime.now().strftime('%Y-%m-%d')
}})

local_csv = "/tmp/food_products.csv"
food_products.to_csv(local_csv, index=False)
csv_path = f"abfss://raw-data@agentstge.dfs.core.windows.net/products/food/food_catalog_{{timestamp}}.csv"
dbutils.fs.cp(f"file:{{local_csv}}", csv_path)
print(f"✅ Created: {{csv_path}}")
os.remove(local_csv)

# 3. Create today's sales data
print("\\n3️⃣ Creating Today's Sales Excel...")
today = datetime.now()
sales_data = []
stores = ['North_Store', 'South_Store', 'East_Store', 'West_Store', 'Central_Store', 'Online']

for i in range(100):  # 100 transactions
    sales_data.append({{
        'transaction_id': f'TRX{today.strftime("%Y%m%d")}{{i:04d}}',
        'transaction_date': today.strftime('%Y-%m-%d'),
        'customer_id': f'CUST{{random.randint(1, 500):05d}}',
        'product_code': random.choice([f'GAM{{random.randint(1,20):03d}}', f'FOOD{{random.randint(1,15):03d}}', 
                                     f'ELE{{random.randint(1,50):03d}}', f'CLO{{random.randint(1,50):03d}}']),
        'quantity': random.randint(1, 5),
        'unit_price': round(random.uniform(10, 300), 2),
        'total_amount': 0,
        'payment_method': random.choice(['Cash', 'Credit Card', 'Debit Card', 'Digital Wallet', 'Bank Transfer']),
        'store_location': random.choice(stores),
        'discount_percent': random.choice([0] * 8 + [5, 10, 15, 20]),
        'tax_amount': 0
    }})

# Calculate totals
for record in sales_data:
    subtotal = record['quantity'] * record['unit_price']
    discount = subtotal * (record['discount_percent'] / 100)
    record['total_amount'] = round((subtotal - discount) * 1.08, 2)
    record['tax_amount'] = round((subtotal - discount) * 0.08, 2)

df_sales = pd.DataFrame(sales_data)
local_excel = "/tmp/today_sales.xlsx"
df_sales.to_excel(local_excel, index=False, sheet_name='Sales Data')

excel_path = f"abfss://raw-data@agentstge.dfs.core.windows.net/sales/daily/{{today.year}}/{{today.month:02d}}/{{today.day:02d}}/sales_{{today.strftime('%Y%m%d')}}_{{timestamp}}.xlsx"
dbutils.fs.cp(f"file:{{local_excel}}", excel_path)
print(f"✅ Created: {{excel_path}}")
os.remove(local_excel)

# 4. Create a special promo sales file
print("\\n4️⃣ Creating Special Promo Sales Excel...")
promo_sales = []
for i in range(50):  # 50 promo transactions
    promo_sales.append({{
        'transaction_id': f'PROMO{today.strftime("%Y%m%d")}{{i:04d}}',
        'transaction_date': today.strftime('%Y-%m-%d'),
        'customer_id': f'PCUST{{random.randint(1, 200):05d}}',
        'product_code': random.choice([f'GAM{{random.randint(1,20):03d}}', f'ELE{{random.randint(1,50):03d}}']),
        'quantity': random.randint(1, 3),
        'unit_price': round(random.uniform(50, 500), 2),
        'total_amount': 0,
        'payment_method': random.choice(['Credit Card', 'Digital Wallet']),
        'store_location': 'Online',
        'discount_percent': random.choice([20, 25, 30, 35, 40]),  # High discounts for promo
        'tax_amount': 0
    }})

for record in promo_sales:
    subtotal = record['quantity'] * record['unit_price']
    discount = subtotal * (record['discount_percent'] / 100)
    record['total_amount'] = round((subtotal - discount) * 1.08, 2)
    record['tax_amount'] = round((subtotal - discount) * 0.08, 2)

df_promo = pd.DataFrame(promo_sales)
local_promo = "/tmp/promo_sales.xlsx"
df_promo.to_excel(local_promo, index=False, sheet_name='Promo Sales')

promo_path = f"abfss://raw-data@agentstge.dfs.core.windows.net/sales/promo/{{today.year}}/{{today.month:02d}}/promo_sales_{{timestamp}}.xlsx"
dbutils.fs.cp(f"file:{{local_promo}}", promo_path)
print(f"✅ Created: {{promo_path}}")
os.remove(local_promo)

# Summary
print(f"\\n📊 NEW FILES CREATED:")
print(f"   - 2 new product categories (Gaming, Food & Beverage)")
print(f"   - 35 new products total")
print(f"   - 150 new sales transactions")
print(f"   - Files saved with timestamp: {{timestamp}}")
print(f"\\n✅ These files should be automatically processed by the pipeline!")

dbutils.notebook.exit("SUCCESS")
'''

# Upload notebook
notebook_path = "/Users/balaji.krishnan@nanba.co.uk/create_new_test_files"
w.workspace.import_(
    path=notebook_path,
    content=base64.b64encode(test_notebook.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Create and run job
job = w.jobs.create(
    name=f"Create New Test Files - {datetime.now().strftime('%Y%m%d_%H%M%S')}",
    tasks=[
        jobs.Task(
            task_key="create_files",
            notebook_task=jobs.NotebookTask(
                notebook_path=notebook_path
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ File creation started: Run ID {run.run_id}")
print(f"📊 Monitor at: https://adb-2908121449961741.1.azuredatabricks.net/#job/{job.job_id}/run/{run.run_id}")
print("\n📁 Creating new test files:")
print("   - Gaming products category")
print("   - Food & Beverage products category") 
print("   - Today's sales transactions")
print("   - Special promo sales")
print("\n⏳ Files will be created in ~1-2 minutes")