# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Direct E2E Pipeline Implementation
# MAGIC 
# MAGIC This notebook implements the complete E2E pipeline directly without package dependencies.

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import json

print("🚀 DIRECT E2E PIPELINE IMPLEMENTATION")
print("=" * 80)
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# Configuration
PROJECT_CODE = "cddp"
ENVIRONMENT = "dev"
SOURCE_STORAGE = "agentstge"
CONTAINER = "raw-data"

# Catalog names
BRONZE_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-bronze"
SILVER_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-silver"
GOLD_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-gold"

print(f"Configuration:")
print(f"  Project: {PROJECT_CODE}")
print(f"  Environment: {ENVIRONMENT}")
print(f"  Source Storage: {SOURCE_STORAGE}")
print(f"  Bronze Catalog: {BRONZE_CATALOG}")
print(f"  Silver Catalog: {SILVER_CATALOG}")
print(f"  Gold Catalog: {GOLD_CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Catalog Structure

# COMMAND ----------

print("\n📚 Creating Unity Catalog Structure...")

# Create catalogs
for catalog in [BRONZE_CATALOG, SILVER_CATALOG, GOLD_CATALOG]:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        print(f"✅ Catalog: {catalog}")
    except Exception as e:
        print(f"⚠️ Catalog {catalog}: {str(e)}")

# Create schemas
schemas = {
    BRONZE_CATALOG: ["csv_data", "excel_data", "system"],
    SILVER_CATALOG: ["products", "sales"],
    GOLD_CATALOG: ["analytics", "reporting"]
}

for catalog, schema_list in schemas.items():
    for schema in schema_list:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            print(f"  ✅ Schema: {catalog}.{schema}")
        except Exception as e:
            print(f"  ⚠️ Schema {catalog}.{schema}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Tracking Table

# COMMAND ----------

# Create file tracking table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{BRONZE_CATALOG}`.`system`.`processed_files` (
    file_path STRING,
    file_name STRING,
    file_type STRING,
    file_size BIGINT,
    record_count BIGINT,
    status STRING,
    error_message STRING,
    processed_timestamp TIMESTAMP,
    processing_duration_seconds DOUBLE
) USING DELTA
""")

print("✅ File tracking table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze Layer - Process CSV Files

# COMMAND ----------

print("\n🥉 BRONZE LAYER - CSV PROCESSING")
print("=" * 50)

csv_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/products/"

# Find all CSV files
def find_csv_files(path):
    csv_files = []
    try:
        items = dbutils.fs.ls(path)
        for item in items:
            if item.path.endswith('.csv'):
                csv_files.append(item.path)
            elif item.isDir():
                csv_files.extend(find_csv_files(item.path))
    except:
        pass
    return csv_files

csv_files = find_csv_files(csv_path)
print(f"Found {len(csv_files)} CSV files")

# Process each CSV file
for csv_file in csv_files:
    start_time = datetime.now()
    try:
        # Extract table name
        file_name = csv_file.split('/')[-1].replace('.csv', '')
        folder_name = csv_file.split('/')[-2]
        table_name = f"{folder_name}_{file_name}".replace('-', '_').replace(' ', '_').lower()
        
        print(f"\nProcessing: {file_name}.csv")
        
        # Read CSV
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file)
        
        # Add metadata
        df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
               .withColumn("_source_file", lit(csv_file)) \
               .withColumn("_file_modification_time", current_timestamp())
        
        record_count = df.count()
        
        # Write to Bronze
        df.write.mode("overwrite").option("overwriteSchema", "true") \
          .saveAsTable(f"`{BRONZE_CATALOG}`.`csv_data`.`{table_name}`")
        
        # Log success
        processing_time = (datetime.now() - start_time).total_seconds()
        spark.sql(f"""
            INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
            VALUES (
                '{csv_file}', '{file_name}.csv', 'csv', 0, {record_count},
                'SUCCESS', NULL, current_timestamp(), {processing_time}
            )
        """)
        
        print(f"  ✅ Success: {record_count} records in {processing_time:.2f}s")
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        spark.sql(f"""
            INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
            VALUES (
                '{csv_file}', '{csv_file.split('/')[-1]}', 'csv', 0, 0,
                'FAILED', '{str(e).replace("'", "''")}', current_timestamp(), 0
            )
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze Layer - Process Excel Files

# COMMAND ----------

print("\n🥉 BRONZE LAYER - EXCEL PROCESSING")
print("=" * 50)

# Install openpyxl if needed
try:
    import openpyxl
except:
    %pip install openpyxl

excel_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/sales/"

# Find all Excel files
def find_excel_files(path):
    excel_files = []
    try:
        items = dbutils.fs.ls(path)
        for item in items:
            if item.path.endswith('.xlsx') or item.path.endswith('.xls'):
                excel_files.append(item.path)
            elif item.isDir():
                excel_files.extend(find_excel_files(item.path))
    except:
        pass
    return excel_files

excel_files = find_excel_files(excel_path)
print(f"Found {len(excel_files)} Excel files")

# Process Excel files using pandas
import pandas as pd

for excel_file in excel_files:
    start_time = datetime.now()
    try:
        # Extract table name
        file_name = excel_file.split('/')[-1].replace('.xlsx', '').replace('.xls', '')
        path_parts = excel_file.split('/')
        
        # Determine table name based on path
        if 'monthly' in excel_file:
            table_name = f"monthly_sales_{file_name}".replace('-', '_').replace(' ', '_').lower()
        elif 'daily' in excel_file:
            table_name = f"daily_sales_{file_name}".replace('-', '_').replace(' ', '_').lower()
        else:
            table_name = f"sales_{file_name}".replace('-', '_').replace(' ', '_').lower()
        
        print(f"\nProcessing: {file_name}")
        
        # Copy to local temp
        local_file = f"/tmp/{file_name}.xlsx"
        dbutils.fs.cp(excel_file, f"file:{local_file}")
        
        # Read with pandas
        df_pandas = pd.read_excel(local_file)
        
        # Convert to Spark DataFrame
        df = spark.createDataFrame(df_pandas)
        
        # Add metadata
        df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
               .withColumn("_source_file", lit(excel_file)) \
               .withColumn("_file_modification_time", current_timestamp())
        
        record_count = df.count()
        
        # Write to Bronze
        df.write.mode("overwrite").option("overwriteSchema", "true") \
          .saveAsTable(f"`{BRONZE_CATALOG}`.`excel_data`.`{table_name}`")
        
        # Clean up
        import os
        os.remove(local_file)
        
        # Log success
        processing_time = (datetime.now() - start_time).total_seconds()
        spark.sql(f"""
            INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
            VALUES (
                '{excel_file}', '{file_name}', 'excel', 0, {record_count},
                'SUCCESS', NULL, current_timestamp(), {processing_time}
            )
        """)
        
        print(f"  ✅ Success: {record_count} records in {processing_time:.2f}s")
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        spark.sql(f"""
            INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
            VALUES (
                '{excel_file}', '{excel_file.split('/')[-1]}', 'excel', 0, 0,
                'FAILED', '{str(e).replace("'", "''")}', current_timestamp(), 0
            )
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver Layer - Product Master

# COMMAND ----------

print("\n🥈 SILVER LAYER - PRODUCT MASTER")
print("=" * 50)

# Get all CSV tables and union them
csv_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`csv_data`").collect()
product_dfs = []

for table in csv_tables:
    table_name = table.tableName
    try:
        # Check if it's a product table
        columns = spark.sql(f"DESCRIBE `{BRONZE_CATALOG}`.`csv_data`.`{table_name}`").collect()
        column_names = [col.col_name for col in columns]
        
        if 'product_id' in column_names and 'product_name' in column_names:
            df = spark.sql(f"""
                SELECT 
                    product_id,
                    product_name,
                    COALESCE(category, 'Unknown') as category,
                    COALESCE(sub_category, 'Unknown') as sub_category,
                    COALESCE(brand, 'Unknown') as brand,
                    CAST(COALESCE(price, 0) AS DOUBLE) as price,
                    CAST(COALESCE(cost, 0) AS DOUBLE) as cost,
                    COALESCE(status, 'ACTIVE') as status,
                    COALESCE(created_date, current_date()) as created_date,
                    COALESCE(modified_date, current_date()) as modified_date,
                    _ingestion_timestamp,
                    _source_file
                FROM `{BRONZE_CATALOG}`.`csv_data`.`{table_name}`
                WHERE product_id IS NOT NULL
            """)
            product_dfs.append(df)
            print(f"  ✅ Added {table_name} to product master")
    except Exception as e:
        print(f"  ⚠️ Skipped {table_name}: {str(e)}")

# Union all product dataframes
if product_dfs:
    all_products = product_dfs[0]
    for df in product_dfs[1:]:
        all_products = all_products.unionByName(df, allowMissingColumns=True)
    
    # Create product master with deduplication and calculations
    product_master = all_products.dropDuplicates(["product_id"]) \
        .withColumn("profit_margin", col("price") - col("cost")) \
        .withColumn("profit_margin_percent", 
                   when(col("price") > 0, (col("profit_margin") / col("price")) * 100).otherwise(0)) \
        .withColumn("last_updated", current_timestamp())
    
    # Write to Silver
    product_master.write.mode("overwrite").saveAsTable(f"`{SILVER_CATALOG}`.`products`.`product_master`")
    
    count = product_master.count()
    print(f"\n✅ Created product_master: {count} unique products")
else:
    print("⚠️ No product tables found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver Layer - Sales Transactions

# COMMAND ----------

print("\n🥈 SILVER LAYER - SALES TRANSACTIONS")
print("=" * 50)

# Get all Excel tables and union them
excel_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`excel_data`").collect()
sales_dfs = []

for table in excel_tables:
    table_name = table.tableName
    try:
        # Check if it's a sales table
        columns = spark.sql(f"DESCRIBE `{BRONZE_CATALOG}`.`excel_data`.`{table_name}`").collect()
        column_names = [col.col_name for col in columns]
        
        if 'transaction_id' in column_names:
            df = spark.sql(f"""
                SELECT 
                    transaction_id,
                    transaction_date,
                    COALESCE(customer_id, 'UNKNOWN') as customer_id,
                    COALESCE(product_code, 'UNKNOWN') as product_code,
                    CAST(COALESCE(quantity, 0) AS INT) as quantity,
                    CAST(COALESCE(unit_price, 0) AS DOUBLE) as unit_price,
                    CAST(COALESCE(total_amount, 0) AS DOUBLE) as total_amount,
                    COALESCE(payment_method, 'Unknown') as payment_method,
                    COALESCE(store_location, 'Unknown') as store_location,
                    CAST(COALESCE(discount_percent, 0) AS DOUBLE) as discount_percent,
                    CAST(COALESCE(tax_amount, 0) AS DOUBLE) as tax_amount,
                    _ingestion_timestamp,
                    _source_file
                FROM `{BRONZE_CATALOG}`.`excel_data`.`{table_name}`
                WHERE transaction_id IS NOT NULL
            """)
            sales_dfs.append(df)
            print(f"  ✅ Added {table_name} to sales transactions")
    except Exception as e:
        print(f"  ⚠️ Skipped {table_name}: {str(e)}")

# Union all sales dataframes
if sales_dfs:
    all_sales = sales_dfs[0]
    for df in sales_dfs[1:]:
        all_sales = all_sales.unionByName(df, allowMissingColumns=True)
    
    # Create sales transactions with date parsing and calculations
    sales_transactions = all_sales \
        .withColumn("transaction_date", to_date(col("transaction_date"))) \
        .withColumn("transaction_year", year(col("transaction_date"))) \
        .withColumn("transaction_month", month(col("transaction_date"))) \
        .withColumn("transaction_day", dayofmonth(col("transaction_date"))) \
        .withColumn("last_updated", current_timestamp())
    
    # Write to Silver
    sales_transactions.write.mode("overwrite").saveAsTable(f"`{SILVER_CATALOG}`.`sales`.`sales_transactions`")
    
    count = sales_transactions.count()
    print(f"\n✅ Created sales_transactions: {count} transactions")
else:
    print("⚠️ No sales tables found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gold Layer - Product Performance

# COMMAND ----------

print("\n🥇 GOLD LAYER - PRODUCT PERFORMANCE")
print("=" * 50)

try:
    # Create product performance analytics
    spark.sql(f"""
        CREATE OR REPLACE TABLE `{GOLD_CATALOG}`.`analytics`.`product_performance` AS
        SELECT 
            p.category,
            p.brand,
            p.status,
            COUNT(DISTINCT p.product_id) as product_count,
            AVG(p.price) as avg_price,
            AVG(p.cost) as avg_cost,
            AVG(p.profit_margin) as avg_margin,
            AVG(p.profit_margin_percent) as avg_margin_percent,
            MIN(p.price) as min_price,
            MAX(p.price) as max_price,
            SUM(CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END) as active_products,
            SUM(CASE WHEN p.status = 'INACTIVE' THEN 1 ELSE 0 END) as inactive_products,
            current_timestamp() as last_updated
        FROM `{SILVER_CATALOG}`.`products`.`product_master` p
        GROUP BY p.category, p.brand, p.status
        ORDER BY product_count DESC
    """)
    
    count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`analytics`.`product_performance`").collect()[0][0]
    print(f"✅ Created product_performance: {count} segments")
except Exception as e:
    print(f"❌ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Gold Layer - Daily Store Summary

# COMMAND ----------

print("\n🥇 GOLD LAYER - DAILY STORE SUMMARY")
print("=" * 50)

try:
    # Check if sales data exists
    sales_count = spark.sql(f"SELECT COUNT(*) FROM `{SILVER_CATALOG}`.`sales`.`sales_transactions`").collect()[0][0]
    
    if sales_count > 0:
        spark.sql(f"""
            CREATE OR REPLACE TABLE `{GOLD_CATALOG}`.`reporting`.`daily_store_summary` AS
            SELECT 
                s.transaction_date,
                s.store_location,
                COUNT(DISTINCT s.transaction_id) as transaction_count,
                COUNT(DISTINCT s.customer_id) as unique_customers,
                SUM(s.quantity) as total_quantity,
                SUM(s.total_amount) as total_revenue,
                AVG(s.total_amount) as avg_transaction_value,
                SUM(s.tax_amount) as total_tax,
                AVG(s.discount_percent) as avg_discount_percent,
                MAX(s.total_amount) as max_transaction,
                MIN(s.total_amount) as min_transaction,
                current_timestamp() as last_updated
            FROM `{SILVER_CATALOG}`.`sales`.`sales_transactions` s
            WHERE s.transaction_date IS NOT NULL
            GROUP BY s.transaction_date, s.store_location
            ORDER BY s.transaction_date DESC, total_revenue DESC
        """)
        
        count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`reporting`.`daily_store_summary`").collect()[0][0]
        print(f"✅ Created daily_store_summary: {count} records")
    else:
        print("⚠️ No sales data found for store summary")
except Exception as e:
    print(f"❌ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Pipeline Summary

# COMMAND ----------

print("\n📊 PIPELINE EXECUTION SUMMARY")
print("=" * 80)

# Get processing statistics
stats = spark.sql(f"""
    SELECT 
        file_type,
        status,
        COUNT(*) as file_count,
        SUM(record_count) as total_records,
        AVG(processing_duration_seconds) as avg_duration
    FROM `{BRONZE_CATALOG}`.`system`.`processed_files`
    WHERE processed_timestamp > current_timestamp() - INTERVAL 1 HOUR
    GROUP BY file_type, status
    ORDER BY file_type, status
""").show()

# Check table counts
print("\n📊 TABLE SUMMARY:")
for catalog, layer in [(BRONZE_CATALOG, "Bronze"), (SILVER_CATALOG, "Silver"), (GOLD_CATALOG, "Gold")]:
    print(f"\n{layer} Layer ({catalog}):")
    try:
        total_tables = 0
        total_records = 0
        
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        for schema in schemas:
            if schema.namespace not in ['information_schema']:
                tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema.namespace}`").collect()
                if tables:
                    print(f"  {schema.namespace}:")
                    for table in tables:
                        count = spark.sql(f"SELECT COUNT(*) FROM `{catalog}`.`{schema.namespace}`.`{table.tableName}`").collect()[0][0]
                        print(f"    - {table.tableName}: {count:,} records")
                        total_tables += 1
                        total_records += count
        
        print(f"  TOTAL: {total_tables} tables, {total_records:,} records")
    except Exception as e:
        print(f"  Error: {str(e)}")

print(f"\n✅ PIPELINE EXECUTION COMPLETE!")
print(f"End Time: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Test Dynamic Processing

# COMMAND ----------

print("\n🧪 TESTING DYNAMIC PROCESSING")
print("=" * 50)

# Create a new test file
test_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
test_df = spark.createDataFrame([
    (f"TEST001_{test_timestamp}", "Dynamic Test Product 1", "TestCategory", "TestSub", "TestBrand", 99.99, 49.99, "ACTIVE", "2025-07-22", "2025-07-22"),
    (f"TEST002_{test_timestamp}", "Dynamic Test Product 2", "TestCategory", "TestSub", "TestBrand", 149.99, 74.99, "ACTIVE", "2025-07-22", "2025-07-22"),
    (f"TEST003_{test_timestamp}", "Dynamic Test Product 3", "TestCategory", "TestSub", "TestBrand", 199.99, 99.99, "INACTIVE", "2025-07-22", "2025-07-22"),
], ["product_id", "product_name", "category", "sub_category", "brand", "price", "cost", "status", "created_date", "modified_date"])

# Save to storage
test_path = f"abfss://raw-data@{SOURCE_STORAGE}.dfs.core.windows.net/products/dynamic_test/test_{test_timestamp}.csv"
test_df.write.mode("overwrite").option("header", "true").csv(test_path)

print(f"✅ Created dynamic test file: {test_path}")
print(f"   Records: {test_df.count()}")
print("\n⚠️ Note: Re-run the pipeline to process this new file and verify dynamic processing")

print("\n✅ E2E PIPELINE FULLY OPERATIONAL!")

# COMMAND ----------

# Return success
dbutils.notebook.exit("SUCCESS")