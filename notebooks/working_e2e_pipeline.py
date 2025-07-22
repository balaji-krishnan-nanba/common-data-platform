# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Working E2E Pipeline - Direct Implementation
# MAGIC 
# MAGIC This notebook implements the complete E2E pipeline without requiring package installation.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime
import os

print("🚀 STARTING E2E PIPELINE EXECUTION")
print("=" * 80)
print(f"Execution Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# Configuration
BRONZE_CATALOG = "cddp-dev-bronze"
SILVER_CATALOG = "cddp-dev-silver"
GOLD_CATALOG = "cddp-dev-gold"

SOURCE_STORAGE = "agentstge"
CONTAINER = "raw-data"

# Create schemas if not exist
for catalog in [BRONZE_CATALOG, SILVER_CATALOG, GOLD_CATALOG]:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BRONZE_CATALOG}`.`csv_data`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BRONZE_CATALOG}`.`excel_data`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BRONZE_CATALOG}`.`system`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SILVER_CATALOG}`.`products`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SILVER_CATALOG}`.`sales`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{GOLD_CATALOG}`.`analytics`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{GOLD_CATALOG}`.`reporting`")

print("✅ Catalogs and schemas created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. File Processing Tracking

# COMMAND ----------

# Create processed files tracking table
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

print("✅ File tracking table created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Layer - CSV Ingestion

# COMMAND ----------

print("\n🥉 BRONZE LAYER - CSV INGESTION")
print("=" * 50)

# Function to process CSV files
def process_csv_files():
    csv_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/products/"
    
    try:
        # List all CSV files recursively
        csv_files = []
        
        def list_files_recursive(path):
            items = dbutils.fs.ls(path)
            for item in items:
                if item.path.endswith('.csv'):
                    csv_files.append(item.path)
                elif item.isDir():
                    list_files_recursive(item.path)
        
        list_files_recursive(csv_path)
        
        print(f"Found {len(csv_files)} CSV files to process")
        
        for csv_file in csv_files:
            start_time = datetime.now()
            try:
                # Extract table name from file path
                parts = csv_file.split('/')
                category = parts[-2]  # Get the folder name
                file_name = parts[-1].replace('.csv', '')
                table_name = f"{category}_{file_name}".replace('-', '_').replace(' ', '_').lower()
                
                print(f"\nProcessing: {csv_file}")
                
                # Read CSV file
                df = spark.read.option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(csv_file)
                
                # Add metadata columns
                df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                    .withColumn("_source_file", lit(csv_file)) \
                    .withColumn("_file_modification_time", current_timestamp())
                
                record_count = df.count()
                
                # Write to Bronze layer
                df.write.mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .saveAsTable(f"`{BRONZE_CATALOG}`.`csv_data`.`{table_name}`")
                
                # Log success
                processing_time = (datetime.now() - start_time).total_seconds()
                
                spark.sql(f"""
                    INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
                    VALUES (
                        '{csv_file}',
                        '{file_name}.csv',
                        'csv',
                        0,
                        {record_count},
                        'SUCCESS',
                        NULL,
                        current_timestamp(),
                        {processing_time}
                    )
                """)
                
                print(f"✅ Processed {table_name}: {record_count} records in {processing_time:.2f}s")
                
            except Exception as e:
                print(f"❌ Error processing {csv_file}: {str(e)}")
                # Log error
                spark.sql(f"""
                    INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
                    VALUES (
                        '{csv_file}',
                        '{csv_file.split('/')[-1]}',
                        'csv',
                        0,
                        0,
                        'FAILED',
                        '{str(e).replace("'", "''")}',
                        current_timestamp(),
                        0
                    )
                """)
                
    except Exception as e:
        print(f"❌ Error listing CSV files: {str(e)}")
        
# Process CSV files
process_csv_files()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze Layer - Excel Ingestion

# COMMAND ----------

print("\n🥉 BRONZE LAYER - EXCEL INGESTION")
print("=" * 50)

# Function to process Excel files
def process_excel_files():
    excel_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/sales/"
    
    try:
        # List all Excel files recursively
        excel_files = []
        
        def list_files_recursive(path):
            try:
                items = dbutils.fs.ls(path)
                for item in items:
                    if item.path.endswith('.xlsx') or item.path.endswith('.xls'):
                        excel_files.append(item.path)
                    elif item.isDir():
                        list_files_recursive(item.path)
            except:
                pass  # Ignore directories that can't be listed
        
        list_files_recursive(excel_path)
        
        print(f"Found {len(excel_files)} Excel files to process")
        
        for excel_file in excel_files:
            start_time = datetime.now()
            try:
                # Extract table name from file path
                parts = excel_file.split('/')
                file_name = parts[-1].replace('.xlsx', '').replace('.xls', '')
                
                # Determine table name based on path
                if 'monthly' in excel_file:
                    table_name = f"monthly_sales_{file_name}".replace('-', '_').replace(' ', '_').lower()
                elif 'daily' in excel_file:
                    table_name = f"daily_sales_{file_name}".replace('-', '_').replace(' ', '_').lower()
                elif 'promo' in excel_file:
                    table_name = f"promo_sales_{file_name}".replace('-', '_').replace(' ', '_').lower()
                else:
                    table_name = f"sales_{file_name}".replace('-', '_').replace(' ', '_').lower()
                
                print(f"\nProcessing: {excel_file}")
                
                # Read Excel file
                df = spark.read.format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("dataAddress", "'Sales Data'!A1") \
                    .load(excel_file)
                
                # Add metadata columns
                df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                    .withColumn("_source_file", lit(excel_file)) \
                    .withColumn("_file_modification_time", current_timestamp())
                
                record_count = df.count()
                
                # Write to Bronze layer
                df.write.mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .saveAsTable(f"`{BRONZE_CATALOG}`.`excel_data`.`{table_name}`")
                
                # Log success
                processing_time = (datetime.now() - start_time).total_seconds()
                
                spark.sql(f"""
                    INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
                    VALUES (
                        '{excel_file}',
                        '{file_name}.xlsx',
                        'excel',
                        0,
                        {record_count},
                        'SUCCESS',
                        NULL,
                        current_timestamp(),
                        {processing_time}
                    )
                """)
                
                print(f"✅ Processed {table_name}: {record_count} records in {processing_time:.2f}s")
                
            except Exception as e:
                print(f"❌ Error processing {excel_file}: {str(e)}")
                # Log error
                spark.sql(f"""
                    INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
                    VALUES (
                        '{excel_file}',
                        '{excel_file.split('/')[-1]}',
                        'excel',
                        0,
                        0,
                        'FAILED',
                        '{str(e).replace("'", "''")}',
                        current_timestamp(),
                        0
                    )
                """)
                
    except Exception as e:
        print(f"❌ Error listing Excel files: {str(e)}")
        
# Process Excel files
process_excel_files()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver Layer - Product Master

# COMMAND ----------

print("\n🥈 SILVER LAYER - PRODUCT MASTER")
print("=" * 50)

# Create product master from all CSV tables
try:
    # Get all CSV tables
    csv_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`csv_data`").collect()
    
    if csv_tables:
        # Union all product tables
        product_dfs = []
        
        for table in csv_tables:
            table_name = table.tableName
            if 'product' in table_name or 'catalog' in table_name:
                df = spark.sql(f"""
                    SELECT 
                        product_id,
                        product_name,
                        category,
                        sub_category,
                        brand,
                        CAST(price AS DOUBLE) as price,
                        CAST(cost AS DOUBLE) as cost,
                        status,
                        created_date,
                        modified_date,
                        _ingestion_timestamp,
                        _source_file
                    FROM `{BRONZE_CATALOG}`.`csv_data`.`{table_name}`
                    WHERE product_id IS NOT NULL
                """)
                product_dfs.append(df)
        
        if product_dfs:
            # Union all dataframes
            all_products = product_dfs[0]
            for df in product_dfs[1:]:
                all_products = all_products.unionByName(df, allowMissingColumns=True)
            
            # Deduplicate and clean
            product_master = all_products \
                .dropDuplicates(["product_id"]) \
                .withColumn("profit_margin", col("price") - col("cost")) \
                .withColumn("profit_margin_percent", 
                           when(col("price") > 0, (col("profit_margin") / col("price")) * 100).otherwise(0)) \
                .withColumn("last_updated", current_timestamp())
            
            # Write to Silver
            product_master.write.mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(f"`{SILVER_CATALOG}`.`products`.`product_master`")
            
            count = product_master.count()
            print(f"✅ Created product_master: {count} unique products")
        else:
            print("⚠️ No product tables found")
    else:
        print("⚠️ No CSV tables found")
        
except Exception as e:
    print(f"❌ Error creating product master: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver Layer - Sales Transactions

# COMMAND ----------

print("\n🥈 SILVER LAYER - SALES TRANSACTIONS")
print("=" * 50)

# Create sales transactions from all Excel tables
try:
    # Get all Excel tables
    excel_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`excel_data`").collect()
    
    if excel_tables:
        # Union all sales tables
        sales_dfs = []
        
        for table in excel_tables:
            table_name = table.tableName
            df = spark.sql(f"""
                SELECT 
                    transaction_id,
                    transaction_date,
                    customer_id,
                    product_code,
                    CAST(quantity AS INT) as quantity,
                    CAST(unit_price AS DOUBLE) as unit_price,
                    CAST(total_amount AS DOUBLE) as total_amount,
                    payment_method,
                    store_location,
                    CAST(COALESCE(discount_percent, 0) AS DOUBLE) as discount_percent,
                    CAST(COALESCE(tax_amount, 0) AS DOUBLE) as tax_amount,
                    _ingestion_timestamp,
                    _source_file
                FROM `{BRONZE_CATALOG}`.`excel_data`.`{table_name}`
                WHERE transaction_id IS NOT NULL
            """)
            sales_dfs.append(df)
        
        if sales_dfs:
            # Union all dataframes
            all_sales = sales_dfs[0]
            for df in sales_dfs[1:]:
                all_sales = all_sales.unionByName(df, allowMissingColumns=True)
            
            # Clean and enhance
            sales_transactions = all_sales \
                .withColumn("transaction_date", to_date(col("transaction_date"))) \
                .withColumn("transaction_year", year(col("transaction_date"))) \
                .withColumn("transaction_month", month(col("transaction_date"))) \
                .withColumn("transaction_day", dayofmonth(col("transaction_date"))) \
                .withColumn("last_updated", current_timestamp())
            
            # Write to Silver
            sales_transactions.write.mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(f"`{SILVER_CATALOG}`.`sales`.`sales_transactions`")
            
            count = sales_transactions.count()
            print(f"✅ Created sales_transactions: {count} transactions")
        else:
            print("⚠️ No sales tables found")
    else:
        print("⚠️ No Excel tables found")
        
except Exception as e:
    print(f"❌ Error creating sales transactions: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold Layer - Product Performance

# COMMAND ----------

print("\n🥇 GOLD LAYER - PRODUCT PERFORMANCE")
print("=" * 50)

# Create product performance analytics
try:
    product_performance = spark.sql(f"""
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
            current_timestamp() as last_updated
        FROM `{SILVER_CATALOG}`.`products`.`product_master` p
        GROUP BY p.category, p.brand, p.status
    """)
    
    product_performance.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"`{GOLD_CATALOG}`.`analytics`.`product_performance`")
    
    count = product_performance.count()
    print(f"✅ Created product_performance: {count} segments")
    
except Exception as e:
    print(f"❌ Error creating product performance: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gold Layer - Store Summary

# COMMAND ----------

print("\n🥇 GOLD LAYER - STORE SUMMARY")
print("=" * 50)

# Create daily store summary
try:
    # Check if sales transactions exist
    sales_count = spark.sql(f"SELECT COUNT(*) FROM `{SILVER_CATALOG}`.`sales`.`sales_transactions`").collect()[0][0]
    
    if sales_count > 0:
        store_summary = spark.sql(f"""
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
                current_timestamp() as last_updated
            FROM `{SILVER_CATALOG}`.`sales`.`sales_transactions` s
            GROUP BY s.transaction_date, s.store_location
        """)
        
        store_summary.write.mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"`{GOLD_CATALOG}`.`reporting`.`daily_store_summary`")
        
        count = store_summary.count()
        print(f"✅ Created daily_store_summary: {count} records")
    else:
        print("⚠️ No sales data found for store summary")
        
except Exception as e:
    print(f"❌ Error creating store summary: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Pipeline Summary

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
""").collect()

print("\nFile Processing Summary:")
for stat in stats:
    print(f"  {stat.file_type} - {stat.status}: {stat.file_count} files, {stat.total_records:,} records, {stat.avg_duration:.2f}s avg")

# Check table counts
for catalog in [BRONZE_CATALOG, SILVER_CATALOG, GOLD_CATALOG]:
    try:
        table_count = 0
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        for schema in schemas:
            if schema.namespace not in ['information_schema']:
                tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema.namespace}`").collect()
                table_count += len(tables)
        print(f"\n{catalog}: {table_count} tables created")
    except:
        pass

print("\n✅ PIPELINE EXECUTION COMPLETE!")
print(f"Completion Time: {datetime.now()}")

# Return success
dbutils.notebook.exit("SUCCESS")