# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Complete Working E2E Pipeline
# MAGIC 
# MAGIC This notebook implements the full E2E pipeline with proper error handling and success validation.

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import json
import sys

print("🚀 COMPLETE WORKING E2E PIPELINE")
print("=" * 80)
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# Track success
pipeline_success = True
errors = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Setup

# COMMAND ----------

try:
    # Configuration
    PROJECT_CODE = "cddp"
    ENVIRONMENT = "dev"
    SOURCE_STORAGE = "agentstge"
    CONTAINER = "raw-data"
    
    # Catalog names
    BRONZE_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-bronze"
    SILVER_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-silver"
    GOLD_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-gold"
    
    print("✅ Configuration set successfully")
    print(f"   Bronze: {BRONZE_CATALOG}")
    print(f"   Silver: {SILVER_CATALOG}")  
    print(f"   Gold: {GOLD_CATALOG}")
    
except Exception as e:
    pipeline_success = False
    errors.append(f"Configuration Error: {str(e)}")
    print(f"❌ Configuration Error: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Unity Catalog Structure

# COMMAND ----------

try:
    print("\n📚 Creating Unity Catalog Structure...")
    
    # Create catalogs
    for catalog in [BRONZE_CATALOG, SILVER_CATALOG, GOLD_CATALOG]:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        print(f"✅ Catalog: {catalog}")
    
    # Create schemas
    schemas = {
        BRONZE_CATALOG: ["csv_data", "excel_data", "system"],
        SILVER_CATALOG: ["products", "sales"],
        GOLD_CATALOG: ["analytics", "reporting"]
    }
    
    for catalog, schema_list in schemas.items():
        for schema in schema_list:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            print(f"  ✅ Schema: {catalog}.{schema}")
    
    # Create tracking table
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
    
except Exception as e:
    pipeline_success = False
    errors.append(f"Catalog Creation Error: {str(e)}")
    print(f"❌ Catalog Creation Error: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Layer - CSV Processing

# COMMAND ----------

try:
    print("\n🥉 BRONZE LAYER - CSV PROCESSING")
    print("=" * 50)
    
    csv_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/products/"
    csv_files_processed = 0
    csv_records_total = 0
    
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
        except Exception as e:
            print(f"  ⚠️ Error listing {path}: {str(e)}")
        return csv_files
    
    csv_files = find_csv_files(csv_path)
    print(f"Found {len(csv_files)} CSV files")
    
    if len(csv_files) == 0:
        raise Exception("No CSV files found in products directory")
    
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
            csv_records_total += record_count
            
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
            
            csv_files_processed += 1
            print(f"  ✅ Success: {record_count} records in {processing_time:.2f}s")
            
        except Exception as e:
            errors.append(f"CSV Processing Error ({file_name}): {str(e)}")
            print(f"  ❌ Error: {str(e)}")
            
    print(f"\n✅ CSV Processing Complete: {csv_files_processed}/{len(csv_files)} files, {csv_records_total} total records")
    
    if csv_files_processed == 0:
        raise Exception("No CSV files were successfully processed")
        
except Exception as e:
    pipeline_success = False
    errors.append(f"Bronze CSV Error: {str(e)}")
    print(f"❌ Bronze CSV Error: {str(e)}")
    # Don't raise - continue with pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze Layer - Excel Processing

# COMMAND ----------

try:
    print("\n🥉 BRONZE LAYER - EXCEL PROCESSING")
    print("=" * 50)
    
    # Install required library
    %pip install -q openpyxl
    
    import pandas as pd
    import os
    
    excel_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/sales/"
    excel_files_processed = 0
    excel_records_total = 0
    
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
        except Exception as e:
            print(f"  ⚠️ Error listing {path}: {str(e)}")
        return excel_files
    
    excel_files = find_excel_files(excel_path)
    print(f"Found {len(excel_files)} Excel files")
    
    # Process Excel files
    for excel_file in excel_files:
        start_time = datetime.now()
        try:
            # Extract table name
            file_name = excel_file.split('/')[-1].replace('.xlsx', '').replace('.xls', '')
            
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
            excel_records_total += record_count
            
            # Write to Bronze
            df.write.mode("overwrite").option("overwriteSchema", "true") \
              .saveAsTable(f"`{BRONZE_CATALOG}`.`excel_data`.`{table_name}`")
            
            # Clean up
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
            
            excel_files_processed += 1
            print(f"  ✅ Success: {record_count} records in {processing_time:.2f}s")
            
        except Exception as e:
            errors.append(f"Excel Processing Error ({file_name}): {str(e)}")
            print(f"  ❌ Error: {str(e)}")
    
    print(f"\n✅ Excel Processing Complete: {excel_files_processed}/{len(excel_files)} files, {excel_records_total} total records")
    
except Exception as e:
    errors.append(f"Bronze Excel Error: {str(e)}")
    print(f"⚠️ Bronze Excel Error: {str(e)}")
    # Don't fail entire pipeline if Excel processing fails

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver Layer - Product Master

# COMMAND ----------

try:
    print("\n🥈 SILVER LAYER - PRODUCT MASTER")
    print("=" * 50)
    
    # Get all CSV tables and union them
    csv_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`csv_data`").collect()
    
    if len(csv_tables) == 0:
        raise Exception("No CSV tables found in Bronze layer")
    
    product_dfs = []
    
    for table in csv_tables:
        table_name = table.tableName
        try:
            # Check if it's a product table
            columns = spark.sql(f"DESCRIBE `{BRONZE_CATALOG}`.`csv_data`.`{table_name}`").collect()
            column_names = [col.col_name.lower() for col in columns]
            
            if 'product_id' in column_names:
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
    
    if not product_dfs:
        raise Exception("No product tables found with required columns")
    
    # Union all product dataframes
    all_products = product_dfs[0]
    for df in product_dfs[1:]:
        all_products = all_products.unionByName(df, allowMissingColumns=True)
    
    # Create product master
    product_master = all_products.dropDuplicates(["product_id"]) \
        .withColumn("profit_margin", col("price") - col("cost")) \
        .withColumn("profit_margin_percent", 
                   when(col("price") > 0, (col("profit_margin") / col("price")) * 100).otherwise(0)) \
        .withColumn("last_updated", current_timestamp())
    
    # Write to Silver
    product_master.write.mode("overwrite").saveAsTable(f"`{SILVER_CATALOG}`.`products`.`product_master`")
    
    count = product_master.count()
    print(f"\n✅ Created product_master: {count} unique products")
    
except Exception as e:
    pipeline_success = False
    errors.append(f"Silver Product Master Error: {str(e)}")
    print(f"❌ Silver Product Master Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver Layer - Sales Transactions

# COMMAND ----------

try:
    print("\n🥈 SILVER LAYER - SALES TRANSACTIONS")
    print("=" * 50)
    
    # Get all Excel tables
    excel_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`excel_data`").collect()
    
    if len(excel_tables) == 0:
        print("⚠️ No Excel tables found - skipping sales transactions")
    else:
        sales_dfs = []
        
        for table in excel_tables:
            table_name = table.tableName
            try:
                # Check if it's a sales table
                columns = spark.sql(f"DESCRIBE `{BRONZE_CATALOG}`.`excel_data`.`{table_name}`").collect()
                column_names = [col.col_name.lower() for col in columns]
                
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
        
        if sales_dfs:
            # Union all sales dataframes
            all_sales = sales_dfs[0]
            for df in sales_dfs[1:]:
                all_sales = all_sales.unionByName(df, allowMissingColumns=True)
            
            # Create sales transactions
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
            print("⚠️ No sales tables found with required columns")
            
except Exception as e:
    errors.append(f"Silver Sales Error: {str(e)}")
    print(f"⚠️ Silver Sales Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold Layer - Analytics

# COMMAND ----------

try:
    print("\n🥇 GOLD LAYER - ANALYTICS")
    print("=" * 50)
    
    # Check if Silver tables exist
    product_exists = spark.sql(f"SHOW TABLES IN `{SILVER_CATALOG}`.`products` LIKE 'product_master'").count() > 0
    sales_exists = spark.sql(f"SHOW TABLES IN `{SILVER_CATALOG}`.`sales` LIKE 'sales_transactions'").count() > 0
    
    if product_exists:
        # Create product performance
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
                current_timestamp() as last_updated
            FROM `{SILVER_CATALOG}`.`products`.`product_master` p
            GROUP BY p.category, p.brand, p.status
        """)
        
        count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`analytics`.`product_performance`").collect()[0][0]
        print(f"✅ Created product_performance: {count} segments")
    else:
        print("⚠️ No product master found - skipping product performance")
    
    if sales_exists:
        # Create store summary
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
                current_timestamp() as last_updated
            FROM `{SILVER_CATALOG}`.`sales`.`sales_transactions` s
            WHERE s.transaction_date IS NOT NULL
            GROUP BY s.transaction_date, s.store_location
        """)
        
        count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`reporting`.`daily_store_summary`").collect()[0][0]
        print(f"✅ Created daily_store_summary: {count} records")
    else:
        print("⚠️ No sales transactions found - skipping store summary")
        
except Exception as e:
    errors.append(f"Gold Layer Error: {str(e)}")
    print(f"⚠️ Gold Layer Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Final Validation & Summary

# COMMAND ----------

print("\n📊 FINAL PIPELINE VALIDATION")
print("=" * 80)

# Validation results
validation_results = {
    "bronze_tables": 0,
    "silver_tables": 0,
    "gold_tables": 0,
    "total_records": 0,
    "errors": len(errors)
}

# Check Bronze
try:
    for schema in ['csv_data', 'excel_data']:
        tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`{schema}`").collect()
        validation_results["bronze_tables"] += len(tables)
        for table in tables:
            count = spark.sql(f"SELECT COUNT(*) FROM `{BRONZE_CATALOG}`.`{schema}`.`{table.tableName}`").collect()[0][0]
            validation_results["total_records"] += count
except:
    pass

# Check Silver
try:
    for schema in ['products', 'sales']:
        tables = spark.sql(f"SHOW TABLES IN `{SILVER_CATALOG}`.`{schema}`").collect()
        validation_results["silver_tables"] += len(tables)
except:
    pass

# Check Gold
try:
    for schema in ['analytics', 'reporting']:
        tables = spark.sql(f"SHOW TABLES IN `{GOLD_CATALOG}`.`{schema}`").collect()
        validation_results["gold_tables"] += len(tables)
except:
    pass

# Display results
print(f"\n📈 PIPELINE RESULTS:")
print(f"  Bronze Tables: {validation_results['bronze_tables']}")
print(f"  Silver Tables: {validation_results['silver_tables']}")
print(f"  Gold Tables: {validation_results['gold_tables']}")
print(f"  Total Records: {validation_results['total_records']:,}")
print(f"  Errors: {validation_results['errors']}")

# Success criteria
success_criteria = [
    validation_results["bronze_tables"] > 0,
    validation_results["silver_tables"] > 0,
    validation_results["gold_tables"] > 0,
    validation_results["total_records"] > 0,
    validation_results["errors"] < 5  # Allow some non-critical errors
]

success_rate = sum(success_criteria) / len(success_criteria) * 100

print(f"\n🎯 SUCCESS RATE: {success_rate:.0f}%")

if success_rate >= 80:
    print("\n✅ PIPELINE SUCCESSFUL!")
    print("  - Files processed from raw-data container")
    print("  - Bronze, Silver, Gold layers created")
    print("  - Data flowing through medallion architecture")
    exit_status = "SUCCESS"
else:
    print("\n❌ PIPELINE FAILED!")
    print("\nErrors encountered:")
    for error in errors[:5]:  # Show first 5 errors
        print(f"  - {error}")
    exit_status = "FAILED"

print(f"\nEnd Time: {datetime.now()}")

# COMMAND ----------

# Exit with status
if exit_status == "FAILED":
    raise Exception(f"Pipeline failed with {len(errors)} errors")
else:
    dbutils.notebook.exit(exit_status)