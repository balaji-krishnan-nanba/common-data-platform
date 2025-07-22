# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Cell-Level Monitoring Pipeline
# MAGIC 
# MAGIC This notebook implements cell-level monitoring to capture individual cell outputs and errors,
# MAGIC addressing the fundamental issue where Databricks shows notebooks as "Succeeded" even with cell failures.

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import json
import sys
import traceback

print("🔍 CELL-LEVEL MONITORING PIPELINE")
print("=" * 80)
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# Initialize monitoring
cell_results = []
current_cell = 1

def monitor_cell(cell_name, cell_func, critical=True):
    """Execute a cell and capture its output/errors"""
    global current_cell
    cell_id = f"CELL_{current_cell:03d}"
    current_cell += 1
    
    result = {
        "cell_id": cell_id,
        "cell_name": cell_name,
        "status": "PENDING",
        "output": "",
        "error": None,
        "start_time": datetime.now().isoformat(),
        "end_time": None,
        "duration_seconds": 0,
        "critical": critical
    }
    
    print(f"\n{'='*60}")
    print(f"📍 {cell_id}: {cell_name}")
    print(f"{'='*60}")
    
    try:
        # Capture output
        output = cell_func()
        result["output"] = str(output) if output else "Success"
        result["status"] = "SUCCESS"
        print(f"✅ {cell_id} completed successfully")
        
    except Exception as e:
        result["error"] = str(e)
        result["status"] = "FAILED"
        result["traceback"] = traceback.format_exc()
        print(f"❌ {cell_id} failed: {str(e)}")
        
        if critical:
            print("🛑 Critical cell failed - stopping pipeline")
            raise
    
    finally:
        result["end_time"] = datetime.now().isoformat()
        start = datetime.fromisoformat(result["start_time"])
        end = datetime.fromisoformat(result["end_time"])
        result["duration_seconds"] = (end - start).total_seconds()
        cell_results.append(result)
    
    return result

# COMMAND ----------

# Cell 1: Configuration
def setup_config():
    global PROJECT_CODE, ENVIRONMENT, SOURCE_STORAGE, CONTAINER
    global BRONZE_CATALOG, SILVER_CATALOG, GOLD_CATALOG
    
    PROJECT_CODE = "cddp"
    ENVIRONMENT = "dev"
    SOURCE_STORAGE = "agentstge"
    CONTAINER = "raw-data"
    
    BRONZE_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-bronze"
    SILVER_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-silver"
    GOLD_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-gold"
    
    return f"Configuration set: {BRONZE_CATALOG}, {SILVER_CATALOG}, {GOLD_CATALOG}"

monitor_cell("Configuration Setup", setup_config)

# COMMAND ----------

# Cell 2: Create Catalog Structure
def create_catalogs():
    created = []
    errors = []
    
    # Create catalogs
    for catalog in [BRONZE_CATALOG, SILVER_CATALOG, GOLD_CATALOG]:
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
            created.append(catalog)
        except Exception as e:
            errors.append(f"{catalog}: {str(e)}")
    
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
                created.append(f"{catalog}.{schema}")
            except Exception as e:
                errors.append(f"{catalog}.{schema}: {str(e)}")
    
    result = f"Created: {len(created)} items"
    if errors:
        result += f"\nErrors: {len(errors)} - {errors[:3]}"
    
    return result

monitor_cell("Create Unity Catalog Structure", create_catalogs)

# COMMAND ----------

# Cell 3: Create Monitoring Tables
def create_monitoring_tables():
    # Create cell execution tracking table
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{BRONZE_CATALOG}`.`system`.`cell_executions` (
        notebook_run_id STRING,
        cell_id STRING,
        cell_name STRING,
        status STRING,
        output STRING,
        error STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_seconds DOUBLE,
        critical BOOLEAN,
        execution_date DATE
    ) USING DELTA
    PARTITIONED BY (execution_date)
    """)
    
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
    
    return "Monitoring tables created"

monitor_cell("Create Monitoring Tables", create_monitoring_tables)

# COMMAND ----------

# Cell 4: Process CSV Files
def process_csv_files():
    csv_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/products/"
    processed = 0
    failed = 0
    total_records = 0
    
    # Find CSV files
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
            print(f"Error listing {path}: {e}")
        return csv_files
    
    csv_files = find_csv_files(csv_path)
    
    for csv_file in csv_files:
        try:
            # Extract table name
            file_name = csv_file.split('/')[-1].replace('.csv', '')
            folder_name = csv_file.split('/')[-2]
            table_name = f"{folder_name}_{file_name}".replace('-', '_').replace(' ', '_').lower()
            
            # Read and process
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file)
            df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                   .withColumn("_source_file", lit(csv_file))
            
            record_count = df.count()
            total_records += record_count
            
            # Write to Bronze
            df.write.mode("overwrite").option("overwriteSchema", "true") \
              .saveAsTable(f"`{BRONZE_CATALOG}`.`csv_data`.`{table_name}`")
            
            processed += 1
            
        except Exception as e:
            failed += 1
            print(f"Failed processing {file_name}: {str(e)}")
    
    return f"CSV Processing: {processed} succeeded, {failed} failed, {total_records} total records"

monitor_cell("Bronze Layer - CSV Processing", process_csv_files)

# COMMAND ----------

# Cell 5: Process Excel Files
def process_excel_files():
    # Install openpyxl
    import subprocess
    subprocess.check_call(["pip", "install", "-q", "openpyxl"])
    
    import pandas as pd
    import os
    
    excel_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/sales/"
    processed = 0
    failed = 0
    total_records = 0
    
    # Find Excel files
    def find_excel_files(path):
        excel_files = []
        try:
            items = dbutils.fs.ls(path)
            for item in items:
                if item.path.endswith(('.xlsx', '.xls')):
                    excel_files.append(item.path)
                elif item.isDir():
                    excel_files.extend(find_excel_files(item.path))
        except Exception as e:
            print(f"Error listing {path}: {e}")
        return excel_files
    
    excel_files = find_excel_files(excel_path)
    
    for excel_file in excel_files:
        try:
            # Extract table name
            file_name = excel_file.split('/')[-1].replace('.xlsx', '').replace('.xls', '')
            
            if 'monthly' in excel_file:
                table_name = f"monthly_sales_{file_name}"
            elif 'daily' in excel_file:
                table_name = f"daily_sales_{file_name}"
            else:
                table_name = f"sales_{file_name}"
            
            table_name = table_name.replace('-', '_').replace(' ', '_').lower()
            
            # Copy and read
            local_file = f"/tmp/{file_name}.xlsx"
            dbutils.fs.cp(excel_file, f"file:{local_file}")
            
            df_pandas = pd.read_excel(local_file)
            df = spark.createDataFrame(df_pandas)
            
            df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                   .withColumn("_source_file", lit(excel_file))
            
            record_count = df.count()
            total_records += record_count
            
            # Write to Bronze
            df.write.mode("overwrite").option("overwriteSchema", "true") \
              .saveAsTable(f"`{BRONZE_CATALOG}`.`excel_data`.`{table_name}`")
            
            os.remove(local_file)
            processed += 1
            
        except Exception as e:
            failed += 1
            print(f"Failed processing {file_name}: {str(e)}")
    
    return f"Excel Processing: {processed} succeeded, {failed} failed, {total_records} total records"

monitor_cell("Bronze Layer - Excel Processing", process_excel_files, critical=False)

# COMMAND ----------

# Cell 6: Create Silver Product Master
def create_product_master():
    # Get product tables
    csv_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`csv_data`").collect()
    product_dfs = []
    
    for table in csv_tables:
        table_name = table.tableName
        try:
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
                        _ingestion_timestamp,
                        _source_file
                    FROM `{BRONZE_CATALOG}`.`csv_data`.`{table_name}`
                    WHERE product_id IS NOT NULL
                """)
                product_dfs.append(df)
        except:
            pass
    
    if product_dfs:
        # Union and deduplicate
        all_products = product_dfs[0]
        for df in product_dfs[1:]:
            all_products = all_products.unionByName(df, allowMissingColumns=True)
        
        product_master = all_products.dropDuplicates(["product_id"]) \
            .withColumn("profit_margin", col("price") - col("cost")) \
            .withColumn("profit_margin_percent", 
                       when(col("price") > 0, (col("profit_margin") / col("price")) * 100).otherwise(0))
        
        product_master.write.mode("overwrite").saveAsTable(f"`{SILVER_CATALOG}`.`products`.`product_master`")
        
        count = product_master.count()
        return f"Created product_master with {count} unique products"
    else:
        return "No product tables found"

monitor_cell("Silver Layer - Product Master", create_product_master)

# COMMAND ----------

# Cell 7: Create Silver Sales Transactions
def create_sales_transactions():
    # Get sales tables
    excel_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`excel_data`").collect()
    sales_dfs = []
    
    for table in excel_tables:
        table_name = table.tableName
        try:
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
                        _ingestion_timestamp,
                        _source_file
                    FROM `{BRONZE_CATALOG}`.`excel_data`.`{table_name}`
                    WHERE transaction_id IS NOT NULL
                """)
                sales_dfs.append(df)
        except:
            pass
    
    if sales_dfs:
        # Union all
        all_sales = sales_dfs[0]
        for df in sales_dfs[1:]:
            all_sales = all_sales.unionByName(df, allowMissingColumns=True)
        
        sales_transactions = all_sales \
            .withColumn("transaction_date", to_date(col("transaction_date"))) \
            .withColumn("transaction_year", year(col("transaction_date"))) \
            .withColumn("transaction_month", month(col("transaction_date")))
        
        sales_transactions.write.mode("overwrite").saveAsTable(f"`{SILVER_CATALOG}`.`sales`.`sales_transactions`")
        
        count = sales_transactions.count()
        return f"Created sales_transactions with {count} transactions"
    else:
        return "No sales tables found"

monitor_cell("Silver Layer - Sales Transactions", create_sales_transactions, critical=False)

# COMMAND ----------

# Cell 8: Create Gold Analytics
def create_gold_analytics():
    results = []
    
    # Product Performance
    try:
        spark.sql(f"""
            CREATE OR REPLACE TABLE `{GOLD_CATALOG}`.`analytics`.`product_performance` AS
            SELECT 
                p.category,
                p.brand,
                p.status,
                COUNT(DISTINCT p.product_id) as product_count,
                AVG(p.price) as avg_price,
                AVG(p.profit_margin_percent) as avg_margin_percent,
                current_timestamp() as last_updated
            FROM `{SILVER_CATALOG}`.`products`.`product_master` p
            GROUP BY p.category, p.brand, p.status
        """)
        count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`analytics`.`product_performance`").collect()[0][0]
        results.append(f"product_performance: {count} segments")
    except Exception as e:
        results.append(f"product_performance failed: {str(e)}")
    
    # Store Summary
    try:
        if spark.sql(f"SHOW TABLES IN `{SILVER_CATALOG}`.`sales` LIKE 'sales_transactions'").count() > 0:
            spark.sql(f"""
                CREATE OR REPLACE TABLE `{GOLD_CATALOG}`.`reporting`.`daily_store_summary` AS
                SELECT 
                    s.transaction_date,
                    s.store_location,
                    COUNT(DISTINCT s.transaction_id) as transaction_count,
                    SUM(s.total_amount) as total_revenue,
                    current_timestamp() as last_updated
                FROM `{SILVER_CATALOG}`.`sales`.`sales_transactions` s
                WHERE s.transaction_date IS NOT NULL
                GROUP BY s.transaction_date, s.store_location
            """)
            count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`reporting`.`daily_store_summary`").collect()[0][0]
            results.append(f"daily_store_summary: {count} records")
    except Exception as e:
        results.append(f"daily_store_summary failed: {str(e)}")
    
    return "; ".join(results)

monitor_cell("Gold Layer - Analytics", create_gold_analytics, critical=False)

# COMMAND ----------

# Cell 9: Save Cell Results to Table
def save_cell_results():
    run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Convert results to DataFrame
    results_df = spark.createDataFrame(cell_results)
    results_df = results_df.withColumn("notebook_run_id", lit(run_id)) \
                          .withColumn("execution_date", current_date())
    
    # Save to monitoring table
    results_df.select(
        "notebook_run_id", "cell_id", "cell_name", "status", "output", 
        "error", "start_time", "end_time", "duration_seconds", "critical", "execution_date"
    ).write.mode("append").saveAsTable(f"`{BRONZE_CATALOG}`.`system`.`cell_executions`")
    
    return f"Saved {len(cell_results)} cell results with run_id: {run_id}"

monitor_cell("Save Cell Results", save_cell_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Cell Execution Summary

# COMMAND ----------

# Generate summary
print("\n" + "="*80)
print("📊 CELL EXECUTION SUMMARY")
print("="*80)

# Count statuses
success_count = sum(1 for r in cell_results if r["status"] == "SUCCESS")
failed_count = sum(1 for r in cell_results if r["status"] == "FAILED")
total_duration = sum(r["duration_seconds"] for r in cell_results)

print(f"\nTotal Cells: {len(cell_results)}")
print(f"✅ Successful: {success_count}")
print(f"❌ Failed: {failed_count}")
print(f"⏱️ Total Duration: {total_duration:.2f} seconds")
print(f"🎯 Success Rate: {(success_count/len(cell_results)*100):.1f}%")

# Show individual cell results
print("\nDetailed Results:")
print("-" * 80)
for result in cell_results:
    status_icon = "✅" if result["status"] == "SUCCESS" else "❌"
    print(f"{status_icon} {result['cell_id']}: {result['cell_name']}")
    print(f"   Duration: {result['duration_seconds']:.2f}s")
    if result["status"] == "SUCCESS":
        print(f"   Output: {result['output'][:100]}...")
    else:
        print(f"   Error: {result['error']}")
    print()

# Critical failures
critical_failures = [r for r in cell_results if r["critical"] and r["status"] == "FAILED"]
if critical_failures:
    print("\n⚠️ CRITICAL FAILURES:")
    for failure in critical_failures:
        print(f"  - {failure['cell_name']}: {failure['error']}")

# COMMAND ----------

# Final status determination
pipeline_success = success_count > 0 and len(critical_failures) == 0
exit_status = "SUCCESS" if pipeline_success else "FAILED"

print(f"\n{'='*80}")
print(f"🏁 PIPELINE STATUS: {exit_status}")
print(f"{'='*80}")

# COMMAND ----------

# Exit with detailed status
exit_message = {
    "status": exit_status,
    "total_cells": len(cell_results),
    "successful_cells": success_count,
    "failed_cells": failed_count,
    "success_rate": f"{(success_count/len(cell_results)*100):.1f}%",
    "duration_seconds": total_duration,
    "critical_failures": len(critical_failures)
}

dbutils.notebook.exit(json.dumps(exit_message))