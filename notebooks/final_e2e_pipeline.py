# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Final E2E Pipeline with Cell Monitoring
# MAGIC 
# MAGIC Complete implementation with all fixes and cell-level monitoring

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import json
import traceback

print("🚀 FINAL E2E PIPELINE WITH MONITORING")
print("=" * 80)
print(f"Start Time: {datetime.now()}")

# Initialize monitoring
pipeline_metrics = {
    "start_time": datetime.now(),
    "cells_executed": 0,
    "cells_passed": 0,
    "cells_failed": 0,
    "files_processed": 0,
    "records_processed": 0,
    "errors": []
}

def log_cell_execution(cell_name, status, message="", error=None):
    """Log cell execution status"""
    pipeline_metrics["cells_executed"] += 1
    if status == "SUCCESS":
        pipeline_metrics["cells_passed"] += 1
        print(f"✅ {cell_name}: {message}")
    else:
        pipeline_metrics["cells_failed"] += 1
        pipeline_metrics["errors"].append(f"{cell_name}: {error}")
        print(f"❌ {cell_name}: {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

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
    
    log_cell_execution("Configuration", "SUCCESS", f"Catalogs: {BRONZE_CATALOG}, {SILVER_CATALOG}, {GOLD_CATALOG}")
except Exception as e:
    log_cell_execution("Configuration", "FAILED", error=str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Unity Catalog Structure

# COMMAND ----------

try:
    created_items = []
    
    # Create catalogs
    for catalog in [BRONZE_CATALOG, SILVER_CATALOG, GOLD_CATALOG]:
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
            created_items.append(f"Catalog: {catalog}")
        except Exception as e:
            print(f"  Warning: {catalog} - {str(e)}")
    
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
                created_items.append(f"Schema: {catalog}.{schema}")
            except Exception as e:
                print(f"  Warning: {catalog}.{schema} - {str(e)}")
    
    # Create tracking tables
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
        processing_duration_seconds DOUBLE,
        pipeline_run_id STRING
    ) USING DELTA
    """)
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{BRONZE_CATALOG}`.`system`.`pipeline_metrics` (
        pipeline_run_id STRING,
        pipeline_name STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_seconds DOUBLE,
        files_processed INT,
        records_processed BIGINT,
        cells_executed INT,
        cells_passed INT,
        cells_failed INT,
        status STRING,
        error_details STRING
    ) USING DELTA
    """)
    
    log_cell_execution("Catalog Structure", "SUCCESS", f"Created {len(created_items)} items")
except Exception as e:
    log_cell_execution("Catalog Structure", "FAILED", error=str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Layer - CSV Processing

# COMMAND ----------

pipeline_run_id = f"PIPELINE_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

try:
    csv_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/products/"
    csv_processed = 0
    csv_records = 0
    
    # Find all CSV files
    def find_files(path, extension):
        files = []
        try:
            items = dbutils.fs.ls(path)
            for item in items:
                if item.path.endswith(extension):
                    files.append(item)
                elif item.isDir():
                    files.extend(find_files(item.path, extension))
        except Exception as e:
            print(f"  Warning: Error listing {path}: {str(e)}")
        return files
    
    csv_files = find_files(csv_path, '.csv')
    print(f"Found {len(csv_files)} CSV files")
    
    for file_info in csv_files:
        start_time = datetime.now()
        try:
            # Extract table name
            file_name = file_info.path.split('/')[-1].replace('.csv', '')
            folder_name = file_info.path.split('/')[-2]
            table_name = f"{folder_name}_{file_name}".replace('-', '_').replace(' ', '_').lower()
            
            # Read CSV
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_info.path)
            
            # Add metadata
            df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                   .withColumn("_source_file", lit(file_info.path)) \
                   .withColumn("_pipeline_run_id", lit(pipeline_run_id))
            
            record_count = df.count()
            csv_records += record_count
            
            # Write to Bronze
            df.write.mode("overwrite").option("overwriteSchema", "true") \
              .saveAsTable(f"`{BRONZE_CATALOG}`.`csv_data`.`{table_name}`")
            
            # Log success
            processing_time = (datetime.now() - start_time).total_seconds()
            spark.sql(f"""
                INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
                VALUES (
                    '{file_info.path}', '{file_name}.csv', 'csv', {file_info.size}, {record_count},
                    'SUCCESS', NULL, current_timestamp(), {processing_time}, '{pipeline_run_id}'
                )
            """)
            
            csv_processed += 1
            pipeline_metrics["files_processed"] += 1
            pipeline_metrics["records_processed"] += record_count
            print(f"  ✅ {file_name}.csv: {record_count} records")
            
        except Exception as e:
            spark.sql(f"""
                INSERT INTO `{BRONZE_CATALOG}`.`system`.`processed_files`
                VALUES (
                    '{file_info.path}', '{file_info.path.split('/')[-1]}', 'csv', {file_info.size}, 0,
                    'FAILED', '{str(e).replace("'", "''")}', current_timestamp(), 0, '{pipeline_run_id}'
                )
            """)
            print(f"  ❌ Failed: {str(e)}")
    
    log_cell_execution("Bronze CSV Processing", "SUCCESS", f"Processed {csv_processed} files, {csv_records} records")
    
except Exception as e:
    log_cell_execution("Bronze CSV Processing", "FAILED", error=str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze Layer - Excel Processing

# COMMAND ----------

try:
    # Install openpyxl
    import subprocess
    subprocess.check_call(["pip", "install", "-q", "openpyxl"])
    
    import pandas as pd
    import os
    
    excel_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/sales/"
    excel_processed = 0
    excel_records = 0
    
    excel_files = find_files(excel_path, '.xlsx')
    excel_files.extend(find_files(excel_path, '.xls'))
    print(f"Found {len(excel_files)} Excel files")
    
    for file_info in excel_files:
        start_time = datetime.now()
        try:
            # Extract table name
            file_name = file_info.path.split('/')[-1].replace('.xlsx', '').replace('.xls', '')
            
            if 'monthly' in file_info.path:
                table_name = f"monthly_{file_name}"
            elif 'daily' in file_info.path:
                table_name = f"daily_{file_name}"
            else:
                table_name = f"sales_{file_name}"
            
            table_name = table_name.replace('-', '_').replace(' ', '_').lower()
            
            # Copy to local
            local_file = f"/tmp/{file_name}.xlsx"
            dbutils.fs.cp(file_info.path, f"file:{local_file}")
            
            # Read with pandas
            df_pandas = pd.read_excel(local_file)
            df = spark.createDataFrame(df_pandas)
            
            # Add metadata
            df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                   .withColumn("_source_file", lit(file_info.path)) \
                   .withColumn("_pipeline_run_id", lit(pipeline_run_id))
            
            record_count = df.count()
            excel_records += record_count
            
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
                    '{file_info.path}', '{file_name}', 'excel', {file_info.size}, {record_count},
                    'SUCCESS', NULL, current_timestamp(), {processing_time}, '{pipeline_run_id}'
                )
            """)
            
            excel_processed += 1
            pipeline_metrics["files_processed"] += 1
            pipeline_metrics["records_processed"] += record_count
            print(f"  ✅ {file_name}: {record_count} records")
            
        except Exception as e:
            print(f"  ❌ Failed: {str(e)}")
    
    if excel_processed > 0:
        log_cell_execution("Bronze Excel Processing", "SUCCESS", f"Processed {excel_processed} files, {excel_records} records")
    else:
        log_cell_execution("Bronze Excel Processing", "WARNING", "No Excel files processed")
        
except Exception as e:
    log_cell_execution("Bronze Excel Processing", "WARNING", error=str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver Layer - Product Master

# COMMAND ----------

try:
    # Get all product tables from Bronze
    csv_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`csv_data`").collect()
    product_dfs = []
    
    for table in csv_tables:
        table_name = table.tableName
        try:
            # Check columns
            columns = spark.sql(f"DESCRIBE `{BRONZE_CATALOG}`.`csv_data`.`{table_name}`").collect()
            column_names = [col.col_name.lower() for col in columns]
            
            if 'product_id' in column_names:
                # Build dynamic select based on available columns
                select_cols = ["product_id", "product_name"]
                optional_cols = {
                    "category": "COALESCE(category, 'Unknown') as category",
                    "sub_category": "COALESCE(sub_category, 'Unknown') as sub_category",
                    "brand": "COALESCE(brand, 'Unknown') as brand",
                    "price": "CAST(COALESCE(price, 0) AS DOUBLE) as price",
                    "cost": "CAST(COALESCE(cost, 0) AS DOUBLE) as cost",
                    "status": "COALESCE(status, 'ACTIVE') as status"
                }
                
                for col, expr in optional_cols.items():
                    if col in column_names:
                        select_cols.append(expr)
                
                select_cols.extend(["_ingestion_timestamp", "_source_file"])
                
                df = spark.sql(f"""
                    SELECT {', '.join(select_cols)}
                    FROM `{BRONZE_CATALOG}`.`csv_data`.`{table_name}`
                    WHERE product_id IS NOT NULL
                """)
                
                product_dfs.append(df)
                print(f"  Added {table_name} to product master")
        except Exception as e:
            print(f"  Skipped {table_name}: {str(e)}")
    
    if product_dfs:
        # Union all
        all_products = product_dfs[0]
        for df in product_dfs[1:]:
            all_products = all_products.unionByName(df, allowMissingColumns=True)
        
        # Create master with calculations
        product_master = all_products.dropDuplicates(["product_id"])
        
        # Add calculated fields if price and cost exist
        if "price" in product_master.columns and "cost" in product_master.columns:
            product_master = product_master \
                .withColumn("profit_margin", col("price") - col("cost")) \
                .withColumn("profit_margin_percent", 
                           when(col("price") > 0, (col("profit_margin") / col("price")) * 100).otherwise(0))
        
        product_master = product_master.withColumn("last_updated", current_timestamp())
        
        # Write to Silver
        product_master.write.mode("overwrite").saveAsTable(f"`{SILVER_CATALOG}`.`products`.`product_master`")
        
        count = product_master.count()
        log_cell_execution("Silver Product Master", "SUCCESS", f"Created with {count} unique products")
    else:
        log_cell_execution("Silver Product Master", "WARNING", "No product tables found")
        
except Exception as e:
    log_cell_execution("Silver Product Master", "FAILED", error=str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver Layer - Sales Transactions

# COMMAND ----------

try:
    # Get all sales tables from Bronze
    excel_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`excel_data`").collect()
    sales_dfs = []
    
    for table in excel_tables:
        table_name = table.tableName
        try:
            # Check columns
            columns = spark.sql(f"DESCRIBE `{BRONZE_CATALOG}`.`excel_data`.`{table_name}`").collect()
            column_names = [col.col_name.lower() for col in columns]
            
            # Look for sales-related tables
            if any(col in column_names for col in ['transaction_id', 'order_id', 'sale_id']):
                # Build dynamic query
                id_col = next((col for col in ['transaction_id', 'order_id', 'sale_id'] if col in column_names), None)
                
                select_expr = f"""
                    SELECT 
                        {id_col} as transaction_id,
                        COALESCE(transaction_date, sale_date, order_date, current_date()) as transaction_date,
                        COALESCE(customer_id, 'UNKNOWN') as customer_id,
                        COALESCE(product_code, product_id, 'UNKNOWN') as product_code,
                        CAST(COALESCE(quantity, 1) AS INT) as quantity,
                        CAST(COALESCE(unit_price, price, 0) AS DOUBLE) as unit_price,
                        CAST(COALESCE(total_amount, amount, 0) AS DOUBLE) as total_amount,
                        _ingestion_timestamp,
                        _source_file
                    FROM `{BRONZE_CATALOG}`.`excel_data`.`{table_name}`
                    WHERE {id_col} IS NOT NULL
                """
                
                df = spark.sql(select_expr)
                sales_dfs.append(df)
                print(f"  Added {table_name} to sales transactions")
        except Exception as e:
            print(f"  Skipped {table_name}: {str(e)}")
    
    if sales_dfs:
        # Union all
        all_sales = sales_dfs[0]
        for df in sales_dfs[1:]:
            all_sales = all_sales.unionByName(df, allowMissingColumns=True)
        
        # Process dates and add time dimensions
        sales_transactions = all_sales \
            .withColumn("transaction_date", to_date(col("transaction_date"))) \
            .withColumn("transaction_year", year(col("transaction_date"))) \
            .withColumn("transaction_month", month(col("transaction_date"))) \
            .withColumn("transaction_day", dayofmonth(col("transaction_date"))) \
            .withColumn("last_updated", current_timestamp())
        
        # Write to Silver
        sales_transactions.write.mode("overwrite").saveAsTable(f"`{SILVER_CATALOG}`.`sales`.`sales_transactions`")
        
        count = sales_transactions.count()
        log_cell_execution("Silver Sales Transactions", "SUCCESS", f"Created with {count} transactions")
    else:
        log_cell_execution("Silver Sales Transactions", "WARNING", "No sales tables found")
        
except Exception as e:
    log_cell_execution("Silver Sales Transactions", "WARNING", error=str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold Layer - Analytics

# COMMAND ----------

try:
    # Product Performance
    if spark.sql(f"SHOW TABLES IN `{SILVER_CATALOG}`.`products` LIKE 'product_master'").count() > 0:
        spark.sql(f"""
            CREATE OR REPLACE TABLE `{GOLD_CATALOG}`.`analytics`.`product_performance` AS
            WITH product_metrics AS (
                SELECT 
                    COALESCE(category, 'Unknown') as category,
                    COALESCE(brand, 'Unknown') as brand,
                    COALESCE(status, 'ACTIVE') as status,
                    COUNT(DISTINCT product_id) as product_count,
                    AVG(COALESCE(price, 0)) as avg_price,
                    AVG(COALESCE(cost, 0)) as avg_cost,
                    AVG(COALESCE(profit_margin, 0)) as avg_margin,
                    AVG(COALESCE(profit_margin_percent, 0)) as avg_margin_percent
                FROM `{SILVER_CATALOG}`.`products`.`product_master`
                GROUP BY category, brand, status
            )
            SELECT 
                *,
                CASE 
                    WHEN avg_margin_percent > 50 THEN 'High Margin'
                    WHEN avg_margin_percent > 20 THEN 'Medium Margin'
                    ELSE 'Low Margin'
                END as margin_category,
                current_timestamp() as last_updated
            FROM product_metrics
        """)
        
        count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`analytics`.`product_performance`").collect()[0][0]
        print(f"  ✅ Product Performance: {count} segments")
    
    # Sales Summary
    if spark.sql(f"SHOW TABLES IN `{SILVER_CATALOG}`.`sales` LIKE 'sales_transactions'").count() > 0:
        spark.sql(f"""
            CREATE OR REPLACE TABLE `{GOLD_CATALOG}`.`reporting`.`sales_summary` AS
            SELECT 
                transaction_date,
                transaction_year,
                transaction_month,
                COUNT(DISTINCT transaction_id) as transaction_count,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(quantity) as total_quantity,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_transaction_value,
                current_timestamp() as last_updated
            FROM `{SILVER_CATALOG}`.`sales`.`sales_transactions`
            WHERE transaction_date IS NOT NULL
            GROUP BY transaction_date, transaction_year, transaction_month
        """)
        
        count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`reporting`.`sales_summary`").collect()[0][0]
        print(f"  ✅ Sales Summary: {count} days")
    
    log_cell_execution("Gold Layer Analytics", "SUCCESS", "Analytics tables created")
    
except Exception as e:
    log_cell_execution("Gold Layer Analytics", "WARNING", error=str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Test Dynamic Processing

# COMMAND ----------

try:
    # Create dynamic test file
    test_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    test_data = [
        (f"FINAL001_{test_timestamp}", "Final Test Product 1", "TestCategory", "TestSub", "FinalBrand", 299.99, 149.99, "ACTIVE"),
        (f"FINAL002_{test_timestamp}", "Final Test Product 2", "TestCategory", "TestSub", "FinalBrand", 399.99, 199.99, "ACTIVE"),
        (f"FINAL003_{test_timestamp}", "Final Test Product 3", "TestCategory", "TestSub", "FinalBrand", 499.99, 249.99, "ACTIVE"),
        (f"FINAL004_{test_timestamp}", "Final Test Product 4", "TestCategory", "TestSub", "FinalBrand", 599.99, 299.99, "INACTIVE")
    ]
    
    test_df = spark.createDataFrame(test_data, 
        ["product_id", "product_name", "category", "sub_category", "brand", "price", "cost", "status"])
    
    # Save to storage
    test_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/products/final_test/dynamic_{test_timestamp}.csv"
    test_df.write.mode("overwrite").option("header", "true").csv(test_path)
    
    log_cell_execution("Dynamic Test File", "SUCCESS", f"Created test file with {test_df.count()} records")
    
    print(f"\n📝 To verify dynamic processing:")
    print(f"   1. Re-run this pipeline")
    print(f"   2. Check if the new file 'dynamic_{test_timestamp}.csv' is processed")
    print(f"   3. Verify records appear in Bronze, Silver, and Gold layers")
    
except Exception as e:
    log_cell_execution("Dynamic Test File", "WARNING", error=str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Pipeline Metrics & Summary

# COMMAND ----------

# Calculate final metrics
pipeline_metrics["end_time"] = datetime.now()
pipeline_metrics["duration_seconds"] = (pipeline_metrics["end_time"] - pipeline_metrics["start_time"]).total_seconds()

# Save pipeline metrics
try:
    metrics_data = [(
        pipeline_run_id,
        "Final E2E Pipeline",
        pipeline_metrics["start_time"],
        pipeline_metrics["end_time"],
        pipeline_metrics["duration_seconds"],
        pipeline_metrics["files_processed"],
        pipeline_metrics["records_processed"],
        pipeline_metrics["cells_executed"],
        pipeline_metrics["cells_passed"],
        pipeline_metrics["cells_failed"],
        "SUCCESS" if pipeline_metrics["cells_failed"] == 0 else "COMPLETED_WITH_ERRORS",
        json.dumps(pipeline_metrics["errors"]) if pipeline_metrics["errors"] else None
    )]
    
    metrics_df = spark.createDataFrame(metrics_data, 
        ["pipeline_run_id", "pipeline_name", "start_time", "end_time", "duration_seconds",
         "files_processed", "records_processed", "cells_executed", "cells_passed", "cells_failed",
         "status", "error_details"])
    
    metrics_df.write.mode("append").saveAsTable(f"`{BRONZE_CATALOG}`.`system`.`pipeline_metrics`")
except:
    pass

# Display summary
print("\n" + "="*80)
print("📊 PIPELINE EXECUTION SUMMARY")
print("="*80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Duration: {pipeline_metrics['duration_seconds']:.2f} seconds")
print(f"Files Processed: {pipeline_metrics['files_processed']}")
print(f"Records Processed: {pipeline_metrics['records_processed']:,}")
print(f"Cells Executed: {pipeline_metrics['cells_executed']}")
print(f"Cells Passed: {pipeline_metrics['cells_passed']}")
print(f"Cells Failed: {pipeline_metrics['cells_failed']}")

if pipeline_metrics["cells_failed"] > 0:
    print(f"\n⚠️ Errors encountered:")
    for error in pipeline_metrics["errors"][:5]:
        print(f"  - {error}")

# Success rate
success_rate = (pipeline_metrics["cells_passed"] / pipeline_metrics["cells_executed"] * 100) if pipeline_metrics["cells_executed"] > 0 else 0
print(f"\n🎯 SUCCESS RATE: {success_rate:.1f}%")

if success_rate == 100:
    print("\n✅ PIPELINE FULLY SUCCESSFUL!")
elif success_rate >= 80:
    print("\n✅ PIPELINE COMPLETED WITH MINOR ISSUES")
else:
    print("\n⚠️ PIPELINE COMPLETED WITH ERRORS")

print(f"\nEnd Time: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Data Quality Checks

# COMMAND ----------

print("\n📊 DATA QUALITY VALIDATION")
print("="*60)

# Check Bronze layer
bronze_csv = spark.sql(f"SELECT COUNT(*) FROM `{BRONZE_CATALOG}`.`system`.`processed_files` WHERE file_type = 'csv' AND status = 'SUCCESS'").collect()[0][0]
bronze_excel = spark.sql(f"SELECT COUNT(*) FROM `{BRONZE_CATALOG}`.`system`.`processed_files` WHERE file_type = 'excel' AND status = 'SUCCESS'").collect()[0][0]

print(f"\n🥉 Bronze Layer:")
print(f"   CSV files: {bronze_csv}")
print(f"   Excel files: {bronze_excel}")

# Check Silver layer
try:
    product_count = spark.sql(f"SELECT COUNT(*) FROM `{SILVER_CATALOG}`.`products`.`product_master`").collect()[0][0]
    print(f"\n🥈 Silver Layer:")
    print(f"   Product Master: {product_count:,} products")
except:
    print("\n🥈 Silver Layer: No product master found")

try:
    sales_count = spark.sql(f"SELECT COUNT(*) FROM `{SILVER_CATALOG}`.`sales`.`sales_transactions`").collect()[0][0]
    print(f"   Sales Transactions: {sales_count:,} records")
except:
    print("   Sales Transactions: No data found")

# Check Gold layer
try:
    print(f"\n🥇 Gold Layer:")
    perf_count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`analytics`.`product_performance`").collect()[0][0]
    print(f"   Product Performance: {perf_count} segments")
except:
    print("   Product Performance: Not created")

# COMMAND ----------

# Return status
exit_status = {
    "pipeline_run_id": pipeline_run_id,
    "status": "SUCCESS" if pipeline_metrics["cells_failed"] == 0 else "COMPLETED_WITH_ERRORS",
    "success_rate": f"{success_rate:.1f}%",
    "files_processed": pipeline_metrics["files_processed"],
    "records_processed": pipeline_metrics["records_processed"],
    "duration_seconds": pipeline_metrics["duration_seconds"]
}

dbutils.notebook.exit(json.dumps(exit_status))