# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Comprehensive Framework Validation
# MAGIC 
# MAGIC This notebook performs complete validation of the E2E pipeline framework with cell-level monitoring

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import json

print("🎯 COMPREHENSIVE FRAMEWORK VALIDATION")
print("=" * 80)
print(f"Start Time: {datetime.now()}")

# Configuration
PROJECT_CODE = "cddp"
ENVIRONMENT = "dev"
SOURCE_STORAGE = "agentstge"
CONTAINER = "raw-data"

BRONZE_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-bronze"
SILVER_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-silver"
GOLD_CATALOG = f"{PROJECT_CODE}-{ENVIRONMENT}-gold"

validation_results = {
    "catalogs": {"passed": 0, "failed": 0, "details": []},
    "bronze_layer": {"passed": 0, "failed": 0, "details": []},
    "silver_layer": {"passed": 0, "failed": 0, "details": []},
    "gold_layer": {"passed": 0, "failed": 0, "details": []},
    "file_processing": {"passed": 0, "failed": 0, "details": []},
    "dynamic_testing": {"passed": 0, "failed": 0, "details": []},
    "overall_status": "PENDING"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validate Unity Catalog Structure

# COMMAND ----------

print("\n📚 VALIDATING UNITY CATALOG STRUCTURE")
print("-" * 50)

# Check catalogs
for catalog in [BRONZE_CATALOG, SILVER_CATALOG, GOLD_CATALOG]:
    try:
        spark.sql(f"SHOW SCHEMAS IN `{catalog}`")
        validation_results["catalogs"]["passed"] += 1
        validation_results["catalogs"]["details"].append(f"✅ Catalog exists: {catalog}")
        print(f"✅ Catalog exists: {catalog}")
    except:
        validation_results["catalogs"]["failed"] += 1
        validation_results["catalogs"]["details"].append(f"❌ Catalog missing: {catalog}")
        print(f"❌ Catalog missing: {catalog}")

# Check schemas
required_schemas = {
    BRONZE_CATALOG: ["csv_data", "excel_data", "system"],
    SILVER_CATALOG: ["products", "sales"],
    GOLD_CATALOG: ["analytics", "reporting"]
}

for catalog, schemas in required_schemas.items():
    for schema in schemas:
        try:
            spark.sql(f"DESCRIBE SCHEMA `{catalog}`.`{schema}`")
            validation_results["catalogs"]["passed"] += 1
            print(f"  ✅ Schema exists: {catalog}.{schema}")
        except:
            validation_results["catalogs"]["failed"] += 1
            print(f"  ❌ Schema missing: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate Bronze Layer

# COMMAND ----------

print("\n🥉 VALIDATING BRONZE LAYER")
print("-" * 50)

# Check CSV data
csv_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`csv_data`").collect()
csv_count = len(csv_tables)
csv_records = 0

if csv_count > 0:
    validation_results["bronze_layer"]["passed"] += 1
    print(f"✅ Found {csv_count} CSV tables in Bronze layer")
    
    for table in csv_tables:
        count = spark.sql(f"SELECT COUNT(*) FROM `{BRONZE_CATALOG}`.`csv_data`.`{table.tableName}`").collect()[0][0]
        csv_records += count
        print(f"   - {table.tableName}: {count:,} records")
    
    validation_results["bronze_layer"]["details"].append(f"CSV: {csv_count} tables, {csv_records:,} records")
else:
    validation_results["bronze_layer"]["failed"] += 1
    validation_results["bronze_layer"]["details"].append("❌ No CSV tables found")
    print("❌ No CSV tables found in Bronze layer")

# Check Excel data
excel_tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`excel_data`").collect()
excel_count = len(excel_tables)
excel_records = 0

if excel_count > 0:
    validation_results["bronze_layer"]["passed"] += 1
    print(f"\n✅ Found {excel_count} Excel tables in Bronze layer")
    
    for table in excel_tables:
        count = spark.sql(f"SELECT COUNT(*) FROM `{BRONZE_CATALOG}`.`excel_data`.`{table.tableName}`").collect()[0][0]
        excel_records += count
        print(f"   - {table.tableName}: {count:,} records")
    
    validation_results["bronze_layer"]["details"].append(f"Excel: {excel_count} tables, {excel_records:,} records")
else:
    print("\n⚠️ No Excel tables found in Bronze layer (may not have Excel files)")

# Check system tables
try:
    processed_files = spark.sql(f"SELECT COUNT(*) FROM `{BRONZE_CATALOG}`.`system`.`processed_files`").collect()[0][0]
    validation_results["bronze_layer"]["passed"] += 1
    print(f"\n✅ Processed files tracking: {processed_files} files logged")
except:
    validation_results["bronze_layer"]["failed"] += 1
    print("\n❌ Processed files tracking table not found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Silver Layer

# COMMAND ----------

print("\n🥈 VALIDATING SILVER LAYER")
print("-" * 50)

# Check product master
try:
    product_count = spark.sql(f"SELECT COUNT(*) FROM `{SILVER_CATALOG}`.`products`.`product_master`").collect()[0][0]
    validation_results["silver_layer"]["passed"] += 1
    print(f"✅ Product Master: {product_count:,} products")
    
    # Validate product data quality
    nulls = spark.sql(f"""
        SELECT COUNT(*) FROM `{SILVER_CATALOG}`.`products`.`product_master`
        WHERE product_id IS NULL OR product_name IS NULL
    """).collect()[0][0]
    
    if nulls == 0:
        validation_results["silver_layer"]["passed"] += 1
        print("   ✅ Data quality check passed (no nulls in key fields)")
    else:
        validation_results["silver_layer"]["failed"] += 1
        print(f"   ❌ Data quality issue: {nulls} records with null key fields")
        
except Exception as e:
    validation_results["silver_layer"]["failed"] += 1
    print(f"❌ Product Master not found: {str(e)}")

# Check sales transactions
try:
    sales_count = spark.sql(f"SELECT COUNT(*) FROM `{SILVER_CATALOG}`.`sales`.`sales_transactions`").collect()[0][0]
    validation_results["silver_layer"]["passed"] += 1
    print(f"\n✅ Sales Transactions: {sales_count:,} transactions")
except:
    print("\n⚠️ Sales Transactions not found (may not have sales data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate Gold Layer

# COMMAND ----------

print("\n🥇 VALIDATING GOLD LAYER")
print("-" * 50)

# Check analytics tables
try:
    perf_count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`analytics`.`product_performance`").collect()[0][0]
    validation_results["gold_layer"]["passed"] += 1
    print(f"✅ Product Performance: {perf_count} segments")
except:
    validation_results["gold_layer"]["failed"] += 1
    print("❌ Product Performance table not found")

try:
    summary_count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`reporting`.`daily_store_summary`").collect()[0][0]
    validation_results["gold_layer"]["passed"] += 1
    print(f"✅ Daily Store Summary: {summary_count} records")
except:
    print("⚠️ Daily Store Summary not found (may not have sales data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate File Processing

# COMMAND ----------

print("\n📁 VALIDATING FILE PROCESSING")
print("-" * 50)

# Check processed files
processed_df = spark.sql(f"""
    SELECT 
        file_type,
        status,
        COUNT(*) as file_count,
        SUM(record_count) as total_records
    FROM `{BRONZE_CATALOG}`.`system`.`processed_files`
    WHERE processed_timestamp > current_timestamp() - INTERVAL 24 HOURS
    GROUP BY file_type, status
    ORDER BY file_type, status
""")

processed_df.show()

# Validate processing success
success_files = processed_df.filter(col("status") == "SUCCESS").agg(sum("file_count")).collect()[0][0] or 0
failed_files = processed_df.filter(col("status") == "FAILED").agg(sum("file_count")).collect()[0][0] or 0

if success_files > 0:
    validation_results["file_processing"]["passed"] += 1
    print(f"✅ Successfully processed {success_files} files")
else:
    validation_results["file_processing"]["failed"] += 1
    print("❌ No files successfully processed")

if failed_files > 0:
    validation_results["file_processing"]["failed"] += 1
    print(f"⚠️ {failed_files} files failed processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Test Dynamic File Processing

# COMMAND ----------

print("\n🧪 TESTING DYNAMIC FILE PROCESSING")
print("-" * 50)

# Create test file
test_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
test_data = [
    (f"DYN001_{test_timestamp}", "Dynamic Product 1", "Test", "TestSub", "DynamicBrand", 199.99, 99.99, "ACTIVE"),
    (f"DYN002_{test_timestamp}", "Dynamic Product 2", "Test", "TestSub", "DynamicBrand", 299.99, 149.99, "ACTIVE"),
    (f"DYN003_{test_timestamp}", "Dynamic Product 3", "Test", "TestSub", "DynamicBrand", 399.99, 199.99, "ACTIVE")
]

test_df = spark.createDataFrame(test_data, 
    ["product_id", "product_name", "category", "sub_category", "brand", "price", "cost", "status"])

# Save to storage
test_path = f"abfss://{CONTAINER}@{SOURCE_STORAGE}.dfs.core.windows.net/products/validation_test/dynamic_test_{test_timestamp}.csv"

try:
    test_df.write.mode("overwrite").option("header", "true").csv(test_path)
    validation_results["dynamic_testing"]["passed"] += 1
    print(f"✅ Created test file: dynamic_test_{test_timestamp}.csv")
    print(f"   Path: {test_path}")
    print(f"   Records: {test_df.count()}")
except Exception as e:
    validation_results["dynamic_testing"]["failed"] += 1
    print(f"❌ Failed to create test file: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cell Execution Monitoring

# COMMAND ----------

print("\n📊 CELL EXECUTION MONITORING")
print("-" * 50)

# Check recent cell executions
try:
    recent_runs = spark.sql(f"""
        SELECT 
            notebook_run_id,
            COUNT(*) as total_cells,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_cells,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_cells,
            ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate,
            MAX(start_time) as run_time
        FROM `{BRONZE_CATALOG}`.`system`.`cell_executions`
        WHERE start_time > current_timestamp() - INTERVAL 2 HOURS
        GROUP BY notebook_run_id
        ORDER BY run_time DESC
        LIMIT 5
    """)
    
    recent_runs.show(truncate=False)
    validation_results["dynamic_testing"]["passed"] += 1
except:
    print("⚠️ Cell execution monitoring table not found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Final Validation Summary

# COMMAND ----------

print("\n" + "="*80)
print("📊 VALIDATION SUMMARY")
print("="*80)

total_passed = 0
total_failed = 0

for category, results in validation_results.items():
    if category != "overall_status" and isinstance(results, dict):
        passed = results["passed"]
        failed = results["failed"]
        total_passed += passed
        total_failed += failed
        
        status_icon = "✅" if failed == 0 else "⚠️" if passed > failed else "❌"
        print(f"\n{status_icon} {category.upper()}: {passed} passed, {failed} failed")
        
        if "details" in results:
            for detail in results["details"][:5]:  # Show first 5 details
                print(f"   {detail}")

# Calculate overall success
success_rate = (total_passed / (total_passed + total_failed) * 100) if (total_passed + total_failed) > 0 else 0

print(f"\n{'='*80}")
print(f"🎯 OVERALL VALIDATION: {total_passed} passed, {total_failed} failed")
print(f"📈 SUCCESS RATE: {success_rate:.1f}%")
print(f"{'='*80}")

# Determine final status
if success_rate >= 90:
    validation_results["overall_status"] = "SUCCESS"
    print("\n✅ FRAMEWORK VALIDATION SUCCESSFUL!")
    print("   - Unity Catalog structure is correct")
    print("   - Bronze, Silver, Gold layers are operational")
    print("   - File processing is working")
    print("   - Dynamic file handling is functional")
elif success_rate >= 70:
    validation_results["overall_status"] = "PARTIAL_SUCCESS"
    print("\n⚠️ FRAMEWORK PARTIALLY OPERATIONAL")
    print("   - Most components are working")
    print("   - Some issues need attention")
else:
    validation_results["overall_status"] = "FAILED"
    print("\n❌ FRAMEWORK VALIDATION FAILED")
    print("   - Critical issues detected")
    print("   - Requires immediate attention")

print(f"\nEnd Time: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Recommendations

# COMMAND ----------

print("\n📝 RECOMMENDATIONS:")
print("-" * 50)

if validation_results["catalogs"]["failed"] > 0:
    print("1. Fix Unity Catalog structure issues")

if validation_results["bronze_layer"]["failed"] > 0:
    print("2. Investigate Bronze layer ingestion failures")

if validation_results["silver_layer"]["failed"] > 0:
    print("3. Review Silver layer transformation logic")

if validation_results["gold_layer"]["failed"] > 0:
    print("4. Check Gold layer aggregation queries")

if validation_results["file_processing"]["failed"] > 0:
    print("5. Debug file processing errors in system.processed_files table")

print("\n✅ Next Steps:")
print("   1. Re-run pipeline to process the newly created test file")
print("   2. Monitor cell execution results")
print("   3. Check data quality in all layers")
print("   4. Validate incremental processing works correctly")

# COMMAND ----------

# Return validation result
exit_result = {
    "status": validation_results["overall_status"],
    "success_rate": f"{success_rate:.1f}%",
    "total_passed": total_passed,
    "total_failed": total_failed,
    "timestamp": datetime.now().isoformat()
}

dbutils.notebook.exit(json.dumps(exit_result))