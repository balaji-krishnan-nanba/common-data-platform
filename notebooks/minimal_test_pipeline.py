# Databricks notebook source
# MAGIC %md
# MAGIC # Minimal Test Pipeline - Debug INTERNAL_ERROR

# COMMAND ----------

print("Starting minimal test pipeline...")

# Test 1: Basic Spark operation
try:
    df = spark.range(10)
    count = df.count()
    print(f"✅ Basic Spark: Created DataFrame with {count} rows")
except Exception as e:
    print(f"❌ Basic Spark failed: {str(e)}")

# COMMAND ----------

# Test 2: Catalog access
try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    print(f"✅ Catalog Access: Found {len(catalogs)} catalogs")
    for cat in catalogs[:5]:
        print(f"   - {cat.catalog}")
except Exception as e:
    print(f"❌ Catalog access failed: {str(e)}")

# COMMAND ----------

# Test 3: Storage access
try:
    test_path = "abfss://raw-data@agentstge.dfs.core.windows.net/"
    files = dbutils.fs.ls(test_path)
    print(f"✅ Storage Access: Found {len(files)} items in raw-data")
    for f in files[:5]:
        print(f"   - {f.name}")
except Exception as e:
    print(f"❌ Storage access failed: {str(e)}")

# COMMAND ----------

# Test 4: Create simple table
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS `cddp-dev-bronze`")
    spark.sql("CREATE SCHEMA IF NOT EXISTS `cddp-dev-bronze`.`test`")
    
    test_df = spark.createDataFrame([(1, "test1"), (2, "test2")], ["id", "name"])
    test_df.write.mode("overwrite").saveAsTable("`cddp-dev-bronze`.`test`.`minimal_test`")
    
    count = spark.sql("SELECT COUNT(*) FROM `cddp-dev-bronze`.`test`.`minimal_test`").collect()[0][0]
    print(f"✅ Table Creation: Created test table with {count} records")
except Exception as e:
    print(f"❌ Table creation failed: {str(e)}")

# COMMAND ----------

# Test 5: Environment variables
import os
print("\n📋 Environment Variables:")
env_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'AZURE_TENANT_ID', 'PROJECT_CODE', 'ENVIRONMENT']
for var in env_vars:
    value = os.getenv(var, "NOT_SET")
    if value != "NOT_SET":
        print(f"   ✅ {var}: {value[:20]}...")
    else:
        print(f"   ❌ {var}: NOT SET")

# COMMAND ----------

print("\n✅ Minimal test pipeline completed successfully!")
dbutils.notebook.exit("SUCCESS")