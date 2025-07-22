# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Diagnose Framework Issues

# COMMAND ----------

import os
import sys

print("🔍 DIAGNOSING FRAMEWORK ISSUES")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check Repo Access

# COMMAND ----------

print("1. Checking Repo Access:")
try:
    # Check if repo exists
    repo_path = "/Repos/balaji.krishnan@nanba.co.uk/common-data-platform"
    files = dbutils.fs.ls(f"file:{repo_path}")
    print(f"✅ Repo exists at {repo_path}")
    print("   Contents:")
    for f in files[:10]:
        print(f"   - {f.name}")
except Exception as e:
    print(f"❌ Repo access error: {str(e)}")
    # Try workspace path
    try:
        workspace_path = "/Workspace/Repos/balaji.krishnan@nanba.co.uk/common-data-platform"
        files = dbutils.fs.ls(f"file:{workspace_path}")
        print(f"✅ Found at workspace path: {workspace_path}")
    except Exception as e2:
        print(f"❌ Workspace path error: {str(e2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check Environment Variables

# COMMAND ----------

print("\n2. Environment Variables:")
env_vars = ['PROJECT_CODE', 'ENVIRONMENT', 'AZURE_SOURCE_STORAGE_ACCOUNT']
for var in env_vars:
    value = spark.conf.get(f"spark.env.{var}", "NOT SET")
    print(f"   {var}: {value}")

# Set them if not set
os.environ['PROJECT_CODE'] = 'cddp'
os.environ['ENVIRONMENT'] = 'dev'
os.environ['AZURE_SOURCE_STORAGE_ACCOUNT'] = 'agentstge'
print("\n✅ Environment variables set")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test Package Installation

# COMMAND ----------

print("\n3. Testing Package Installation:")

# First, add repo to path
sys.path.insert(0, '/Repos/balaji.krishnan@nanba.co.uk/common-data-platform/src')
print("✅ Added src to path")

# Try direct import
try:
    import common_data_platform
    print("✅ Can import common_data_platform")
    print(f"   Location: {common_data_platform.__file__}")
except Exception as e:
    print(f"❌ Direct import failed: {str(e)}")
    
    # Try installing
    print("\n   Attempting pip install...")
    %pip install -e /Repos/balaji.krishnan@nanba.co.uk/common-data-platform
    
    # Restart and try again
    dbutils.library.restartPython()

# COMMAND ----------

# After restart, test imports
import os
import sys

# Set environment
os.environ['PROJECT_CODE'] = 'cddp'
os.environ['ENVIRONMENT'] = 'dev'
os.environ['AZURE_SOURCE_STORAGE_ACCOUNT'] = 'agentstge'

# Add to path
sys.path.insert(0, '/Repos/balaji.krishnan@nanba.co.uk/common-data-platform/src')

print("4. Testing Imports After Install:")
try:
    from common_data_platform.core.config_manager import ConfigManager
    from common_data_platform.ingestion.file_ingester import FileIngester
    print("✅ All imports successful!")
except Exception as e:
    print(f"❌ Import error: {str(e)}")
    print("\n   Checking package structure...")
    
    # List what's in the package
    import os
    src_path = "/Repos/balaji.krishnan@nanba.co.uk/common-data-platform/src/common_data_platform"
    if os.path.exists(src_path):
        print(f"   Contents of {src_path}:")
        for item in os.listdir(src_path):
            print(f"   - {item}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Check Storage Access

# COMMAND ----------

print("\n5. Storage Access Test:")
try:
    # Test direct ABFSS access
    storage_path = "abfss://raw-data@agentstge.dfs.core.windows.net/"
    files = dbutils.fs.ls(storage_path)
    print(f"✅ Can access storage: {len(files)} items found")
    for f in files[:5]:
        print(f"   - {f.name}")
except Exception as e:
    print(f"❌ Storage access error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Check Unity Catalog

# COMMAND ----------

print("\n6. Unity Catalog Check:")
try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    print(f"✅ Found {len(catalogs)} catalogs")
    for cat in catalogs:
        if 'cddp' in cat.catalog:
            print(f"   - {cat.catalog}")
            # Check schemas
            try:
                schemas = spark.sql(f"SHOW SCHEMAS IN `{cat.catalog}`").collect()
                print(f"     Schemas: {len(schemas)}")
            except:
                pass
except Exception as e:
    print(f"❌ Catalog error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Test Direct Ingestion

# COMMAND ----------

print("\n7. Testing Direct File Read:")
try:
    # Read a CSV file directly
    test_df = spark.read.option("header", "true").csv("abfss://raw-data@agentstge.dfs.core.windows.net/products/catalog/product_catalog.csv")
    count = test_df.count()
    print(f"✅ Can read CSV directly: {count} records")
    test_df.show(5, truncate=False)
except Exception as e:
    print(f"❌ File read error: {str(e)}")

# COMMAND ----------

print("\n✅ Diagnosis complete - check results above for issues")