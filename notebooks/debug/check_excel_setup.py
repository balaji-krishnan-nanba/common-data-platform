# Databricks notebook source
# MAGIC %md
# MAGIC # Debug Excel Pipeline Setup
# MAGIC This notebook checks the Excel pipeline configuration and storage setup

# COMMAND ----------

import os
import sys

# Check environment variables
print("🔍 Environment Variables:")
env_vars = [
    'PROJECT_CODE', 
    'ENVIRONMENT', 
    'AZURE_STORAGE_ACCOUNT',
    'AZURE_SOURCE_STORAGE_ACCOUNT',
    'AZURE_DATALAKE_STORAGE_ACCOUNT'
]

for var in env_vars:
    value = os.getenv(var, 'NOT SET')
    print(f"   {var}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if package is installed

# COMMAND ----------

try:
    import common_data_platform
    print("✅ common_data_platform package is installed")
    print(f"   Version: {common_data_platform.__version__ if hasattr(common_data_platform, '__version__') else 'Unknown'}")
except ImportError as e:
    print("❌ common_data_platform package NOT installed")
    print(f"   Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Excel source configuration

# COMMAND ----------

try:
    from common_data_platform.core.config_manager import ConfigManager
    
    # Initialize config manager
    config_manager = ConfigManager()
    
    print(f"📋 Configuration:")
    print(f"   Project Code: {config_manager.project_code}")
    print(f"   Environment: {config_manager.environment}")
    
    # Load Excel sources
    excel_config = config_manager.load_source_config("excel")
    
    print(f"\n📊 Excel Sources Found: {len(excel_config)}")
    for source_name, source_config in excel_config.items():
        print(f"\n   Source: {source_name}")
        print(f"   Storage Account: {source_config.get('connection', {}).get('storage_account', 'Not set')}")
        print(f"   Container: {source_config.get('connection', {}).get('container', 'Not set')}")
        print(f"   Path Pattern: {source_config.get('connection', {}).get('path_pattern', 'Not set')}")
        
except Exception as e:
    print(f"❌ Error loading Excel configuration: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Azure Storage Access

# COMMAND ----------

try:
    from common_data_platform.core.secret_manager import SecretManager
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    secret_manager = SecretManager(spark)
    
    # Check if we can access secrets
    print("🔑 Checking secret access...")
    
    # These would be the expected secret keys based on the config
    secret_keys = ['azure-client-id', 'azure-client-secret']
    
    for key in secret_keys:
        try:
            # Try to check if secret exists (won't print the actual value)
            secret_manager.get_secret(key)
            print(f"   ✅ Secret '{key}' is accessible")
        except Exception as e:
            print(f"   ❌ Secret '{key}' not accessible: {e}")
            
except Exception as e:
    print(f"❌ Error checking secrets: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List files in storage (if accessible)

# COMMAND ----------

try:
    # Try to list files using dbutils if available
    storage_account = os.getenv('AZURE_SOURCE_STORAGE_ACCOUNT', 'agentstge')
    container = "raw-data"
    path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    
    print(f"📁 Attempting to list files in: {path}")
    
    try:
        files = dbutils.fs.ls(path)
        print(f"   ✅ Can access storage. Found {len(files)} items in root")
        
        # Check for reports directory
        reports_path = f"{path}reports/"
        try:
            excel_files = dbutils.fs.ls(reports_path)
            print(f"   ✅ Found {len(excel_files)} items in reports/")
            
            # List Excel files
            excel_count = 0
            for file in excel_files:
                if file.name.endswith('.xlsx') or file.name.endswith('.xls'):
                    print(f"      📄 {file.name} ({file.size} bytes)")
                    excel_count += 1
            
            if excel_count == 0:
                print("   ⚠️  No Excel files found in reports/")
                
        except Exception as e:
            print(f"   ❌ Cannot access reports/ directory: {e}")
            
    except Exception as e:
        print(f"   ❌ Cannot access storage: {e}")
        print("   This might be due to missing credentials or permissions")
        
except Exception as e:
    print(f"❌ Error listing files: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Recommendations

# COMMAND ----------

print("📋 Diagnostic Summary:")
print("\n1. Environment Setup:")
print(f"   - Project Code: {os.getenv('PROJECT_CODE', 'NOT SET')}")
print(f"   - Environment: {os.getenv('ENVIRONMENT', 'NOT SET')}")
print(f"   - Source Storage: {os.getenv('AZURE_SOURCE_STORAGE_ACCOUNT', 'NOT SET')}")
print(f"   - Data Lake Storage: {os.getenv('AZURE_DATALAKE_STORAGE_ACCOUNT', 'NOT SET')}")

print("\n2. Common Issues:")
print("   - If package not installed: Run 'databricks bundle deploy' or install wheel")
print("   - If environment variables not set: Check cluster configuration or CI/CD deployment")
print("   - If storage not accessible: Check Azure credentials and Key Vault secrets")
print("   - If no Excel files found: Upload Excel files to source storage account")

print("\n3. Next Steps:")
print("   - Ensure CI/CD has deployed with new storage variables")
print("   - Verify Excel files exist in agentstge/raw-data/reports/")
print("   - Check Azure Key Vault has required secrets")
print("   - Re-run Excel pipeline test after fixing issues")