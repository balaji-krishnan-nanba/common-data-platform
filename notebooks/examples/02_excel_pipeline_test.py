# Databricks notebook source
# MAGIC %md
# MAGIC # Excel Pipeline Test - Parameterized
# MAGIC 
# MAGIC This notebook demonstrates how to run a complete Excel ingestion pipeline from Bronze to Silver layer.
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog setup completed (run `unity_catalog_setup.py` first)
# MAGIC - Excel file available in Azure Storage
# MAGIC - Run on a Unity Catalog enabled cluster
# MAGIC 
# MAGIC **Parameters:**
# MAGIC - `project_code`: Project identifier (default from env)
# MAGIC - `environment`: Target environment (default from env)
# MAGIC 
# MAGIC **What this demonstrates:**
# MAGIC - Installing the deployed Python wheel package
# MAGIC - Running bronze layer ingestion from Excel file
# MAGIC - Running silver layer transformation with data quality checks
# MAGIC - Verifying results and data quality metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set Parameters

# COMMAND ----------

import os

# Define parameters with defaults from environment variables
default_project_code = os.getenv('PROJECT_CODE', 'cddp')
default_environment = os.getenv('ENVIRONMENT', 'dev')

# Get storage account from environment variable
storage_account = os.getenv('AZURE_STORAGE_ACCOUNT', "")

dbutils.widgets.text("project_code", default_project_code, "Project Code (4-letter identifier)")
dbutils.widgets.dropdown("environment", default_environment, ["dev", "test", "prod"], "Environment")

# Get parameter values
project_code = dbutils.widgets.get("project_code")
environment = dbutils.widgets.get("environment")

# Validate that storage account is provided
if not storage_account:
    raise ValueError(f"Storage account not provided. Please set environment variable AZURE_STORAGE_ACCOUNT")

print(f"🔧 Configuration:")
print(f"   Project Code: {project_code}")
print(f"   Environment: {environment}")
print(f"   Storage Account: {storage_account}")
print(f"   Source: Environment variable AZURE_STORAGE_ACCOUNT")

# Set environment variables for the framework
os.environ['PROJECT_CODE'] = project_code
os.environ['ENVIRONMENT'] = environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Install Framework Package

# COMMAND ----------

# Install the Common Data Platform wheel package
%pip install /dbfs/FileStore/jars/common_data_platform-0.1.0-py3-none-any.whl --force-reinstall

# Restart Python to use the new package
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Installation and Setup

# COMMAND ----------

# Re-get parameters after restart (import os already done above)
project_code = dbutils.widgets.get("project_code")
environment = dbutils.widgets.get("environment")
# Get storage account from environment variable
storage_account = os.getenv('AZURE_STORAGE_ACCOUNT', "")

# Set environment variables for the framework
os.environ['PROJECT_CODE'] = project_code
os.environ['ENVIRONMENT'] = environment

# Import the framework modules
from src.core.config_manager import ConfigManager
from src.core.secret_manager import SecretManager
from src.ingestion.file_ingester import FileIngester
from pyspark.sql import SparkSession
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize configuration manager
config_manager = ConfigManager()

print(f"✅ Framework installed successfully")
print(f"📋 Project Code: {config_manager.project_code}")
print(f"🌍 Environment: {config_manager.environment}")
print(f"📚 Bronze Catalog: {config_manager.get_catalog_name('bronze')}")
print(f"🥈 Silver Catalog: {config_manager.get_catalog_name('silver')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Unity Catalog Structure

# COMMAND ----------

# Verify catalogs exist using parameters
catalog_pattern = f"{project_code}-{environment}-*"
spark.sql(f"SHOW CATALOGS LIKE '{catalog_pattern}'").display()

# COMMAND ----------

# Verify schemas exist using parameters
schema_query = f"""
SELECT 
    catalog_name,
    schema_name,
    schema_comment
FROM information_schema.schemata 
WHERE catalog_name LIKE '{project_code}-{environment}-%'
ORDER BY catalog_name, schema_name
"""
spark.sql(schema_query).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Load Excel Source Configuration

# COMMAND ----------

# Load Excel source configuration
excel_config = config_manager.load_source_config("excel")

# Display the daily_sales_excel configuration
if "daily_sales_excel" in excel_config:
    daily_sales_config = excel_config["daily_sales_excel"]
    print("📊 Daily Sales Excel Configuration:")
    print(f"   Source: {daily_sales_config['connection']['storage_account']}")
    print(f"   Container: {daily_sales_config['connection']['container']}")
    print(f"   Path Pattern: {daily_sales_config['connection']['path_pattern']}")
    print(f"   Target: {daily_sales_config['target']['catalog']}.{daily_sales_config['target']['schema']}.{daily_sales_config['target']['table']}")
else:
    print("❌ Daily sales Excel configuration not found")
    print("Available sources:", list(excel_config.keys()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Run Bronze Layer Ingestion

# COMMAND ----------

# Initialize File ingestor
spark = SparkSession.builder.appName("Excel Pipeline Test").getOrCreate()
secret_manager = SecretManager(spark)
ingestor = FileIngester(spark, config_manager, secret_manager)

print("🔄 Starting Bronze layer ingestion...")

try:
    # Load source configuration
    source_config = daily_sales_config
    
    # Configure target
    target_config = {
        'catalog': f'{project_code}-{environment}-bronze',
        'schema': 'excel_data',
        'table': 'daily_sales'
    }
    
    # Run ingestion for the daily sales Excel file
    result = ingestor.ingest(
        source_config=source_config,
        target_config=target_config
    )
    
    print(f"✅ Bronze ingestion completed successfully!")
    print(f"📊 Records processed: {result.get('records_processed', 'N/A')}")
    print(f"📁 Target table: {result.get('target_table', 'N/A')}")
    
except Exception as e:
    print(f"❌ Bronze ingestion failed: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Bronze Data

# COMMAND ----------

# Check bronze data using parameters
bronze_table = f"`{project_code}-{environment}-bronze`.`excel_data`.`daily_sales`"
bronze_query = f"""
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT sale_date) as unique_dates,
    MIN(sale_date) as earliest_date,
    MAX(sale_date) as latest_date,
    SUM(amount) as total_amount
FROM {bronze_table}
"""
spark.sql(bronze_query).display()

# COMMAND ----------

# Sample bronze records
bronze_sample_query = f"""
SELECT * 
FROM {bronze_table}
LIMIT 10
"""
spark.sql(bronze_sample_query).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Bronze Data Successfully Ingested
# MAGIC 
# MAGIC The bronze layer ingestion is complete. For silver layer transformations, 
# MAGIC you would use the CLI command or implement transformations as needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Check System Tracking Tables

# COMMAND ----------

# Check processed files tracking using parameters
system_catalog = f"`{project_code}-{environment}-bronze`.`system`"
processed_files_query = f"""
SELECT 
    file_path,
    source_name,
    processed_timestamp,
    records_processed,
    batch_id
FROM {system_catalog}.`processed_files`
ORDER BY processed_timestamp DESC
"""
spark.sql(processed_files_query).display()

# COMMAND ----------

# Check pipeline execution history using parameters
pipeline_executions_query = f"""
SELECT 
    pipeline_name,
    start_time,
    end_time,
    status,
    records_processed,
    environment
FROM {system_catalog}.`pipeline_executions`
ORDER BY start_time DESC
"""
spark.sql(pipeline_executions_query).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Pipeline Test Complete!
# MAGIC 
# MAGIC **Summary of what we accomplished:**
# MAGIC - ✅ Installed the Common Data Platform framework
# MAGIC - ✅ Verified Unity Catalog structure
# MAGIC - ✅ Loaded Excel source configuration
# MAGIC - ✅ Ingested Excel data to Bronze layer
# MAGIC - ✅ Verified data ingestion and completeness
# MAGIC - ✅ Tracked pipeline execution in system tables
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. **Add more Excel files** - Drop additional files in the storage path pattern
# MAGIC 2. **Set up incremental processing** - Framework will automatically detect new files
# MAGIC 3. **Configure Databricks Jobs** - Schedule this pipeline to run automatically
# MAGIC 4. **Add Gold layer transformations** - Create business-ready analytics tables
# MAGIC 5. **Set up monitoring** - Use the system tables for pipeline monitoring
# MAGIC 
# MAGIC **Key Tables Created:**
# MAGIC - Bronze data table - Raw Excel data
# MAGIC - Silver data table - Cleansed and validated data  
# MAGIC - System tracking tables - File processing and pipeline execution history

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Manual File Processing
# MAGIC 
# MAGIC If you want to process a different Excel file manually:

# COMMAND ----------

# Example: Process a different Excel file
# Uncomment and modify the path below:

# # Example: Process a different Excel file
# different_source_config = daily_sales_config.copy()
# different_target_config = target_config.copy()
# 
# different_file_result = ingestor.ingest(
#     source_config=different_source_config,
#     target_config=different_target_config
# )
# print(f"Processed {different_file_result.get('records_processed', 0)} records from different file")

print("📝 To process different files, uncomment and modify the code above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting
# MAGIC 
# MAGIC **Common Issues:**
# MAGIC 
# MAGIC 1. **Package not found**: Make sure the wheel file is deployed to DBFS
# MAGIC 2. **Catalog/Schema not found**: Run the Unity Catalog setup notebook first
# MAGIC 3. **File not found**: Verify the Excel file exists in Azure Storage
# MAGIC 4. **Permission denied**: Check Azure Storage access and Key Vault secrets
# MAGIC 
# MAGIC **Debug Commands:**
# MAGIC ```python
# MAGIC # Check if package is installed
# MAGIC import pkg_resources
# MAGIC [pkg.project_name for pkg in pkg_resources.working_set if 'common_data_platform' in pkg.project_name]
# MAGIC 
# MAGIC # Check environment variables
# MAGIC import os
# MAGIC print(f"PROJECT_CODE: {os.getenv('PROJECT_CODE')}")
# MAGIC print(f"ENVIRONMENT: {os.getenv('ENVIRONMENT')}")
# MAGIC ```