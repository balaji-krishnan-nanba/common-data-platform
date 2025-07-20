# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Setup - Parameterized
# MAGIC
# MAGIC This notebook sets up Unity Catalog structure for any environment using parameters.
# MAGIC It can be run with different parameters for dev, test, and prod environments.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run this notebook on a Unity Catalog enabled cluster
# MAGIC - Ensure you have catalog creation permissions
# MAGIC - Pass parameters: project_code, environment, storage_account
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `project_code`: Project identifier (default: 'cddp')
# MAGIC - `environment`: Target environment ('dev', 'test', 'prod')
# MAGIC - `storage_account`: Azure Data Lake Storage account name

# COMMAND ----------

import os

# Define parameters with defaults - get from environment variables
default_project_code = os.getenv('PROJECT_CODE', 'cddp')
default_environment = os.getenv('ENVIRONMENT', 'dev')

# Get storage account from environment variable
storage_account = os.getenv('AZURE_STORAGE_ACCOUNT', "")
#if not default_storage_account and default_environment:
#    # Fall back to environment-specific variable for backward compatibility
#    storage_var = f"AZURE_STORAGE_ACCOUNT_{default_environment.upper()}"
#    default_storage_account = os.getenv(storage_var, "")

dbutils.widgets.text("project_code", default_project_code, "Project Code (4-letter identifier)")
dbutils.widgets.dropdown("environment", default_environment, ["dev", "test", "prod"], "Environment")
#dbutils.widgets.text("storage_account", default_storage_account, "Azure Data Lake Storage Account")

# Get parameter values
project_code = dbutils.widgets.get("project_code")
environment = dbutils.widgets.get("environment")
#storage_account = dbutils.widgets.get("storage_account")

# Validate that storage account is provided
if not storage_account:
    storage_var = f"AZURE_STORAGE_ACCOUNT_{environment.upper()}"
    raise ValueError(f"Storage account not provided. Please set parameter or environment variable AZURE_STORAGE_ACCOUNT or {storage_var}")

print(f"🔧 Configuration:")
print(f"   Project Code: {project_code}")
print(f"   Environment: {environment}")
print(f"   Storage Account: {storage_account}")
# Show which environment variable was used
source_var = "AZURE_STORAGE_ACCOUNT" if os.getenv('AZURE_STORAGE_ACCOUNT') else f"AZURE_STORAGE_ACCOUNT_{environment.upper()}"
print(f"   Source: Environment variable {source_var}")

# Catalog names
bronze_catalog = f"{project_code}-{environment}-bronze"
silver_catalog = f"{project_code}-{environment}-silver"
gold_catalog = f"{project_code}-{environment}-gold"

# Storage URLs
bronze_url = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
silver_url = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
gold_url = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

print(f"\n📚 Catalogs to create:")
print(f"   Bronze: {bronze_catalog} → {bronze_url}")
print(f"   Silver: {silver_catalog} → {silver_url}")
print(f"   Gold: {gold_catalog} → {gold_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create External Locations for Data Lake Storage

# COMMAND ----------

# Check if external locations exist before creating
try:
    existing_locations = spark.sql("SHOW EXTERNAL LOCATIONS").collect()
    location_names = [loc.location_name for loc in existing_locations]
except:
    location_names = []
    print("⚠️ Could not retrieve existing external locations")

# Create Bronze external location
bronze_location_name = f"{project_code}_{environment}_bronze_location"
if bronze_location_name not in location_names:
    try:
        spark.sql(f"""
        CREATE EXTERNAL LOCATION `{bronze_location_name}`
        URL '{bronze_url}'
        WITH (STORAGE_CREDENTIAL `databricks_managed_identity`)
        COMMENT 'Bronze layer external location for {environment} environment'
        """)
        print(f"✅ Created external location: {bronze_location_name}")
    except Exception as e:
        print(f"❌ Failed to create bronze external location: {str(e)}")
else:
    print(f"⚠️ External location already exists: {bronze_location_name}")

# Create Silver external location
silver_location_name = f"{project_code}_{environment}_silver_location"
if silver_location_name not in location_names:
    try:
        spark.sql(f"""
        CREATE EXTERNAL LOCATION `{silver_location_name}`
        URL '{silver_url}'
        WITH (STORAGE_CREDENTIAL `databricks_managed_identity`)
        COMMENT 'Silver layer external location for {environment} environment'
        """)
        print(f"✅ Created external location: {silver_location_name}")
    except Exception as e:
        print(f"❌ Failed to create silver external location: {str(e)}")
else:
    print(f"⚠️ External location already exists: {silver_location_name}")

# Create Gold external location
gold_location_name = f"{project_code}_{environment}_gold_location"
if gold_location_name not in location_names:
    try:
        spark.sql(f"""
        CREATE EXTERNAL LOCATION `{gold_location_name}`
        URL '{gold_url}'
        WITH (STORAGE_CREDENTIAL `databricks_managed_identity`)
        COMMENT 'Gold layer external location for {environment} environment'
        """)
        print(f"✅ Created external location: {gold_location_name}")
    except Exception as e:
        print(f"❌ Failed to create gold external location: {str(e)}")
else:
    print(f"⚠️ External location already exists: {gold_location_name}")

print("\n✅ External locations setup completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Bronze Catalog

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS `{bronze_catalog}`
MANAGED LOCATION '{bronze_url}'
COMMENT 'Bronze layer - raw data ingestion for {environment} environment'
""")

# Verify creation
bronze_info = spark.sql(f"DESCRIBE CATALOG `{bronze_catalog}`").collect()
print(f"✅ Bronze catalog created: {bronze_catalog}")
for row in bronze_info:
    print(f"   {row['info_name']}: {row['info_value']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Silver Catalog

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS `{silver_catalog}`
MANAGED LOCATION '{silver_url}'
COMMENT 'Silver layer - cleansed and conformed data for {environment} environment'
""")

# Verify creation
silver_info = spark.sql(f"DESCRIBE CATALOG `{silver_catalog}`").collect()
print(f"✅ Silver catalog created: {silver_catalog}")
for row in silver_info:
    print(f"   {row['info_name']}: {row['info_value']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Gold Catalog

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS `{gold_catalog}`
MANAGED LOCATION '{gold_url}'
COMMENT 'Gold layer - analytics-ready data for {environment} environment'
""")

# Verify creation
gold_info = spark.sql(f"DESCRIBE CATALOG `{gold_catalog}`").collect()
print(f"✅ Gold catalog created: {gold_catalog}")
for row in gold_info:
    print(f"   {row['info_name']}: {row['info_value']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Schemas in Bronze Catalog

# COMMAND ----------

# Create schemas for different source types
schemas_bronze = [
    ("excel_data", "Excel file ingestion data in bronze layer"),
    ("csv_data", "CSV file ingestion data in bronze layer"),
    ("oracle_data", "Oracle database ingestion data in bronze layer"),
    ("system", "System tables for pipeline tracking and metadata")
]

for schema_name, comment in schemas_bronze:
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `{bronze_catalog}`.{schema_name}
    COMMENT '{comment}'
    """)
    print(f"✅ Created schema: {bronze_catalog}.{schema_name}")

# Show all schemas in bronze catalog
bronze_schemas = spark.sql(f"SHOW SCHEMAS IN `{bronze_catalog}`").collect()
print(f"\n📋 Schemas in {bronze_catalog}:")
for schema in bronze_schemas:
    print(f"   - {schema['databaseName']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Schemas in Silver Catalog

# COMMAND ----------

# Create schemas for cleansed data
schemas_silver = [
    ("excel_data", "Cleansed Excel data in silver layer"),
    ("csv_data", "Cleansed CSV data in silver layer"), 
    ("oracle_data", "Cleansed Oracle data in silver layer")
]

for schema_name, comment in schemas_silver:
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `{silver_catalog}`.{schema_name}
    COMMENT '{comment}'
    """)
    print(f"✅ Created schema: {silver_catalog}.{schema_name}")

# Show all schemas in silver catalog
silver_schemas = spark.sql(f"SHOW SCHEMAS IN `{silver_catalog}`").collect()
print(f"\n📋 Schemas in {silver_catalog}:")
for schema in silver_schemas:
    print(f"   - {schema['databaseName']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Schemas in Gold Catalog

# COMMAND ----------

# Create schemas for analytics and reporting
schemas_gold = [
    ("analytics", "Analytics-ready data in gold layer"),
    ("reporting", "Reporting datasets in gold layer"),
    ("customer_360", "Customer 360 analytical views"),
    ("sales_analytics", "Sales analytics and KPIs")
]

for schema_name, comment in schemas_gold:
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `{gold_catalog}`.{schema_name}
    COMMENT '{comment}'
    """)
    print(f"✅ Created schema: {gold_catalog}.{schema_name}")

# Show all schemas in gold catalog
gold_schemas = spark.sql(f"SHOW SCHEMAS IN `{gold_catalog}`").collect()
print(f"\n📋 Schemas in {gold_catalog}:")
for schema in gold_schemas:
    print(f"   - {schema['databaseName']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create System Tracking Tables

# COMMAND ----------

# Create processed files tracking table for incremental loading
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.system.processed_files (
    file_path STRING NOT NULL COMMENT 'Full path to the processed file',
    source_name STRING NOT NULL COMMENT 'Source configuration name',
    processed_timestamp TIMESTAMP NOT NULL COMMENT 'When the file was processed',
    file_size_bytes BIGINT COMMENT 'Size of the processed file in bytes',
    record_count BIGINT COMMENT 'Number of records processed from the file',
    checksum STRING COMMENT 'File checksum for integrity verification',
    processing_duration_seconds DOUBLE COMMENT 'Time taken to process the file',
    status STRING COMMENT 'Processing status (success, failed, partial)',
    error_message STRING COMMENT 'Error message if processing failed',
    batch_id STRING COMMENT 'Batch identifier for the processing run',
    environment STRING COMMENT 'Environment where processing occurred',
    created_by STRING COMMENT 'User or service that processed the file'
) USING DELTA
PARTITIONED BY (source_name, DATE(processed_timestamp))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Tracks processed files for incremental loading and audit purposes'
""")

print(f"✅ Created table: {bronze_catalog}.system.processed_files")

# COMMAND ----------

# Create pipeline execution tracking table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.system.pipeline_executions (
    pipeline_id STRING NOT NULL COMMENT 'Unique pipeline execution identifier',
    pipeline_name STRING NOT NULL COMMENT 'Name of the pipeline',
    status STRING NOT NULL COMMENT 'Pipeline execution status (success, failed, running)',
    start_time TIMESTAMP NOT NULL COMMENT 'Pipeline start timestamp',
    end_time TIMESTAMP COMMENT 'Pipeline end timestamp',
    duration_seconds DOUBLE COMMENT 'Total execution duration in seconds',
    records_processed BIGINT COMMENT 'Total records processed',
    files_processed INT COMMENT 'Number of files processed',
    error_message STRING COMMENT 'Error message if pipeline failed',
    source_name STRING COMMENT 'Source configuration name',
    target_layer STRING COMMENT 'Target layer (bronze, silver, gold)',
    batch_id STRING COMMENT 'Batch identifier for the processing run',
    environment STRING NOT NULL COMMENT 'Environment (dev, test, prod)',
    user_name STRING COMMENT 'User who triggered the pipeline',
    cluster_id STRING COMMENT 'Databricks cluster ID used for execution'
) USING DELTA
PARTITIONED BY (DATE(start_time), environment)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Tracks pipeline execution history for monitoring and debugging'
""")

print(f"✅ Created table: {bronze_catalog}.system.pipeline_executions")

# COMMAND ----------

# Create watermarks table for incremental loads
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.system.watermarks (
    source_name STRING NOT NULL COMMENT 'Source configuration name',
    table_name STRING NOT NULL COMMENT 'Target table name',
    watermark_column STRING NOT NULL COMMENT 'Column used for watermarking',
    watermark_value STRING NOT NULL COMMENT 'Current watermark value',
    last_updated TIMESTAMP NOT NULL COMMENT 'When watermark was last updated',
    environment STRING NOT NULL COMMENT 'Environment (dev, test, prod)'
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Tracks watermarks for incremental data loading'
""")

print(f"✅ Created table: {bronze_catalog}.system.watermarks")

# COMMAND ----------

# Create data quality results table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.system.data_quality_results (
    check_id STRING NOT NULL COMMENT 'Unique identifier for the quality check',
    table_name STRING NOT NULL COMMENT 'Table being checked',
    check_type STRING NOT NULL COMMENT 'Type of quality check (null_check, range_check, etc.)',
    check_name STRING NOT NULL COMMENT 'Name of the quality check',
    status STRING NOT NULL COMMENT 'Check result (passed, failed, warning)',
    result_details STRING COMMENT 'Detailed results or error information',
    records_checked BIGINT COMMENT 'Number of records checked',
    records_failed BIGINT COMMENT 'Number of records that failed the check',
    check_timestamp TIMESTAMP NOT NULL COMMENT 'When the check was performed',
    batch_id STRING COMMENT 'Batch identifier for the processing run',
    environment STRING COMMENT 'Environment (dev, test, prod)'
) USING DELTA
PARTITIONED BY (DATE(check_timestamp))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Tracks data quality check results for monitoring and compliance'
""")

print(f"✅ Created table: {bronze_catalog}.system.data_quality_results")

# COMMAND ----------

# Create stage executions table for granular logging
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.system.stage_executions (
    pipeline_id STRING NOT NULL COMMENT 'Pipeline execution identifier',
    stage_name STRING NOT NULL COMMENT 'Name of the pipeline stage',
    status STRING NOT NULL COMMENT 'Stage execution status (success, failed, running)',
    start_time TIMESTAMP NOT NULL COMMENT 'Stage start timestamp',
    end_time TIMESTAMP COMMENT 'Stage end timestamp',
    duration_seconds DOUBLE COMMENT 'Stage execution duration in seconds',
    records_processed BIGINT COMMENT 'Number of records processed in this stage',
    stage_details STRING COMMENT 'Additional stage-specific details',
    error_message STRING COMMENT 'Error message if stage failed',
    environment STRING NOT NULL COMMENT 'Environment (dev, test, prod)'
) USING DELTA
PARTITIONED BY (DATE(start_time), environment)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Tracks individual stage executions within pipelines for detailed monitoring'
""")

print(f"✅ Created table: {bronze_catalog}.system.stage_executions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Verify Complete Structure

# COMMAND ----------

# Show all catalogs for this project and environment
project_catalogs = spark.sql(f"SHOW CATALOGS LIKE '{project_code}-{environment}-*'").collect()
print(f"📚 Catalogs for {project_code}-{environment}:")
for catalog in project_catalogs:
    print(f"   - {catalog['catalog']}")

# COMMAND ----------

# Show complete catalog and schema structure
structure_df = spark.sql(f"""
SELECT 
    catalog_name,
    schema_name,
    schema_comment
FROM information_schema.schemata 
WHERE catalog_name LIKE '{project_code}-{environment}-%'
ORDER BY catalog_name, schema_name
""")

print(f"🏗️ Complete structure for {project_code}-{environment}:")
structure = structure_df.collect()
for row in structure:
    print(f"   {row['catalog_name']}.{row['schema_name']} - {row['schema_comment']}")

# COMMAND ----------

# Show system tables created
system_tables = spark.sql(f"SHOW TABLES IN `{bronze_catalog}`.system").collect()
print(f"🛠️ System tables in {bronze_catalog}.system:")
for table in system_tables:
    print(f"   - {table['tableName']}")

print(f"\n📊 System Tables Summary:")
print(f"   • processed_files - File processing tracking")
print(f"   • pipeline_executions - Pipeline execution history")
print(f"   • watermarks - Incremental load watermarks")
print(f"   • data_quality_results - Data quality check results")
print(f"   • stage_executions - Granular stage-level tracking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC
# MAGIC **Unity Catalog structure created for environment: {environment}**

# COMMAND ----------

# Print final summary
print(f"""
🎉 Unity Catalog Setup Complete!

Environment: {environment}
Project Code: {project_code}
Storage Account: {storage_account}

📚 Catalogs Created:
   - {bronze_catalog} → {bronze_url}
   - {silver_catalog} → {silver_url}  
   - {gold_catalog} → {gold_url}

📁 Schemas Created:
   Bronze: excel_data, csv_data, oracle_data, system
   Silver: excel_data, csv_data, oracle_data
   Gold: analytics, reporting, customer_360, sales_analytics

🛠️ System Tables:
   - {bronze_catalog}.system.processed_files - File processing tracking
   - {bronze_catalog}.system.pipeline_executions - Pipeline execution history  
   - {bronze_catalog}.system.watermarks - Incremental load watermarks
   - {bronze_catalog}.system.data_quality_results - Data quality check results
   - {bronze_catalog}.system.stage_executions - Granular stage-level tracking

✨ Next Steps:
   1. Run Excel pipeline test notebook
   2. Configure data sources in devops/config/sources/
   3. Deploy Databricks workflows for scheduled processing
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Grant Permissions (if needed)
# MAGIC
# MAGIC If you need to grant access to other users or service principals:

# COMMAND ----------

# Example permissions (uncomment and modify as needed)
print("""
# Example permission commands (uncomment and modify as needed):

# Grant usage on catalogs
# GRANT USAGE ON CATALOG `{bronze_catalog}` TO `your-service-principal`;
# GRANT USAGE ON CATALOG `{silver_catalog}` TO `your-service-principal`;
# GRANT USAGE ON CATALOG `{gold_catalog}` TO `your-service-principal`;

# Grant schema permissions
# GRANT ALL PRIVILEGES ON SCHEMA `{bronze_catalog}`.excel_data TO `your-service-principal`;
# GRANT ALL PRIVILEGES ON SCHEMA `{silver_catalog}`.excel_data TO `your-service-principal`;
# GRANT ALL PRIVILEGES ON SCHEMA `{gold_catalog}`.analytics TO `your-service-principal`;
""".format(
    bronze_catalog=bronze_catalog,
    silver_catalog=silver_catalog,
    gold_catalog=gold_catalog
))

print("📝 Modify and run the permission commands above if needed for your environment.")
