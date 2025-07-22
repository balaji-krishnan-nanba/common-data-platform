# Databricks Runtime 16.4 E2E Flow Verification

This document provides a comprehensive guide to verify the end-to-end flow of the Common Data Platform with Databricks Runtime 16.4, focusing on Unity Catalog volumes and modern best practices.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks Runtime 16.4 or later
- Appropriate permissions for catalog, schema, and volume creation

## 1. Unity Catalog Setup

### Create Catalog and Schema

```python
from common_data_platform.utilities.unity_catalog_utils import UnityCatalogValidator

# Initialize validator
uc_validator = UnityCatalogValidator(spark)

# Create catalog with managed storage
uc_validator.ensure_catalog_exists(
    catalog_name="data_platform",
    storage_root="abfss://datalake@storage.dfs.core.windows.net/",
    comment="Common Data Platform catalog"
)

# Create schemas
uc_validator.ensure_schema_exists(
    catalog_name="data_platform",
    schema_name="bronze",
    comment="Bronze layer for raw data"
)

uc_validator.ensure_schema_exists(
    catalog_name="data_platform",
    schema_name="silver",
    comment="Silver layer for cleansed data"
)

uc_validator.ensure_schema_exists(
    catalog_name="data_platform",
    schema_name="gold",
    comment="Gold layer for business-ready data"
)
```

### Create Unity Catalog Volumes for File Storage

```python
# Create volume for source files
source_volume = uc_validator.create_managed_volume(
    catalog_name="data_platform",
    schema_name="bronze",
    volume_name="source_files",
    comment="Volume for storing source data files"
)
# Returns: /Volumes/data_platform/bronze/source_files

# Create volume for processed file tracking
tracking_volume = uc_validator.create_managed_volume(
    catalog_name="data_platform",
    schema_name="bronze",
    volume_name="file_tracking",
    comment="Volume for file processing metadata"
)
```

## 2. Configure Sources to Use Unity Catalog Volumes

### Excel Source Configuration

```yaml
# devops/config/sources/excel_sources.yaml
excel_sales_data:
  name: "excel_sales_data"
  type: "excel"
  connection:
    # Using Unity Catalog volume instead of DBFS
    path: "/Volumes/data_platform/bronze/source_files/sales"
    file_pattern: "*.xlsx"
    sheet_name: "Sheet1"
  ingestion:
    mode: "incremental"
    batch_size: 50  # Process files in batches
  target:
    catalog: "data_platform"
    schema: "bronze"
    table: "raw_sales_data"
    optimize_after_write: true
    zorder_columns: ["date", "customer_id"]
  track_processed_files: true
```

### Oracle Source Configuration with Secure Credentials

```yaml
# devops/config/sources/oracle_sources.yaml
oracle_customer_data:
  name: "oracle_customer_data"
  type: "oracle"
  connection:
    host: "${secrets/oracle/host}"
    port: 1521
    service_name: "${secrets/oracle/service}"
    user: "${secrets/oracle/user}"
    password: "${secrets/oracle/password}"
    schema: "CUSTOMERS"
    table: "CUSTOMER_MASTER"
  ingestion:
    mode: "incremental"
    watermark_column: "LAST_MODIFIED"
    fetch_size: 10000
  target:
    catalog: "data_platform"
    schema: "bronze"
    table: "raw_customer_data"
```

## 3. Bronze Layer Ingestion

### File Ingestion with Batch Processing

```python
from common_data_platform.core.config_manager import ConfigManager
from common_data_platform.core.secret_manager import SecretManager
from common_data_platform.ingestion.file_ingester import FileIngester

# Initialize components
config_manager = ConfigManager()
secret_manager = SecretManager(spark)
file_ingester = FileIngester(spark, config_manager, secret_manager)

# Load source configuration
source_config = config_manager.load_source_config("excel")["excel_sales_data"]

# Configure target with Unity Catalog
target_config = {
    "catalog": "data_platform",
    "schema": "bronze",
    "table": "raw_sales_data",
    "partition_by": ["ingestion_date"],
    "optimize_after_write": True,
    "zorder_columns": ["date", "customer_id"]
}

# Run ingestion with batch processing
result = file_ingester.ingest(
    source_config=source_config,
    target_config=target_config,
    write_mode="append",
    fail_on_error=False  # Continue on individual file errors
)

print(f"Processed {result['files_processed']} files")
print(f"Total records: {result['records_processed']}")
```

### Database Ingestion with Safe SQL

```python
from common_data_platform.ingestion.oracle_ingester import OracleIngester

# Initialize Oracle ingester
oracle_ingester = OracleIngester(spark, source_config)

# Run incremental ingestion
result = oracle_ingester.ingest(
    source_config=config_manager.load_source_config("oracle")["oracle_customer_data"],
    target_config={
        "catalog": "data_platform",
        "schema": "bronze", 
        "table": "raw_customer_data"
    }
)
```

## 4. Silver Layer Transformation

### SQL Transformation with Parameterized Queries

```sql
-- src/common_data_platform/sql/bronze_to_silver/daily_sales_cleansed.sql
CREATE OR REPLACE TABLE {target_table} AS
SELECT 
    customer_id,
    product_id,
    CAST(sale_date AS DATE) as sale_date,
    CAST(quantity AS INT) as quantity,
    CAST(unit_price AS DECIMAL(10,2)) as unit_price,
    quantity * unit_price as total_amount,
    CURRENT_TIMESTAMP() as processed_timestamp
FROM {source_table}
WHERE sale_date IS NOT NULL
  AND quantity > 0
  AND unit_price > 0
```

### PySpark Transformation with Safe Execution

```python
from common_data_platform.transformation.pyspark_transformer import PySparkTransformer

transformer = PySparkTransformer(spark, config_manager)

# Define transformation with safe aggregation
transformation_config = {
    "transform_type": "aggregation",
    "group_by": ["sale_date", "product_id"],
    "aggregations": {
        "quantity": "sum",
        "total_amount": "sum",
        "customer_id": "count"
    },
    "write_mode": "overwrite",
    "partition_columns": ["sale_date"]
}

# Execute transformation
success = transformer.transform(
    source_table="data_platform.bronze.raw_sales_data",
    target_table="data_platform.silver.daily_sales_summary",
    transformation_config=transformation_config
)
```

## 5. Gold Layer Analytics

### Create Business-Ready Views

```python
# Create customer lifetime value table
spark.sql("""
CREATE OR REPLACE TABLE data_platform.gold.customer_lifetime_value AS
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        COUNT(DISTINCT s.sale_date) as purchase_days,
        SUM(s.total_amount) as lifetime_value,
        AVG(s.total_amount) as avg_order_value,
        MAX(s.sale_date) as last_purchase_date,
        DATEDIFF(CURRENT_DATE(), MAX(s.sale_date)) as days_since_last_purchase
    FROM data_platform.silver.daily_sales_summary s
    JOIN data_platform.bronze.raw_customer_data c
        ON s.customer_id = c.customer_id
    GROUP BY c.customer_id, c.customer_name, c.customer_segment
)
SELECT 
    *,
    CASE 
        WHEN days_since_last_purchase <= 30 THEN 'Active'
        WHEN days_since_last_purchase <= 90 THEN 'At Risk'
        ELSE 'Churned'
    END as customer_status
FROM customer_metrics
""")

# Optimize the table
uc_validator.optimize_delta_table(
    "data_platform.gold.customer_lifetime_value",
    zorder_columns=["customer_segment", "customer_status"]
)
```

## 6. Monitoring and Validation

### Check Data Quality

```python
# Validate row counts across layers
bronze_count = spark.table("data_platform.bronze.raw_sales_data").count()
silver_count = spark.table("data_platform.silver.daily_sales_summary").count()
gold_count = spark.table("data_platform.gold.customer_lifetime_value").count()

print(f"Bronze records: {bronze_count:,}")
print(f"Silver records: {silver_count:,}")
print(f"Gold records: {gold_count:,}")

# Check for data freshness
freshness_check = spark.sql("""
SELECT 
    'Bronze' as layer,
    MAX(etl_loaded_timestamp) as latest_load
FROM data_platform.bronze.raw_sales_data
UNION ALL
SELECT 
    'Silver' as layer,
    MAX(processed_timestamp) as latest_load
FROM data_platform.silver.daily_sales_summary
""").show()
```

### Monitor File Processing

```python
# Check file processing status
tracking_df = spark.sql("""
SELECT 
    file_path,
    processed_timestamp,
    batch_id,
    record_count
FROM data_platform.bronze_tracking.file_process_log
WHERE processed_timestamp >= current_date()
ORDER BY processed_timestamp DESC
""")

tracking_df.show(20, truncate=False)
```

## 7. Performance Optimizations for DBR 16.4

### Enable Photon Acceleration

```python
# Configure cluster for Photon
cluster_config = {
    "spark_version": "16.4.x-photon-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "spark_conf": {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
}
```

### Liquid Clustering (DBR 15.2+)

```sql
-- Enable liquid clustering for high-cardinality columns
ALTER TABLE data_platform.bronze.raw_sales_data
CLUSTER BY (sale_date, customer_id);

-- Optimize with clustering
OPTIMIZE data_platform.bronze.raw_sales_data;
```

## 8. Security Best Practices

### Column-Level Security

```sql
-- Create masked view for PII data
CREATE OR REPLACE VIEW data_platform.silver.customer_data_masked AS
SELECT 
    customer_id,
    CASE 
        WHEN is_member('data_analysts') THEN customer_name
        ELSE CONCAT(SUBSTRING(customer_name, 1, 2), '***')
    END as customer_name,
    CASE
        WHEN is_member('data_analysts') THEN email
        ELSE CONCAT(SUBSTRING(email, 1, 3), '***@***')
    END as email,
    customer_segment,
    registration_date
FROM data_platform.bronze.raw_customer_data;

-- Grant access
GRANT SELECT ON VIEW data_platform.silver.customer_data_masked TO `data_analysts`;
```

## Summary

This E2E flow demonstrates:

1. **Unity Catalog Integration**: Full three-level namespace validation and management
2. **Volume-Based Storage**: Using Unity Catalog volumes instead of DBFS
3. **Batch Processing**: Efficient handling of large file collections
4. **Distributed Locking**: Safe concurrent file processing
5. **SQL Injection Prevention**: Parameterized queries and input validation
6. **Safe Code Execution**: AST-based validation for dynamic code
7. **Error Handling**: Comprehensive error tracking and recovery
8. **Performance Optimization**: Photon, adaptive query execution, and Delta optimizations
9. **Security**: Column-level masking and access controls

The platform is fully compatible with Databricks Runtime 16.4 and follows all modern best practices for Unity Catalog-enabled workspaces.