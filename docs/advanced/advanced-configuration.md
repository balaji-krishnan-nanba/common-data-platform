# Advanced Configuration

This guide covers advanced configuration patterns and customizations.

## Custom Connectors

### Creating a New Connector

1. **Inherit from BaseConnector**
   ```python
   from src.connectivity.base_connector import BaseConnector
   
   class CustomConnector(BaseConnector):
       def connect(self):
           # Implementation
           pass
   ```

2. **Create Service Class**
   ```python
   # Create new service following ADLSService/OracleService pattern
   class CustomService:
       def __init__(self, spark, config, secret_manager):
           # Initialize service
   ```

## Advanced Source Patterns

### Dynamic Path Resolution

Use date patterns in file paths:

```yaml
connection:
  path_pattern: "data/{year}/{month}/{day}/file_{date}.xlsx"
```

Supported placeholders:
- `{year}` - Current year (2024)
- `{month}` - Current month (01-12)
- `{day}` - Current day (01-31)
- `{date}` - Current date (20240115)

### Complex Schema Validation

```yaml
schema:
  strict_validation: true
  columns:
    - name: amount
      type: decimal(10,2)
      constraints:
        min: 0
        max: 1000000
      transformations:
        - type: round
          decimals: 2
    - name: email
      type: string
      pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

## Environment-Specific Overrides

### Development Environment

```yaml
# devops/environments/dev.yml
variables:
  # Debug settings
  enable_debug_logging: true
  validation_enabled: true
  
  # Cost optimization
  ingestion_workers: 0
  autotermination_minutes: 30
```

### Production Environment

```yaml
# devops/environments/prod.yml
variables:
  # Performance settings
  ingestion_workers: 4
  autotermination_minutes: 120
  
  # Monitoring
  enable_notifications: true
  alert_on_failure: true
```

## Custom Transformations

### PySpark Transformations

```python
# src/transformations/custom_cleansing.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_replace

def clean_customer_data(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "phone_cleaned",
        regexp_replace(col("phone"), "[^0-9]", "")
    ).withColumn(
        "email_domain",
        split(col("email"), "@").getItem(1)
    )
```

### SQL Transformations

```sql
-- src/sql/custom/data_quality_checks.sql
CREATE OR REPLACE VIEW data_quality_summary AS
SELECT 
    table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN data_quality_flag = 'VALID' THEN 1 END) as valid_records,
    ROUND(COUNT(CASE WHEN data_quality_flag = 'VALID' THEN 1 END) * 100.0 / COUNT(*), 2) as quality_percentage
FROM ${catalog}.${schema}.daily_sales_cleansed
GROUP BY table_name;
```

## Performance Optimization

### Cluster Configuration

```yaml
# Optimized for large files
job_clusters:
  - job_cluster_key: large_file_cluster
    new_cluster:
      spark_version: "16.4.x-scala2.12"
      node_type_id: "Standard_DS4_v2"
      num_workers: 4
      spark_conf:
        "spark.sql.adaptive.enabled": "true"
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
        "spark.sql.adaptive.skewJoin.enabled": "true"
```

### Partitioning Strategy

```yaml
target:
  partition_columns: ["year", "month", "source_system"]
  partition_overwrite_mode: "dynamic"
```

## Monitoring and Alerting

### Custom Metrics

```python
# Custom metrics collection
from src.utilities.logger import DataPipelineLogger

logger = DataPipelineLogger("custom_metrics")
logger.log_metric("records_processed", record_count)
logger.log_metric("processing_time_seconds", execution_time)
```

### Alert Configuration

```yaml
# Webhook alerts
monitoring:
  alerts:
    - type: "failure"
      webhook_url: "${slack_webhook_url}"
      message_template: "Pipeline {pipeline_name} failed in {environment}"
    - type: "data_quality"
      threshold: 0.95
      metric: "quality_percentage"
```