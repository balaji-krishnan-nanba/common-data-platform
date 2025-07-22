# Transformation Configuration Guide

This guide covers configuring data transformations between medallion layers (Bronze → Silver → Gold) using both PySpark and SparkSQL approaches.

## Overview

The framework supports flexible transformations through:
- **SparkSQL Transformations**: Declarative SQL-based transformations
- **PySpark Transformations**: Python-based complex business logic
- **SCD Type 2**: Built-in slowly changing dimension support
- **Data Quality**: Integrated validation and quality checks

## Transformation Configuration Structure

```
config/transformations/
├── bronze_to_silver/           # Bronze to Silver transformations
│   ├── excel_transformations.yaml
│   ├── csv_transformations.yaml
│   └── oracle_transformations.yaml
└── silver_to_gold/            # Silver to Gold transformations
    ├── customer_360.yaml
    ├── sales_analytics.yaml
    └── operational_metrics.yaml
```

## Basic Transformation Configuration

### Simple SparkSQL Transformation

```yaml
# config/transformations/bronze_to_silver/sales_cleansing.yaml
sales_data_cleansing:
  source:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: daily_sales
    
  target:
    catalog: ${project_code}-${environment}-silver
    schema: excel_data
    table: sales_cleansed
    
  engine: sparksql
  sql_file: sql/bronze_to_silver/sales_cleansing.sql
  
  transformations:
    - type: data_quality_checks
      checks:
        - type: null_check
          columns: [transaction_id, customer_id]
          threshold: 0
        - type: duplicate_check
          columns: [transaction_id]
          threshold: 0
          
    - type: standardization
      rules:
        payment_method:
          - from: ["CC", "Credit Card", "CREDIT"]
            to: "Credit Card"
          - from: ["DC", "Debit Card", "DEBIT"] 
            to: "Debit Card"
            
  write_mode: overwrite
  partition_by: [transaction_year, transaction_month]
```

### Corresponding SQL File

```sql
-- src/sql/bronze_to_silver/sales_cleansing.sql
SELECT 
    transaction_id,
    transaction_date,
    customer_id,
    product_code,
    quantity,
    unit_price,
    total_amount,
    
    -- Standardize payment methods
    CASE 
        WHEN UPPER(payment_method) IN ('CC', 'CREDIT CARD', 'CREDIT') THEN 'Credit Card'
        WHEN UPPER(payment_method) IN ('DC', 'DEBIT CARD', 'DEBIT') THEN 'Debit Card'
        WHEN UPPER(payment_method) IN ('CASH') THEN 'Cash'
        ELSE payment_method
    END as payment_method_clean,
    
    -- Add derived columns
    YEAR(transaction_date) as transaction_year,
    MONTH(transaction_date) as transaction_month,
    QUARTER(transaction_date) as transaction_quarter,
    
    CASE 
        WHEN total_amount >= 1000 THEN 'High Value'
        WHEN total_amount >= 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END as revenue_category,
    
    -- Audit columns
    current_timestamp() as processed_timestamp,
    'bronze_to_silver' as processing_layer
    
FROM ${source_table}
WHERE transaction_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND total_amount >= 0
```

## PySpark Transformations

### PySpark Configuration

```yaml
# config/transformations/bronze_to_silver/complex_transformations.yaml
order_enrichment:
  source:
    catalog: ${project_code}-${environment}-bronze
    schema: oracle_data
    table: orders
    
  target:
    catalog: ${project_code}-${environment}-silver
    schema: oracle_data
    table: orders_enriched
    
  engine: pyspark
  pyspark_module: transformations.oracle.order_enrichment
  
  parameters:
    lookback_days: 30
    enrichment_mode: "full"
    
  transformations:
    - type: data_quality_checks
      checks:
        - type: null_check
          columns: [order_id, customer_id]
        - type: range_check
          column: order_amount
          min: 0
          max: 1000000
          
    - type: business_logic
      rules:
        - calculate_customer_lifetime_value: true
        - apply_discount_logic: true
        - enrich_with_customer_data: true
```

### PySpark Module Implementation

```python
# src/transformations/oracle/order_enrichment.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, sum as spark_sum, avg, max as spark_max
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def transform(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """
    Complex order enrichment with customer analytics.
    
    Args:
        spark: Active Spark session
        config: Transformation configuration
        
    Returns:
        Enriched DataFrame
    """
    try:
        # Read source data
        source_table = config['source_table']
        orders_df = spark.table(source_table)
        
        # Read customer data for enrichment
        customers_df = spark.table("cddp_dev_bronze.oracle_data.customers")
        
        # Parameters from configuration
        lookback_days = config.get('parameters', {}).get('lookback_days', 30)
        
        # Calculate customer metrics
        customer_metrics = calculate_customer_metrics(orders_df, lookback_days)
        
        # Enrich orders with customer data
        enriched_df = orders_df.join(
            customers_df.select("customer_id", "customer_segment", "signup_date"),
            on="customer_id",
            how="left"
        )
        
        # Add customer lifetime value
        enriched_df = enriched_df.join(
            customer_metrics,
            on="customer_id", 
            how="left"
        )
        
        # Apply business rules
        enriched_df = apply_business_rules(enriched_df)
        
        # Add audit columns
        enriched_df = enriched_df.withColumn("processed_timestamp", current_timestamp()) \
                                .withColumn("transformation_version", lit("1.0"))
        
        logger.info(f"Successfully enriched {enriched_df.count()} orders")
        return enriched_df
        
    except Exception as e:
        logger.error(f"Error in order enrichment: {str(e)}")
        raise

def calculate_customer_metrics(orders_df: DataFrame, lookback_days: int) -> DataFrame:
    """Calculate customer lifetime metrics."""
    from pyspark.sql.functions import datediff, current_date
    
    # Filter recent orders
    recent_orders = orders_df.filter(
        datediff(current_date(), col("order_date")) <= lookback_days
    )
    
    # Calculate metrics
    customer_metrics = recent_orders.groupBy("customer_id").agg(
        spark_sum("order_amount").alias("total_spent_30d"),
        avg("order_amount").alias("avg_order_value_30d"),
        count("*").alias("order_count_30d"),
        spark_max("order_date").alias("last_order_date")
    )
    
    # Calculate customer lifetime value
    customer_metrics = customer_metrics.withColumn(
        "customer_lifetime_value",
        col("total_spent_30d") * 12  # Annualized estimate
    )
    
    return customer_metrics

def apply_business_rules(df: DataFrame) -> DataFrame:
    """Apply complex business rules."""
    # Customer value segmentation
    df = df.withColumn(
        "customer_value_segment",
        when(col("customer_lifetime_value") >= 10000, "High Value")
        .when(col("customer_lifetime_value") >= 1000, "Medium Value")
        .otherwise("Low Value")
    )
    
    # Order risk scoring
    df = df.withColumn(
        "order_risk_score",
        when(col("order_amount") > col("avg_order_value_30d") * 3, "High Risk")
        .when(col("order_amount") > col("avg_order_value_30d") * 1.5, "Medium Risk")
        .otherwise("Low Risk")
    )
    
    return df
```

## SCD Type 2 Configuration

### Complete SCD Type 2 Setup

```yaml
# config/transformations/bronze_to_silver/customer_scd2.yaml
customer_dimension_scd2:
  source:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: customer_master
    
  target:
    catalog: ${project_code}-${environment}-silver
    schema: excel_data
    table: dim_customers_scd2
    
  change_data_capture:
    type: scd_type_2
    
    # Define business keys (natural keys that identify the entity)
    business_keys:
      - customer_id
      
    # Columns to track for changes (Type 2 attributes)
    tracked_columns:
      - customer_name
      - email
      - phone
      - address
      - city
      - state
      - zip_code
      - customer_segment
      - is_active
      
    # Columns to update without creating new version (Type 1 attributes)
    update_only_columns:
      - last_login_date
      - total_purchases
      - last_order_date
      
    # SCD metadata column names
    scd_columns:
      valid_from: valid_from_date
      valid_to: valid_to_date
      is_current: is_current_flag
      version: record_version
      hash: record_hash
      
    # Effective date source
    effective_date_column: signup_date  # or use current_timestamp
    
  transformations:
    - type: data_quality_checks
      checks:
        - type: null_check
          columns: [customer_id, customer_name]
          threshold: 0
        - type: duplicate_check
          columns: [customer_id]
          threshold: 0
          
    - type: standardization
      rules:
        customer_name:
          - action: trim
          - action: upper_case
        email:
          - action: lower_case
          - action: trim
        phone:
          - action: format_phone
            pattern: "###-###-####"
```

### SCD Type 2 Table Structure

The framework automatically creates tables with this structure:

```sql
-- Auto-generated SCD Type 2 table
CREATE TABLE cddp_dev_silver.excel_data.dim_customers_scd2 (
    -- Business columns
    customer_id STRING NOT NULL,
    customer_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    customer_segment STRING,
    is_active BOOLEAN,
    
    -- Type 1 (update-only) columns
    last_login_date DATE,
    total_purchases DECIMAL(10,2),
    last_order_date DATE,
    
    -- SCD Type 2 metadata columns
    valid_from_date TIMESTAMP NOT NULL,
    valid_to_date TIMESTAMP,
    is_current_flag BOOLEAN NOT NULL,
    record_version INT NOT NULL,
    record_hash STRING NOT NULL,
    
    -- Audit columns
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    source_file STRING,
    
    -- Constraints
    PRIMARY KEY (customer_id, record_version)
) USING DELTA
PARTITIONED BY (is_current_flag);
```

### Querying SCD Type 2 Data

```sql
-- Get current customer data
SELECT * FROM dim_customers_scd2
WHERE is_current_flag = true;

-- Get customer data as of specific date
SELECT * FROM dim_customers_scd2
WHERE '2024-01-15' BETWEEN valid_from_date AND COALESCE(valid_to_date, '9999-12-31');

-- Track customer changes over time
SELECT 
    customer_id,
    customer_name,
    email,
    customer_segment,
    valid_from_date,
    valid_to_date,
    record_version
FROM dim_customers_scd2
WHERE customer_id = 'CUST001'
ORDER BY record_version;

-- Analyze customer segment changes
SELECT 
    customer_id,
    LAG(customer_segment) OVER (PARTITION BY customer_id ORDER BY record_version) as previous_segment,
    customer_segment as current_segment,
    valid_from_date as change_date
FROM dim_customers_scd2
WHERE record_version > 1
  AND customer_segment != LAG(customer_segment) OVER (PARTITION BY customer_id ORDER BY record_version);
```

## Multi-Source Transformations

### Joining Multiple Sources

```yaml
# config/transformations/silver_to_gold/customer_360.yaml
customer_360_view:
  sources:
    - catalog: ${project_code}-${environment}-silver
      schema: excel_data
      table: dim_customers_scd2
      alias: customers
      
    - catalog: ${project_code}-${environment}-silver
      schema: oracle_data
      table: orders_enriched
      alias: orders
      
    - catalog: ${project_code}-${environment}-silver
      schema: csv_data
      table: customer_interactions
      alias: interactions
      
  target:
    catalog: ${project_code}-${environment}-gold
    schema: analytics
    table: customer_360
    
  engine: pyspark
  pyspark_module: transformations.gold.customer_360
  
  join_logic:
    - type: left_join
      left: customers
      right: orders
      on: ["customer_id"]
      
    - type: left_join
      left: result
      right: interactions
      on: ["customer_id"]
      
  aggregations:
    - total_orders: "COUNT(orders.order_id)"
    - total_spent: "SUM(orders.order_amount)"
    - avg_order_value: "AVG(orders.order_amount)"
    - last_order_date: "MAX(orders.order_date)"
    - interaction_count: "COUNT(interactions.interaction_id)"
```

## Advanced Transformation Features

### Conditional Transformations

```yaml
conditional_processing:
  source:
    catalog: ${project_code}-${environment}-bronze
    schema: oracle_data
    table: transactions
    
  target:
    catalog: ${project_code}-${environment}-silver
    schema: oracle_data
    table: transactions_processed
    
  engine: sparksql
  
  conditions:
    - if: "transaction_amount > 10000"
      then:
        transformation: "high_value_processing"
        additional_validation: true
        approval_required: true
        
    - if: "customer_segment = 'VIP'"
      then:
        transformation: "vip_processing"
        priority_flag: true
        
    - else:
        transformation: "standard_processing"
```

### Incremental Processing

```yaml
incremental_transformation:
  source:
    catalog: ${project_code}-${environment}-bronze
    schema: oracle_data
    table: daily_transactions
    
  target:
    catalog: ${project_code}-${environment}-silver
    schema: oracle_data
    table: transactions_aggregated
    
  incremental:
    enabled: true
    mode: "merge"  # or "append"
    merge_keys: ["transaction_date", "customer_id"]
    watermark_column: "transaction_timestamp"
    watermark_table: ${project_code}-${environment}-bronze.system.watermarks
    
  engine: sparksql
  sql_file: sql/bronze_to_silver/incremental_transactions.sql
```

### Error Handling and Recovery

```yaml
error_handling:
  on_validation_failure:
    action: "quarantine"  # or "fail", "skip"
    quarantine_table: ${project_code}-${environment}-bronze.system.quarantine
    
  on_transformation_failure:
    action: "retry"
    max_retries: 3
    backoff_seconds: 60
    
  data_quality:
    error_threshold: 5  # Percentage of errors allowed
    action_on_threshold: "warn"  # or "fail"
```

## Running Transformations

### Single Transformation

```bash
# Run specific transformation
python -m src.cli run-silver-transformation --source sales_data_cleansing

# Run with specific transformation only
python -m src.cli run-silver-transformation \
  --source sales_data_cleansing \
  --transformation sales_data_cleansing \
  --write-mode overwrite
```

### Batch Transformations

```bash
# Run all transformations for a source
python -m src.cli run-silver-transformation --source oracle_erp_system

# Run all bronze to silver transformations
python -m src.cli run-all-transformations --layer bronze_to_silver
```

### Debugging Transformations

```bash
# Run with debug logging
python -m src.cli run-silver-transformation \
  --source sales_data_cleansing \
  --log-level DEBUG

# Dry run (validate without executing)
python -m src.cli validate-transformation --source sales_data_cleansing
```

## Performance Optimization

### Spark Configuration for Transformations

```yaml
# In transformation configuration
spark_config:
  "spark.sql.adaptive.enabled": "true"
  "spark.sql.adaptive.coalescePartitions.enabled": "true"
  "spark.sql.adaptive.skewJoin.enabled": "true"
  "spark.sql.adaptive.localShuffleReader.enabled": "true"
  
optimization:
  # Partition strategy
  partition_columns: ["year", "month"]
  bucket_columns: ["customer_id"]
  
  # Z-ordering for better query performance
  z_order_columns: ["customer_id", "transaction_date"]
  
  # Delta table properties
  delta_properties:
    "delta.autoOptimize.optimizeWrite": "true"
    "delta.autoOptimize.autoCompact": "true"
```

### Large Dataset Handling

```yaml
large_dataset_config:
  batch_processing:
    enabled: true
    batch_size: 1000000  # rows per batch
    
  memory_optimization:
    cache_intermediate_results: false
    repartition_before_write: true
    target_partitions: 200
    
  checkpoint_strategy:
    enabled: true
    checkpoint_interval: 10  # every 10 batches
    checkpoint_location: "abfss://checkpoints@storage.dfs.core.windows.net/transformations"
```

## Best Practices

### 1. **SQL vs PySpark Selection**

| Use SparkSQL When | Use PySpark When |
|-------------------|------------------|
| Standard aggregations | Complex business logic |
| Simple joins | Machine learning features |
| Data type conversions | Custom UDFs needed |
| Window functions | Dynamic schema handling |
| Performance critical | Integration with external APIs |

### 2. **Configuration Organization**

```yaml
# Group related transformations
sales_transformations:
  - sales_cleansing
  - sales_enrichment
  - sales_aggregation
  
customer_transformations:
  - customer_standardization
  - customer_scd2
  - customer_segmentation
```

### 3. **Testing Transformations**

```yaml
# Include test configurations
testing:
  sample_data:
    enabled: true
    sample_fraction: 0.1
    
  validation:
    row_count_validation: true
    schema_validation: true
    data_quality_validation: true
    
  comparison:
    compare_with_previous: true
    tolerance: 0.01  # 1% tolerance for numeric comparisons
```

This comprehensive transformation configuration guide enables you to implement complex data processing logic while maintaining simplicity and performance.