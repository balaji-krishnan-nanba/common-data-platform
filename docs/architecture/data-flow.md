# Data Flow Architecture

This document details the end-to-end data flow architecture of the Common Data Platform framework, illustrating how data moves through the medallion architecture from source systems to analytics-ready datasets.

## Architecture Overview

The framework implements a medallion architecture with three distinct layers:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   BRONZE LAYER  │    │  SILVER LAYER   │    │   GOLD LAYER    │
│   (Raw Data)    │───▶│ (Cleansed Data) │───▶│ (Analytics Data)│
│                 │    │                 │    │                 │
│ • Minimal       │    │ • Validated     │    │ • Aggregated    │
│   Processing    │    │ • Standardized  │    │ • Business KPIs │
│ • Schema        │    │ • Quality       │    │ • Report Ready  │
│   Validation    │    │   Checked       │    │ • Optimized     │
│ • Audit Trail   │    │ • SCD Support   │    │   Performance   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Source System Integration

### Data Source Types

#### 1. File-Based Sources (Excel/CSV)
```
Azure Data Lake Storage Gen2
├── raw-data/
│   ├── sales/
│   │   ├── daily/
│   │   │   ├── 2024/01/15/sales_20240115.xlsx
│   │   │   └── 2024/01/16/sales_20240116.xlsx
│   │   └── monthly/
│   │       └── 2024/01/sales_monthly_202401.xlsx
│   ├── customers/
│   │   └── master/
│   │       └── customer_master_20240115.xlsx
│   └── inventory/
│       └── daily/
│           └── inventory_20240115.csv
```

Data Flow Pattern:
```
External System → Upload → ADLS Gen2 → Framework Detection → Bronze Ingestion
```

#### 2. Database Sources (Oracle)
```
Oracle Database
├── SALES Schema
│   ├── CUSTOMERS (150K rows)
│   ├── ORDERS (2.5M rows)
│   └── ORDER_ITEMS (8.7M rows)
├── INVENTORY Schema
│   ├── PRODUCTS (50K rows)
│   └── STOCK_LEVELS (125K rows)
└── PROCUREMENT Schema
    ├── SUPPLIERS (5K rows)
    └── WAREHOUSES (25 rows)
```

Data Flow Pattern:
```
Oracle DB → JDBC Connection → Incremental/Full Extract → Bronze Layer
```

#### 3. API Sources (Future)
```
REST APIs
├── Customer API (CRM System)
├── Product API (ERP System)
└── Market Data API (External)
```

Data Flow Pattern:
```
API Endpoint → HTTP Request → JSON Response → Schema Mapping → Bronze Layer
```

## Bronze Layer Data Flow

### Ingestion Process

#### File Ingestion Flow
```python
# Simplified file ingestion flow
def ingest_excel_file(source_config: Dict, file_path: str) -> IngestResult:
    """
    1. File Detection → 2. Schema Validation → 3. Data Loading → 4. Audit Logging
    """
    
    # Step 1: File Detection and Metadata
    file_metadata = {
        'file_path': file_path,
        'file_size': get_file_size(file_path),
        'last_modified': get_last_modified(file_path),
        'checksum': calculate_checksum(file_path)
    }
    
    # Step 2: Schema Validation
    df = read_excel_with_schema(file_path, source_config['schema'])
    validation_result = validate_schema(df, source_config['schema'])
    
    # Step 3: Add Framework Columns
    enriched_df = add_audit_columns(df, source_config, file_metadata)
    
    # Step 4: Write to Bronze
    write_to_bronze(enriched_df, source_config['target'])
    
    # Step 5: Update Metadata Tables
    update_processed_files(file_metadata, validation_result)
    
    return IngestResult(success=True, records_processed=len(df))
```

#### Database Ingestion Flow
```python
# Oracle incremental ingestion flow
def ingest_oracle_table(table_config: Dict) -> IngestResult:
    """
    1. Watermark Check → 2. Incremental Extract → 3. Schema Mapping → 4. Bronze Write
    """
    
    # Step 1: Get Last Watermark
    last_watermark = get_watermark(
        table_config['source_table'], 
        table_config['incremental']['key_column']
    )
    
    # Step 2: Build Incremental Query
    sql_query = build_incremental_query(table_config, last_watermark)
    
    # Step 3: Extract Data
    df = spark.read \
        .format("jdbc") \
        .option("url", oracle_connection_string) \
        .option("query", sql_query) \
        .option("fetchsize", 10000) \
        .load()
    
    # Step 4: Schema Transformation
    transformed_df = apply_schema_mapping(df, table_config['schema_override'])
    
    # Step 5: Add Audit Columns
    final_df = add_audit_columns(transformed_df, table_config)
    
    # Step 6: Write to Bronze (Append Mode)
    write_to_bronze(final_df, table_config['target'], mode="append")
    
    # Step 7: Update Watermark
    if not df.isEmpty():
        new_watermark = df.agg({table_config['incremental']['key_column']: "max"}).collect()[0][0]
        update_watermark(table_config['source_table'], new_watermark)
    
    return IngestResult(success=True, records_processed=df.count())
```

### Bronze Layer Schema

#### Standard Bronze Table Structure
```sql
-- Example bronze table schema
CREATE TABLE `cddp-dev-bronze`.`oracle_data`.`customers` (
    -- Business columns (from source)
    customer_id STRING NOT NULL,
    customer_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    signup_date DATE,
    customer_segment STRING,
    is_active BOOLEAN,
    last_modified_date TIMESTAMP,
    
    -- Framework audit columns (added automatically)
    ingestion_timestamp TIMESTAMP NOT NULL,
    ingestion_batch_id STRING NOT NULL,
    source_system STRING NOT NULL,
    source_file STRING,
    source_table STRING,
    record_hash STRING NOT NULL,
    data_quality_flags ARRAY<STRING>,
    
    -- Partitioning (for performance)
    ingestion_year INTEGER GENERATED ALWAYS AS (YEAR(ingestion_timestamp)),
    ingestion_month INTEGER GENERATED ALWAYS AS (MONTH(ingestion_timestamp))
)
USING DELTA
PARTITIONED BY (ingestion_year, ingestion_month)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

#### Framework Metadata Tables
```sql
-- Track processed files
CREATE TABLE `cddp-dev-bronze`.`system`.`processed_files` (
    file_path STRING PRIMARY KEY,
    file_size BIGINT,
    file_checksum STRING,
    last_modified_timestamp TIMESTAMP,
    processing_start_time TIMESTAMP,
    processing_end_time TIMESTAMP,
    records_processed BIGINT,
    status STRING, -- 'processing', 'completed', 'failed'
    error_message STRING,
    source_system STRING,
    target_table STRING
);

-- Track incremental loading watermarks
CREATE TABLE `cddp-dev-bronze`.`system`.`watermarks` (
    source_name STRING,
    table_name STRING,
    watermark_column STRING,
    watermark_value STRING,
    last_updated TIMESTAMP,
    PRIMARY KEY (source_name, table_name, watermark_column)
);

-- Track pipeline execution
CREATE TABLE `cddp-dev-bronze`.`system`.`pipeline_executions` (
    pipeline_id STRING,
    source_name STRING,
    execution_start_time TIMESTAMP,
    execution_end_time TIMESTAMP,
    status STRING, -- 'running', 'success', 'failed'
    records_processed BIGINT,
    files_processed INT,
    error_details STRING,
    cluster_id STRING,
    user_identity STRING
);
```

## Silver Layer Data Flow

### Transformation Process

#### Data Quality and Validation Flow
```python
def silver_transformation_flow(source_table: str, target_table: str, 
                             transformation_config: Dict) -> TransformResult:
    """
    1. Read Bronze → 2. Data Quality → 3. Business Rules → 4. Write Silver
    """
    
    # Step 1: Read from Bronze
    bronze_df = spark.table(source_table)
    
    # Step 2: Data Quality Checks
    quality_results = []
    for check in transformation_config.get('data_quality_checks', []):
        result = execute_quality_check(bronze_df, check)
        quality_results.append(result)
        
        if not result.passed and check.get('fail_on_error', False):
            raise DataQualityError(f"Quality check failed: {result.message}")
    
    # Step 3: Apply Transformations
    if transformation_config['engine'] == 'sparksql':
        # SQL-based transformation
        sql_content = load_sql_file(transformation_config['sql_file'])
        transformed_df = spark.sql(sql_content.format(source_table=source_table))
        
    elif transformation_config['engine'] == 'pyspark':
        # PySpark-based transformation
        transform_module = importlib.import_module(transformation_config['pyspark_module'])
        transformed_df = transform_module.transform(spark, transformation_config)
    
    # Step 4: SCD Type 2 Processing (if configured)
    if transformation_config.get('change_data_capture', {}).get('type') == 'scd_type_2':
        final_df = apply_scd_type_2(transformed_df, transformation_config['change_data_capture'])
    else:
        final_df = transformed_df
    
    # Step 5: Write to Silver
    write_mode = transformation_config.get('write_mode', 'overwrite')
    final_df.write \
        .format("delta") \
        .mode(write_mode) \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    
    # Step 6: Log Quality Results
    log_quality_results(quality_results, target_table)
    
    return TransformResult(success=True, records_processed=final_df.count())
```

#### SCD Type 2 Implementation Flow
```python
def apply_scd_type_2(new_data_df: DataFrame, scd_config: Dict) -> DataFrame:
    """
    Slowly Changing Dimension Type 2 implementation.
    
    1. Hash Current → 2. Detect Changes → 3. Close Old → 4. Insert New
    """
    
    business_keys = scd_config['business_keys']
    tracked_columns = scd_config['tracked_columns']
    
    # Step 1: Calculate hash of tracked columns for change detection
    hash_expr = concat_ws("|", *[col(c) for c in tracked_columns])
    new_data_with_hash = new_data_df.withColumn("record_hash", md5(hash_expr))
    
    # Step 2: Read existing SCD table
    target_table = scd_config['target_table']
    if table_exists(target_table):
        existing_df = spark.table(target_table).filter(col("is_current_flag") == True)
        
        # Step 3: Detect changes by comparing hashes
        changes_df = new_data_with_hash.alias("new") \
            .join(existing_df.alias("old"), business_keys, "left_outer") \
            .where(
                col("old.record_hash").isNull() |  # New records
                (col("new.record_hash") != col("old.record_hash"))  # Changed records
            )
        
        # Step 4: Close old records (set valid_to_date and is_current_flag = false)
        closed_records = existing_df.alias("old") \
            .join(changes_df.select(*business_keys).alias("changed"), business_keys, "inner") \
            .withColumn("valid_to_date", current_timestamp()) \
            .withColumn("is_current_flag", lit(False))
        
        # Step 5: Create new active records
        new_active_records = changes_df \
            .withColumn("valid_from_date", current_timestamp()) \
            .withColumn("valid_to_date", lit(None).cast("timestamp")) \
            .withColumn("is_current_flag", lit(True)) \
            .withColumn("record_version", 
                       coalesce(col("old.record_version"), lit(0)) + 1)
        
        # Step 6: Combine all records
        all_records = existing_df \
            .join(changes_df.select(*business_keys), business_keys, "left_anti") \
            .union(closed_records) \
            .union(new_active_records)
            
    else:
        # First load - all records are new and current
        all_records = new_data_with_hash \
            .withColumn("valid_from_date", current_timestamp()) \
            .withColumn("valid_to_date", lit(None).cast("timestamp")) \
            .withColumn("is_current_flag", lit(True)) \
            .withColumn("record_version", lit(1))
    
    return all_records
```

### Silver Layer Schema Patterns

#### Dimension Table (SCD Type 2)
```sql
CREATE TABLE `cddp-dev-silver`.`customer_data`.`dim_customers_scd2` (
    -- Business keys
    customer_id STRING NOT NULL,
    
    -- Tracked attributes (Type 2)
    customer_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    customer_segment STRING,
    is_active BOOLEAN,
    
    -- Non-tracked attributes (Type 1)
    last_login_date DATE,
    total_lifetime_value DECIMAL(15,2),
    
    -- SCD Type 2 metadata
    valid_from_date TIMESTAMP NOT NULL,
    valid_to_date TIMESTAMP,
    is_current_flag BOOLEAN NOT NULL,
    record_version INTEGER NOT NULL,
    record_hash STRING NOT NULL,
    
    -- Audit columns
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    source_system STRING
)
USING DELTA
PARTITIONED BY (is_current_flag)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

#### Fact Table (Transactional)
```sql
CREATE TABLE `cddp-dev-silver`.`sales_data`.`fact_sales` (
    -- Fact table keys
    sale_id STRING NOT NULL,
    transaction_date DATE NOT NULL,
    
    -- Dimension foreign keys
    customer_key STRING NOT NULL,
    product_key STRING NOT NULL,
    store_key STRING NOT NULL,
    
    -- Measures
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(15,2) NOT NULL,
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    
    -- Derived measures
    gross_profit DECIMAL(15,2),
    margin_percentage DECIMAL(5,2),
    
    -- Audit columns
    processed_timestamp TIMESTAMP,
    source_batch_id STRING
)
USING DELTA
PARTITIONED BY (YEAR(transaction_date), MONTH(transaction_date))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

## Gold Layer Data Flow

### Analytics and Aggregation Flow

#### Business KPI Generation
```python
def create_gold_analytics(silver_tables: List[str], gold_config: Dict) -> AnalyticsResult:
    """
    1. Multi-table Join → 2. Business Logic → 3. Aggregation → 4. Gold Write
    """
    
    # Step 1: Load required silver tables
    datasets = {}
    for table in silver_tables:
        datasets[table] = spark.table(table)
    
    # Step 2: Apply business rules and joins
    if gold_config['engine'] == 'sparksql':
        sql_content = load_sql_file(gold_config['sql_file'])
        analytics_df = spark.sql(sql_content.format(**datasets))
        
    elif gold_config['engine'] == 'pyspark':
        analytics_module = importlib.import_module(gold_config['pyspark_module'])
        analytics_df = analytics_module.create_analytics(spark, datasets, gold_config)
    
    # Step 3: Apply aggregations and window functions
    final_df = apply_business_aggregations(analytics_df, gold_config['aggregations'])
    
    # Step 4: Optimize for query performance
    optimized_df = optimize_for_analytics(final_df, gold_config['optimization'])
    
    # Step 5: Write to Gold layer
    write_to_gold(optimized_df, gold_config['target'])
    
    return AnalyticsResult(success=True, records_created=final_df.count())

def apply_business_aggregations(df: DataFrame, agg_config: Dict) -> DataFrame:
    """Apply complex business aggregations."""
    
    result_df = df
    
    # Customer 360 aggregations
    if 'customer_360' in agg_config:
        customer_metrics = df.groupBy("customer_id") \
            .agg(
                sum("total_amount").alias("lifetime_value"),
                avg("total_amount").alias("avg_order_value"),
                count("*").alias("total_orders"),
                max("transaction_date").alias("last_purchase_date"),
                first("customer_segment").alias("segment"),
                # Advanced metrics
                (sum("total_amount") / count("*")).alias("average_basket_size"),
                countDistinct("product_category").alias("category_diversity"),
                # Recency, Frequency, Monetary (RFM) analysis
                datediff(current_date(), max("transaction_date")).alias("recency_days"),
                count("*").alias("frequency"),
                sum("total_amount").alias("monetary_value")
            )
        
        # Add RFM scoring
        result_df = add_rfm_scoring(customer_metrics)
    
    return result_df
```

#### Real-time Analytics Flow
```python
def create_real_time_analytics(streaming_config: Dict) -> StreamingQuery:
    """
    Real-time analytics using Delta Live Tables and Structured Streaming.
    """
    
    # Read streaming data from bronze
    streaming_df = spark.readStream \
        .format("delta") \
        .table(streaming_config['source_table'])
    
    # Apply real-time transformations
    processed_df = streaming_df \
        .withWatermark("transaction_timestamp", "10 minutes") \
        .groupBy(
            window("transaction_timestamp", "5 minutes"),
            "store_location",
            "payment_method"
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_value")
        )
    
    # Write to gold streaming table
    query = processed_df.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", streaming_config['checkpoint_path']) \
        .table(streaming_config['target_table'])
    
    return query
```

### Gold Layer Schema Patterns

#### Analytics Summary Tables
```sql
CREATE TABLE `cddp-dev-gold`.`analytics`.`daily_sales_summary` (
    -- Date dimensions
    sales_date DATE NOT NULL,
    year INTEGER,
    month INTEGER,
    quarter INTEGER,
    day_of_week INTEGER,
    
    -- Geographic dimensions
    store_location STRING,
    region STRING,
    country STRING,
    
    -- Business metrics
    total_transactions BIGINT,
    unique_customers BIGINT,
    total_revenue DECIMAL(20,2),
    total_cost DECIMAL(20,2),
    gross_profit DECIMAL(20,2),
    margin_percentage DECIMAL(5,2),
    
    -- Performance metrics
    avg_transaction_value DECIMAL(10,2),
    transactions_per_customer DECIMAL(5,2),
    customer_acquisition_cost DECIMAL(10,2),
    
    -- Comparison metrics
    revenue_vs_target DECIMAL(5,2),
    revenue_vs_prior_year DECIMAL(5,2),
    
    -- Data freshness
    last_updated_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (year, month)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

#### Customer 360 View
```sql
CREATE TABLE `cddp-dev-gold`.`analytics`.`customer_360` (
    -- Customer identification
    customer_id STRING PRIMARY KEY,
    customer_name STRING,
    customer_segment STRING,
    
    -- Demographics
    age_group STRING,
    geographic_region STRING,
    acquisition_channel STRING,
    
    -- Transactional metrics
    lifetime_value DECIMAL(15,2),
    total_orders BIGINT,
    avg_order_value DECIMAL(10,2),
    first_purchase_date DATE,
    last_purchase_date DATE,
    
    -- Behavioral metrics
    purchase_frequency DECIMAL(5,2),
    category_diversity INTEGER,
    seasonal_preference STRING,
    payment_preference STRING,
    
    -- Predictive metrics
    churn_risk_score DECIMAL(3,2),
    next_purchase_prediction_days INTEGER,
    upsell_propensity DECIMAL(3,2),
    
    -- RFM Analysis
    recency_score INTEGER,
    frequency_score INTEGER,
    monetary_score INTEGER,
    rfm_segment STRING,
    
    -- Data lineage
    source_systems ARRAY<STRING>,
    last_updated_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

## Performance Optimization

### Data Flow Optimization Strategies

#### 1. Partitioning Strategy
```python
# Optimize partitioning for query patterns
PARTITION_STRATEGIES = {
    'bronze': ['ingestion_year', 'ingestion_month'],  # Time-based for retention
    'silver': ['year', 'month'],  # Business time dimensions
    'gold': ['region', 'year'],   # Analytics dimensions
}

# Example: Sales fact table partitioning
CREATE TABLE fact_sales
PARTITIONED BY (sale_year, sale_month)
CLUSTERED BY (customer_id) INTO 32 BUCKETS
```

#### 2. Z-Ordering for Performance
```sql
-- Optimize tables for common query patterns
OPTIMIZE `cddp-prod-silver`.`sales_data`.`fact_sales`
ZORDER BY (customer_id, product_id, store_id);

-- Customer dimension optimization
OPTIMIZE `cddp-prod-silver`.`customer_data`.`dim_customers_scd2`
ZORDER BY (customer_id, is_current_flag);
```

#### 3. Delta Lake Optimization
```python
# Auto-optimization configuration
DELTA_OPTIMIZATIONS = {
    'delta.autoOptimize.optimizeWrite': 'true',
    'delta.autoOptimize.autoCompact': 'true',
    'delta.logRetentionDuration': '7 days',
    'delta.deletedFileRetentionDuration': '7 days'
}

# Table maintenance scheduling
def optimize_tables_maintenance():
    """Run nightly table optimization."""
    tables_to_optimize = get_frequently_updated_tables()
    
    for table in tables_to_optimize:
        spark.sql(f"OPTIMIZE {table}")
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS")  # 7 days
```

### Monitoring Data Flow Performance

#### Pipeline Performance Metrics
```sql
-- Monitor data flow performance
CREATE OR REPLACE VIEW pipeline_performance_metrics AS
SELECT 
    source_name,
    AVG(
        UNIX_TIMESTAMP(execution_end_time) - 
        UNIX_TIMESTAMP(execution_start_time)
    ) / 60 as avg_duration_minutes,
    AVG(records_processed) as avg_records_per_run,
    AVG(records_processed / 
        ((UNIX_TIMESTAMP(execution_end_time) - 
          UNIX_TIMESTAMP(execution_start_time)) / 60)
    ) as avg_records_per_minute,
    COUNT(*) as total_executions,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*) * 100 as success_rate
FROM `cddp-prod-bronze`.`system`.`pipeline_executions`
WHERE execution_start_time >= current_date() - INTERVAL 30 DAYS
GROUP BY source_name;
```

#### Data Freshness Monitoring
```sql
-- Monitor data freshness across layers
CREATE OR REPLACE VIEW data_freshness_monitor AS
WITH layer_freshness AS (
    SELECT 
        'bronze' as layer,
        source_system,
        MAX(ingestion_timestamp) as latest_data_timestamp,
        COUNT(*) as record_count
    FROM `cddp-prod-bronze`.`oracle_data`.`customers`
    GROUP BY source_system
    
    UNION ALL
    
    SELECT 
        'silver' as layer,
        'customer_data' as source_system,
        MAX(updated_timestamp) as latest_data_timestamp,
        COUNT(*) as record_count
    FROM `cddp-prod-silver`.`customer_data`.`dim_customers_scd2`
    WHERE is_current_flag = true
    
    UNION ALL
    
    SELECT 
        'gold' as layer,
        'analytics' as source_system,
        MAX(last_updated_timestamp) as latest_data_timestamp,
        COUNT(*) as record_count
    FROM `cddp-prod-gold`.`analytics`.`customer_360`
)
SELECT 
    layer,
    source_system,
    latest_data_timestamp,
    record_count,
    ROUND(
        (UNIX_TIMESTAMP(current_timestamp()) - 
         UNIX_TIMESTAMP(latest_data_timestamp)) / 3600, 2
    ) as hours_since_last_update
FROM layer_freshness
ORDER BY layer, source_system;
```

This comprehensive data flow architecture ensures efficient, reliable, and scalable data processing while maintaining data quality and performance across all layers of the medallion architecture.