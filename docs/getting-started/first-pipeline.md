# First Pipeline Guide

This guide walks you through creating your first complete data pipeline using the Common Data Platform framework, from raw data ingestion to business-ready analytics.

## Overview

We'll create a complete pipeline that:
1. **Ingests** Excel sales data to the bronze layer
2. **Cleanses** and validates data in the silver layer
3. **Aggregates** data for analytics in the gold layer
4. **Monitors** pipeline health and data quality

## Prerequisites

Before starting, ensure you have:
- [ ] Framework installed and configured ([Installation Guide](installation.md))
- [ ] Azure resources provisioned
- [ ] Sample data prepared

## Step 1: Prepare Sample Data

### 1.1 Create Sample Excel File

Create a file named `sales_data.xlsx` with this structure:

| transaction_id | transaction_date | customer_id | product_code | quantity | unit_price | total_amount | payment_method | store_location |
|----------------|------------------|-------------|--------------|----------|------------|--------------|----------------|----------------|
| TXN001 | 2024-01-15 | CUST001 | PROD101 | 2 | 25.50 | 51.00 | Credit Card | New York |
| TXN002 | 2024-01-15 | CUST002 | PROD102 | 1 | 15.75 | 15.75 | Cash | Los Angeles |
| TXN003 | 2024-01-15 | CUST001 | PROD103 | 3 | 8.99 | 26.97 | Debit Card | New York |

### 1.2 Upload to Azure Storage

```bash
# Upload sample data to Azure Storage
az storage blob upload \
  --account-name yourstorageaccount \
  --container-name raw-data \
  --name sales/2024/01/15/sales_data.xlsx \
  --file sales_data.xlsx
```

## Step 2: Configure Data Source

### 2.1 Create Source Configuration

Edit `config/sources/excel_sources.yaml`:

```yaml
my_first_pipeline:
  name: my_first_pipeline
  type: excel
  
  connection:
    storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}
    container: raw-data
    path: sales/2024/01/15/sales_data.xlsx
    secret_scope: databricks-secrets-dev
    account_key: storage-account-key
    
  schema:
    strict_validation: true
    columns:
      - name: transaction_id
        type: string
        nullable: false
      - name: transaction_date
        type: date
        nullable: false
      - name: customer_id
        type: string
        nullable: false
      - name: product_code
        type: string
        nullable: false
      - name: quantity
        type: integer
        nullable: false
        constraints:
          min: 1
      - name: unit_price
        type: decimal(10,2)
        nullable: false
        constraints:
          min: 0
      - name: total_amount
        type: decimal(10,2)
        nullable: false
        constraints:
          min: 0
      - name: payment_method
        type: string
        nullable: true
      - name: store_location
        type: string
        nullable: true
        
  read_options:
    header: true
    sheet_name: "Sheet1"
    
  target:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: sales_transactions
    
  data_quality:
    checks:
      - type: null_check
        columns: [transaction_id, customer_id, product_code]
        threshold: 0
      - type: duplicate_check
        columns: [transaction_id]
        threshold: 0
```

### 2.2 Validate Configuration

```bash
# Validate the source configuration
python -m src.cli validate-source-config --source my_first_pipeline
```

Expected output:
```
✅ Source configuration 'my_first_pipeline' is valid
```

## Step 3: Bronze Layer Ingestion

### 3.1 Run Bronze Ingestion

```bash
# Ingest data to bronze layer
python -m src.cli run-bronze-ingestion --source my_first_pipeline --log-level INFO
```

Expected output:
```
2024-01-15 10:30:00 | INFO | Starting bronze ingestion for source: my_first_pipeline
2024-01-15 10:30:02 | INFO | Reading Excel file: sales/2024/01/15/sales_data.xlsx
2024-01-15 10:30:03 | INFO | Schema validation passed
2024-01-15 10:30:04 | INFO | Successfully wrote 3 records to cddp-dev-bronze.excel_data.sales_transactions
2024-01-15 10:30:04 | INFO | Bronze ingestion completed successfully
```

### 3.2 Verify Bronze Data

```sql
-- In Databricks SQL or notebook
USE CATALOG `cddp-dev-bronze`;

-- Check the data
SELECT * FROM excel_data.sales_transactions;

-- Verify audit columns
SELECT 
    transaction_id,
    customer_id,
    total_amount,
    ingestion_timestamp,
    source_system,
    source_file
FROM excel_data.sales_transactions;
```

Expected result:
```
transaction_id | customer_id | total_amount | ingestion_timestamp | source_system | source_file
TXN001         | CUST001     | 51.00        | 2024-01-15 10:30:04 | my_first_pipeline | sales/2024/01/15/sales_data.xlsx
TXN002         | CUST002     | 15.75        | 2024-01-15 10:30:04 | my_first_pipeline | sales/2024/01/15/sales_data.xlsx
TXN003         | CUST001     | 26.97        | 2024-01-15 10:30:04 | my_first_pipeline | sales/2024/01/15/sales_data.xlsx
```

## Step 4: Configure Silver Transformation

### 4.1 Create Transformation Configuration

Create `config/transformations/bronze_to_silver/my_first_transformations.yaml`:

```yaml
sales_cleansing:
  source:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: sales_transactions
    
  target:
    catalog: ${project_code}-${environment}-silver
    schema: excel_data
    table: sales_cleansed
    
  engine: sparksql
  
  transformations:
    - type: data_quality_checks
      checks:
        - type: null_check
          columns: [transaction_id, customer_id, product_code]
          threshold: 0
        - type: range_check
          column: total_amount
          min: 0
          max: 10000
          
    - type: standardization
      rules:
        payment_method:
          - from: ["Credit Card", "CC", "CREDIT"]
            to: "Credit Card"
          - from: ["Debit Card", "DC", "DEBIT"]
            to: "Debit Card"
          - from: ["Cash", "CASH", "cash"]
            to: "Cash"
        store_location:
          - from: ["New York", "NY", "NYC"]
            to: "New York"
          - from: ["Los Angeles", "LA", "L.A."]
            to: "Los Angeles"
            
    - type: derived_columns
      columns:
        - name: transaction_year
          expression: "YEAR(transaction_date)"
        - name: transaction_month
          expression: "MONTH(transaction_date)"
        - name: revenue_category
          expression: |
            CASE 
              WHEN total_amount >= 50 THEN 'High'
              WHEN total_amount >= 20 THEN 'Medium'
              ELSE 'Low'
            END
        - name: discount_amount
          expression: "(unit_price * quantity) - total_amount"
          
  write_mode: overwrite
  partition_by: [transaction_year, transaction_month]
```

### 4.2 Create SQL Transformation File

Create `src/sql/bronze_to_silver/my_first_sales_cleansing.sql`:

```sql
-- Sales data cleansing transformation
WITH standardized_data AS (
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
            WHEN UPPER(payment_method) IN ('CREDIT CARD', 'CC', 'CREDIT') THEN 'Credit Card'
            WHEN UPPER(payment_method) IN ('DEBIT CARD', 'DC', 'DEBIT') THEN 'Debit Card'
            WHEN UPPER(payment_method) IN ('CASH') THEN 'Cash'
            ELSE payment_method
        END as payment_method_clean,
        
        -- Standardize store locations
        CASE 
            WHEN UPPER(store_location) IN ('NEW YORK', 'NY', 'NYC') THEN 'New York'
            WHEN UPPER(store_location) IN ('LOS ANGELES', 'LA', 'L.A.') THEN 'Los Angeles'
            ELSE store_location
        END as store_location_clean,
        
        -- Derived columns
        YEAR(transaction_date) as transaction_year,
        MONTH(transaction_date) as transaction_month,
        QUARTER(transaction_date) as transaction_quarter,
        
        CASE 
            WHEN total_amount >= 50 THEN 'High'
            WHEN total_amount >= 20 THEN 'Medium'
            ELSE 'Low'
        END as revenue_category,
        
        (unit_price * quantity) - total_amount as discount_amount,
        
        -- Audit columns
        ingestion_timestamp,
        source_system,
        source_file,
        current_timestamp() as silver_processed_timestamp
        
    FROM ${source_table}
    WHERE transaction_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND product_code IS NOT NULL
      AND total_amount >= 0
),

quality_checked AS (
    SELECT *,
        -- Data quality flags
        CASE 
            WHEN discount_amount < 0 THEN 'Potential Pricing Issue'
            WHEN discount_amount > (unit_price * quantity * 0.5) THEN 'High Discount'
            ELSE 'Normal'
        END as quality_flag
        
    FROM standardized_data
)

SELECT * FROM quality_checked
```

## Step 5: Run Silver Transformation

### 5.1 Execute Silver Transformation

```bash
# Run silver transformation
python -m src.cli run-silver-transformation --source my_first_pipeline
```

### 5.2 Verify Silver Data

```sql
-- Check silver data
USE CATALOG `cddp-dev-silver`;

SELECT * FROM excel_data.sales_cleansed;

-- Verify transformations
SELECT 
    transaction_id,
    payment_method_clean,
    store_location_clean,
    revenue_category,
    discount_amount,
    quality_flag
FROM excel_data.sales_cleansed;
```

## Step 6: Create Gold Layer Analytics

### 6.1 Configure Gold Transformation

Create `config/transformations/silver_to_gold/sales_analytics.yaml`:

```yaml
daily_sales_summary:
  source:
    catalog: ${project_code}-${environment}-silver
    schema: excel_data
    table: sales_cleansed
    
  target:
    catalog: ${project_code}-${environment}-gold
    schema: analytics
    table: daily_sales_summary
    
  engine: sparksql
  
  aggregations:
    - type: daily_summary
      group_by: [transaction_date, store_location_clean]
      metrics:
        - total_transactions: "COUNT(*)"
        - total_revenue: "SUM(total_amount)"
        - avg_transaction_value: "AVG(total_amount)"
        - total_discount: "SUM(discount_amount)"
        - high_value_transactions: "SUM(CASE WHEN revenue_category = 'High' THEN 1 ELSE 0 END)"
```

### 6.2 Create Gold SQL

Create `src/sql/silver_to_gold/daily_sales_summary.sql`:

```sql
-- Daily sales analytics summary
SELECT 
    transaction_date,
    store_location_clean as store_location,
    
    -- Transaction metrics
    COUNT(*) as total_transactions,
    COUNT(DISTINCT customer_id) as unique_customers,
    
    -- Revenue metrics
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction_value,
    MIN(total_amount) as min_transaction_value,
    MAX(total_amount) as max_transaction_value,
    
    -- Discount metrics
    SUM(discount_amount) as total_discounts,
    AVG(discount_amount) as avg_discount,
    
    -- Category breakdown
    SUM(CASE WHEN revenue_category = 'High' THEN 1 ELSE 0 END) as high_value_transactions,
    SUM(CASE WHEN revenue_category = 'Medium' THEN 1 ELSE 0 END) as medium_value_transactions,
    SUM(CASE WHEN revenue_category = 'Low' THEN 1 ELSE 0 END) as low_value_transactions,
    
    -- Payment method breakdown
    SUM(CASE WHEN payment_method_clean = 'Credit Card' THEN total_amount ELSE 0 END) as credit_card_revenue,
    SUM(CASE WHEN payment_method_clean = 'Debit Card' THEN total_amount ELSE 0 END) as debit_card_revenue,
    SUM(CASE WHEN payment_method_clean = 'Cash' THEN total_amount ELSE 0 END) as cash_revenue,
    
    -- Data quality
    SUM(CASE WHEN quality_flag != 'Normal' THEN 1 ELSE 0 END) as quality_issues,
    
    -- Metadata
    current_timestamp() as created_timestamp
    
FROM ${source_table}
GROUP BY transaction_date, store_location_clean
ORDER BY transaction_date, store_location_clean
```

### 6.3 Run Gold Transformation

```bash
# Run gold transformation
python -m src.cli run-gold-transformation --source sales_analytics
```

### 6.4 Verify Gold Data

```sql
-- Check gold analytics
USE CATALOG `cddp-dev-gold`;

SELECT * FROM analytics.daily_sales_summary;

-- Sample analytics query
SELECT 
    store_location,
    total_transactions,
    total_revenue,
    avg_transaction_value,
    high_value_transactions
FROM analytics.daily_sales_summary
WHERE transaction_date = '2024-01-15';
```

## Step 7: Monitor Pipeline Health

### 7.1 Check Pipeline Execution

```sql
-- Check pipeline execution status
SELECT 
    pipeline_id,
    source_name,
    status,
    records_processed,
    duration_seconds,
    start_time
FROM `cddp-dev-bronze`.`system`.`pipeline_executions`
WHERE source_name = 'my_first_pipeline'
ORDER BY start_time DESC;
```

### 7.2 Check Data Quality Results

```sql
-- Check data quality results
SELECT 
    table_name,
    check_type,
    check_name,
    status,
    records_checked,
    records_failed,
    check_timestamp
FROM `cddp-dev-bronze`.`system`.`data_quality_results`
WHERE table_name LIKE '%sales%'
ORDER BY check_timestamp DESC;
```

## Step 8: Create Monitoring Dashboard

### 8.1 Pipeline Health Query

```sql
-- Pipeline health dashboard
SELECT 
    DATE(start_time) as pipeline_date,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
    AVG(duration_seconds) as avg_duration_seconds,
    SUM(records_processed) as total_records_processed
FROM `cddp-dev-bronze`.`system`.`pipeline_executions`
WHERE source_name = 'my_first_pipeline'
    AND start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY DATE(start_time)
ORDER BY pipeline_date;
```

### 8.2 Data Freshness Check

```sql
-- Data freshness monitoring
SELECT 
    'bronze' as layer,
    'sales_transactions' as table_name,
    MAX(transaction_date) as latest_data_date,
    MAX(ingestion_timestamp) as latest_ingestion,
    datediff(current_date(), MAX(transaction_date)) as days_old
FROM `cddp-dev-bronze`.`excel_data`.`sales_transactions`

UNION ALL

SELECT 
    'silver' as layer,
    'sales_cleansed' as table_name,
    MAX(transaction_date) as latest_data_date,
    MAX(silver_processed_timestamp) as latest_ingestion,
    datediff(current_date(), MAX(transaction_date)) as days_old
FROM `cddp-dev-silver`.`excel_data`.`sales_cleansed`

UNION ALL

SELECT 
    'gold' as layer,
    'daily_sales_summary' as table_name,
    MAX(transaction_date) as latest_data_date,
    MAX(created_timestamp) as latest_ingestion,
    datediff(current_date(), MAX(transaction_date)) as days_old
FROM `cddp-dev-gold`.`analytics`.`daily_sales_summary`;
```

## Step 9: Schedule Automation (Optional)

### 9.1 Create Databricks Workflow

Edit `databricks.yml` to add your pipeline:

```yaml
resources:
  jobs:
    my_first_pipeline_job:
      name: "My First Pipeline - Daily Processing"
      
      tasks:
        - task_key: ingest_to_bronze
          python_wheel_task:
            package_name: common_data_platform
            entry_point: run_bronze_ingestion
            parameters: ["--source", "my_first_pipeline"]
          
        - task_key: transform_to_silver
          depends_on:
            - task_key: ingest_to_bronze
          python_wheel_task:
            package_name: common_data_platform
            entry_point: run_silver_transformation
            parameters: ["--source", "my_first_pipeline"]
        
        - task_key: create_gold_analytics
          depends_on:
            - task_key: transform_to_silver
          python_wheel_task:
            package_name: common_data_platform
            entry_point: run_gold_transformation
            parameters: ["--source", "sales_analytics"]
      
      schedule:
        quartz_cron_expression: "0 0 8 * * ?"  # Daily at 8 AM
        timezone_id: "UTC"
```

### 9.2 Deploy Workflow

```bash
# Deploy the workflow
databricks bundle deploy -t dev

# Test run
databricks jobs run-now --job-id <job-id>
```

## Congratulations! 🎉

You've successfully created your first complete data pipeline with:

✅ **Bronze Layer**: Raw data ingestion with schema validation  
✅ **Silver Layer**: Data cleansing and standardization  
✅ **Gold Layer**: Business analytics and KPIs  
✅ **Monitoring**: Pipeline health and data quality tracking  
✅ **Automation**: Scheduled workflow execution  

## Next Steps

1. **Scale Up**: Add more data sources using the [Source Configuration Guide](../configuration/source-configuration.md)
2. **Advanced Transformations**: Implement SCD Type 2 for customer dimensions
3. **Production Deployment**: Follow the [Environment Management Guide](../configuration/environment-management.md)
4. **Enhanced Monitoring**: Set up alerts and dashboards using the [Monitoring Guide](../monitoring/monitoring-guide.md)

## Troubleshooting

If you encounter issues:
- Check the [Troubleshooting Guide](../monitoring/troubleshooting.md)
- Review pipeline logs in Databricks
- Validate configurations using the CLI tools
- Contact the Data Engineering team for support