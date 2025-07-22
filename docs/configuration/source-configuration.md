# Source Configuration Guide

This guide covers how to configure different types of data sources in the Common Data Platform framework.

## Overview

Data sources are defined in YAML files under `config/sources/`. Each source type has its own configuration file:

- `excel_sources.yaml` - Excel file sources
- `csv_sources.yaml` - CSV file sources  
- `oracle_sources.yaml` - Oracle database sources

## Excel Sources

### Basic Excel Configuration

```yaml
daily_sales_excel:
  name: daily_sales_excel
  type: excel
  
  connection:
    storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}
    container: raw-data
    path: sales/daily/sales_data.xlsx
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
        format: yyyy-MM-dd
      - name: customer_id
        type: string
        nullable: false
      - name: amount
        type: decimal(10,2)
        nullable: false
        constraints:
          min: 0
          
  read_options:
    header: true
    sheet_name: "Sales Data"
    treat_empty_values_as_nulls: true
    
  target:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: daily_sales
```

### Excel with Date-Based Paths

For daily file drops with date-based folder structure:

```yaml
daily_sales_excel:
  name: daily_sales_excel
  type: excel
  
  connection:
    storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}
    container: raw-data
    path_pattern: sales/daily/{year}/{month}/{day}/sales_*.xlsx
    file_pattern: "sales_*.xlsx"
    secret_scope: databricks-secrets-dev
    account_key: storage-account-key
    
  ingestion:
    mode: incremental  # Only process new files
    
  file_tracking:
    method: database
    tracking_table: ${project_code}-${environment}-bronze.system.processed_files
```

The framework automatically resolves date patterns:
- `{year}` → Current year (e.g., 2024)
- `{month}` → Current month (e.g., 01)
- `{day}` → Current day (e.g., 15)

### Excel Schema Validation

Define comprehensive schema validation:

```yaml
customer_master_excel:
  schema:
    strict_validation: true
    columns:
      - name: customer_id
        type: string
        nullable: false
      - name: email
        type: string
        nullable: true
        pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"  # Email regex
      - name: phone
        type: string
        nullable: true
        constraints:
          max_length: 15
      - name: signup_date
        type: date
        nullable: false
      - name: age
        type: integer
        nullable: true
        constraints:
          min: 18
          max: 120
      - name: is_active
        type: boolean
        nullable: false
        default: true
```

## CSV Sources

### Basic CSV Configuration

```yaml
customer_csv:
  name: customer_csv
  type: csv
  
  connection:
    storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}
    container: raw-data
    path: customers/customer_data.csv
    secret_scope: databricks-secrets-dev
    account_key: storage-account-key
    
  read_options:
    header: true
    delimiter: ","
    quote: '"'
    escape: '"'
    encoding: UTF-8
    
  schema:
    columns:
      - name: id
        type: string
        nullable: false
      - name: name
        type: string
        nullable: false
        
  target:
    catalog: ${project_code}-${environment}-bronze
    schema: csv_data
    table: customers
```

### CSV with Custom Delimiter

```yaml
pipe_delimited_data:
  name: pipe_delimited_data
  type: csv
  
  read_options:
    header: true
    delimiter: "|"          # Pipe-delimited
    quote: "'"              # Single quotes
    multiLine: true         # Handle multi-line records
    
  schema:
    columns:
      - name: description
        type: string
        nullable: true      # May contain newlines
```

## Oracle Sources

### Single Table Configuration

```yaml
oracle_customers:
  name: oracle_customers
  type: oracle
  
  connection:
    host: oracle-server.company.com
    port: 1521
    service_name: PROD
    secret_scope: databricks-secrets-dev
    username_key: oracle-username
    password_key: oracle-password
    
  table:
    source_schema: SALES
    source_table: CUSTOMERS
    target_schema: oracle_data
    target_table: customers
    
  jdbc_options:
    fetchsize: 10000
    numPartitions: 4
    
  target:
    catalog: ${project_code}-${environment}-bronze
```

### Oracle Multi-Table Configuration

For ingesting multiple tables (like your 7 Oracle tables requirement):

```yaml
oracle_erp_system:
  name: oracle_erp_system
  type: oracle
  
  connection:
    host: oracle-server.company.com
    port: 1521
    service_name: PROD
    secret_scope: databricks-secrets-dev
    username_key: oracle-username
    password_key: oracle-password
  
  tables:
    - name: customers
      source_schema: SALES
      source_table: CUSTOMERS
      target_schema: oracle_data
      target_table: customers
      incremental:
        enabled: true
        key_column: LAST_MODIFIED_DATE
        
    - name: orders
      source_schema: SALES
      source_table: ORDERS
      target_schema: oracle_data
      target_table: orders
      incremental:
        enabled: true
        key_column: ORDER_DATE
        
    - name: order_items
      source_schema: SALES
      source_table: ORDER_ITEMS
      target_schema: oracle_data
      target_table: order_items
      incremental:
        enabled: true
        key_column: CREATED_DATE
        
    - name: products
      source_schema: INVENTORY
      source_table: PRODUCTS
      target_schema: oracle_data
      target_table: products
      incremental:
        enabled: false  # Full refresh for master data
        
    - name: inventory
      source_schema: INVENTORY
      source_table: STOCK_LEVELS
      target_schema: oracle_data
      target_table: inventory
      incremental:
        enabled: true
        key_column: LAST_UPDATED
        
    - name: suppliers
      source_schema: PROCUREMENT
      source_table: SUPPLIERS
      target_schema: oracle_data
      target_table: suppliers
      incremental:
        enabled: false  # Full refresh
        
    - name: warehouses
      source_schema: LOGISTICS
      source_table: WAREHOUSES
      target_schema: oracle_data
      target_table: warehouses
      incremental:
        enabled: false  # Full refresh

  target:
    catalog: ${project_code}-${environment}-bronze
    
  batch_size: 100000
  
  jdbc_options:
    fetchsize: 10000
    numPartitions: 8  # Parallel processing
```

### Incremental Loading

Configure incremental loading based on timestamp columns:

```yaml
oracle_transactions:
  tables:
    - name: transactions
      incremental:
        enabled: true
        key_column: TRANSACTION_DATE
        watermark_table: ${project_code}-${environment}-bronze.system.watermarks
        mode: timestamp  # or 'sequence' for ID-based
        
        # Optional: custom SQL for complex incremental logic
        custom_filter: "TRANSACTION_DATE > (SELECT MAX(transaction_date) FROM target_table)"
```

## Advanced Source Configuration

### Authentication Methods

#### Service Principal Authentication

```yaml
connection:
  storage_account: mystorageaccount
  secret_scope: databricks-secrets-dev
  service_principal:
    client_id_key: sp-client-id
    client_secret_key: sp-client-secret
    tenant_id: your-tenant-id
```

#### SAS Token Authentication

```yaml
connection:
  storage_account: mystorageaccount
  secret_scope: databricks-secrets-dev
  sas_token_key: storage-sas-token
```

### Data Quality Configuration

Add data quality checks at the source level:

```yaml
source_with_quality:
  schema:
    columns:
      - name: customer_id
        type: string
        nullable: false
        
  data_quality:
    checks:
      - type: null_check
        columns: [customer_id, email]
        threshold: 0  # 0% nulls allowed
        
      - type: duplicate_check
        columns: [customer_id]
        threshold: 0  # No duplicates allowed
        
      - type: range_check
        column: age
        min: 18
        max: 120
        
      - type: pattern_check
        column: email
        pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        
      - type: custom_sql
        condition: "amount > 0 AND amount < 1000000"
        
    on_failure: [fail|warn|continue]  # How to handle quality failures
```

### Partitioning Configuration

```yaml
target:
  catalog: ${project_code}-${environment}-bronze
  schema: oracle_data
  table: large_table
  partition_columns: ["year", "month"]  # Partition by date components
  
  table_properties:
    'delta.autoOptimize.optimizeWrite': 'true'
    'delta.autoOptimize.autoCompact': 'true'
```

### Custom Read Options

```yaml
# Excel-specific options
read_options:
  header: true
  sheet_name: "Data"        # Specific sheet
  startRow: 2              # Skip header rows
  endRow: 1000             # Limit rows
  startCol: "A"            # Start column
  endCol: "J"              # End column

# Oracle-specific options  
jdbc_options:
  fetchsize: 50000         # Larger fetch for performance
  numPartitions: 16        # More partitions for large tables
  partitionColumn: "ID"    # Column to partition on
  lowerBound: 1           # Partition range
  upperBound: 1000000     # Partition range
  customSchema: "id DECIMAL(38,0), name STRING"  # Custom schema
```

## Running Source Ingestion

### Single Source

```bash
# Ingest specific source
python -m src.cli run-bronze-ingestion --source daily_sales_excel

# With custom batch ID
python -m src.cli run-bronze-ingestion --source daily_sales_excel --batch-id "20240115_manual"

# Overwrite mode instead of append
python -m src.cli run-bronze-ingestion --source daily_sales_excel --write-mode overwrite
```

### Oracle Multi-Table

```bash
# Ingest all 7 Oracle tables
python -m src.cli run-bronze-ingestion --source oracle_erp_system

# Continue on individual table errors
python -m src.cli run-bronze-ingestion --source oracle_erp_system --continue-on-error
```

### Validation

```bash
# Validate source configuration before running
python -m src.cli validate-source-config --source oracle_erp_system

# List all configured sources
python -m src.cli list-sources
```

## Best Practices

### 1. **Naming Conventions**
```yaml
# Use descriptive names that indicate:
# - Source system: oracle, excel, csv
# - Data type: sales, customers, inventory  
# - Frequency: daily, weekly, monthly

daily_sales_excel:         # Good
customer_master_oracle:    # Good
dse:                      # Bad - unclear
```

### 2. **Schema Definition**
```yaml
# Always define schema for data quality
# Use appropriate data types
# Set nullable constraints
# Add validation patterns for critical fields
```

### 3. **Incremental Loading**
```yaml
# Use incremental loading for large tables
# Choose appropriate key columns (timestamps preferred)
# Set up watermark tracking
# Consider late-arriving data scenarios
```

### 4. **Error Handling**
```yaml
# Configure appropriate error handling
# Set up file tracking for reprocessing
# Use data quality checks to catch issues early
# Plan for schema evolution
```

### 5. **Performance Optimization**
```yaml
# Use appropriate batch sizes
# Configure partitioning for large datasets
# Optimize JDBC fetch sizes
# Consider parallel processing options
```