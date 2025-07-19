# Modular Data Ingestion Framework for Azure Databricks Lakehouse

## Table of Contents
1. [Overview](#overview)
2. [Requirements](#requirements)
3. [Architecture](#architecture)
4. [Project Structure](#project-structure)
5. [Implementation Details](#implementation-details)
6. [Configuration Management](#configuration-management)
7. [Security](#security)
8. [Data Flow](#data-flow)
9. [SCD Type 2 Implementation](#scd-type-2-implementation)
10. [Orchestration](#orchestration)
11. [Best Practices](#best-practices)

## Overview

This document describes a configuration-driven, modular data ingestion framework for Azure Databricks Lakehouse that follows industry best practices with a bronze/silver/gold medallion architecture.

## Requirements

### 1. Architecture & Catalogs
- Use Unity Catalog with three catalogs (bronze, silver, gold) across dev, test, and prod environments
- Each source system (e.g. Excel, CSV, Oracle) lives in its own database within each catalog
- Automated Provisioning: standalone Python/SQL script that programmatically creates all catalogs and tables

### 2. Data Sources
- Excel & CSV files ingested from ADLS Gen2
- Oracle via JDBC
- Framework must be easily extensible to add new file or database sources

### 3. Layer Responsibilities
- **Bronze**: Raw data ingestion; no business transformations
- **Silver**: Cleansed, conformed tables; hooks for per-source transformations (PySpark/SparkSQL)
- **Gold**: Curated datasets with full business logic (PySpark/SparkSQL)

### 4. Code Structure & Best Practices
- Modular design with separate modules for connectivity, ingestion, transformation, utilities, and configuration
- Python best practices: try/except blocks, typing hints, docstrings, consistent naming
- Centralized configuration (YAML/JSON) for source definitions, connection secrets, transformation rules
- Enforce shared coding style (Black, isort, flake8)

### 5. Security & Secrets
- Azure Key Vault-backed scope for credentials and connection strings

### 6. Metadata & Configuration
- All behavior driven by external config files (no hardcoded paths or SQL)
- Configurable retry policies, schema mappings, and transformation flags per source and layer

## Architecture

### Catalog Naming Convention
Catalogs follow the format: `<4-letter-project-code>-<environment>-<medallion-layer>`

Examples:
- `cddp-dev-bronze`
- `cddp-test-silver`
- `cddp-prod-gold`

The project code is configurable and can be changed for different projects (e.g., `finp` for Finance Platform, `salp` for Sales Platform).

### Unity Catalog Structure
```
Unity Catalog
├── cddp-dev-bronze/
│   ├── excel_data/
│   ├── csv_data/
│   └── oracle_data/
├── cddp-dev-silver/
│   ├── excel_data/
│   ├── csv_data/
│   └── oracle_data/
├── cddp-dev-gold/
│   ├── analytics/
│   └── reporting/
└── ... (test and prod environments)
```

## Project Structure

```
common-data-platform/
├── config/
│   ├── environments/
│   │   ├── dev.yaml
│   │   ├── test.yaml
│   │   └── prod.yaml
│   ├── sources/
│   │   ├── excel_sources.yaml
│   │   ├── csv_sources.yaml
│   │   └── oracle_sources.yaml
│   └── transformations/
│       ├── bronze_to_silver/
│       │   ├── excel_transformations.yaml
│       │   ├── csv_transformations.yaml
│       │   └── oracle_transformations.yaml
│       └── silver_to_gold/
│           ├── customer_360.yaml
│           ├── sales_analytics.yaml
│           └── operational_metrics.yaml
├── src/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config_manager.py
│   │   ├── catalog_manager.py
│   │   └── secret_manager.py
│   ├── connectivity/
│   │   ├── __init__.py
│   │   ├── base_connector.py
│   │   ├── adls_connector.py
│   │   ├── oracle_connector.py
│   │   └── connector_factory.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── base_ingester.py
│   │   ├── file_ingester.py
│   │   ├── database_ingester.py
│   │   └── ingestion_orchestrator.py
│   ├── transformation/
│   │   ├── __init__.py
│   │   ├── base_transformer.py
│   │   ├── transformation_engine.py
│   │   ├── pyspark_transformer.py
│   │   ├── sparksql_transformer.py
│   │   ├── silver_transformer.py
│   │   ├── gold_transformer.py
│   │   ├── scd_type2_transformer.py
│   │   └── transformation_registry.py
│   ├── utilities/
│   │   ├── __init__.py
│   │   ├── logger.py
│   │   ├── error_handler.py
│   │   ├── data_quality.py
│   │   ├── schema_validator.py
│   │   └── metadata_tracker.py
│   ├── provisioning/
│   │   ├── __init__.py
│   │   ├── catalog_provisioner.py
│   │   └── table_provisioner.py
│   ├── sql/
│   │   ├── bronze_to_silver/
│   │   │   ├── excel_cleansing.sql
│   │   │   ├── csv_standardization.sql
│   │   │   └── oracle_conforming.sql
│   │   └── silver_to_gold/
│   │       ├── customer_aggregations.sql
│   │       ├── sales_metrics.sql
│   │       └── operational_kpis.sql
│   └── cli.py
├── scripts/
│   ├── provision_infrastructure.py
│   └── run_ingestion_pipeline.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── notebooks/
│   ├── examples/
│   └── monitoring/
├── requirements.txt
├── setup.py
├── pyproject.toml
├── databricks.yml
├── FRAMEWORK_DESIGN.md
└── README.md
```

## Implementation Details

### Dual Transformation Engine Support

The framework supports both PySpark and SparkSQL transformations:

**PySpark Transformations**:
- Object-oriented approach using DataFrame API
- Complex business logic with Python functions
- Machine learning preprocessing capabilities
- Custom UDFs for specialized transformations

**SparkSQL Transformations**:
- Declarative SQL-based transformations
- Stored in version-controlled .sql files
- Parameterized queries with Jinja2 templating
- Performance optimized for set-based operations

### Transformation Configuration Example

```yaml
# bronze_to_silver/excel_transformations.yaml
transformations:
  customer_data:
    source_table: bronze.excel_data.raw_customers
    target_table: silver.excel_data.cleansed_customers
    engine: sparksql  # or 'pyspark'
    sql_file: sql/bronze_to_silver/customer_cleansing.sql
    pyspark_module: transformations.excel.customer_cleansing
    data_quality_checks:
      - check_nulls: [customer_id, email]
      - check_duplicates: [customer_id]
```

## Configuration Management

### Environment Configuration

```yaml
# databricks.yml
bundle:
  name: common-data-platform
  
variables:
  project_code: cddp  # 4-letter code, easily changeable

targets:
  dev:
    mode: development
    workspace:
      host: ${DATABRICKS_HOST_DEV}
    variables:
      environment: dev
      
  test:
    mode: staging
    workspace:
      host: ${DATABRICKS_HOST_TEST}
    variables:
      environment: test
      
  prod:
    mode: production
    workspace:
      host: ${DATABRICKS_HOST_PROD}
    variables:
      environment: prod
```

### Source Configuration with Schema

```yaml
# config/sources/excel_sources.yaml
daily_sales_excel:
  type: excel
  connection:
    storage_account: mystorageaccount
    container: raw-data
    path_pattern: sales/daily/{year}/{month}/{day}/sales_*.xlsx
    
  schema:
    columns:
      - name: transaction_id
        type: string
        nullable: false
      - name: transaction_date
        type: date
        format: yyyy-MM-dd
      - name: customer_id
        type: string
      - name: product_code
        type: string
      - name: quantity
        type: integer
      - name: unit_price
        type: decimal(10,2)
      - name: total_amount
        type: decimal(10,2)
        
  ingestion:
    mode: incremental
    partition_by: transaction_date
    sheet_name: "Sales Data"
    
  target:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: daily_sales
```

### Oracle Multi-Table Configuration

```yaml
# config/sources/oracle_sources.yaml
oracle_source:
  connection:
    host: oracle-server.company.com
    port: 1521
    service_name: PROD
    secret_scope: databricks-secrets
    username_key: oracle_username
    password_key: oracle_password
  
  tables:
    - name: customers
      source_schema: SALES
      source_table: CUSTOMERS
      target_table: bronze.oracle_data.customers
      
    - name: orders
      source_schema: SALES
      source_table: ORDERS
      target_table: bronze.oracle_data.orders
      
    - name: order_items
      source_schema: SALES
      source_table: ORDER_ITEMS
      target_table: bronze.oracle_data.order_items
      
    # ... additional tables
```

## Security

### Azure Key Vault Integration

- All credentials stored in Azure Key Vault
- Databricks secret scopes backed by Key Vault
- RBAC integration for access control
- No hardcoded secrets in code or configuration

Example usage:
```python
# Secrets accessed via dbutils
password = dbutils.secrets.get(scope="databricks-secrets", key="oracle_password")
```

## Data Flow

### 1. Bronze Layer (Raw Ingestion)
- Direct copy of source data
- No transformations applied
- Schema validation only
- Audit columns added (ingestion_timestamp, source_file)

### 2. Silver Layer (Cleansing & Conforming)
- Data type standardization
- Null handling
- Duplicate removal
- Business rule validation
- SCD Type 2 for dimension tables

### 3. Gold Layer (Business Logic)
- Aggregations and KPIs
- Denormalized views
- ML feature engineering
- Business-specific calculations

## SCD Type 2 Implementation

### Configuration

```yaml
# config/transformations/bronze_to_silver/excel_transformations.yaml
customer_data_scd2:
  source:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: raw_customers
    
  target:
    catalog: ${project_code}-${environment}-silver
    schema: excel_data
    table: dim_customers_scd2
    
  change_data_capture:
    type: scd_type_2
    business_keys:
      - customer_id
    tracked_columns:
      - customer_name
      - email
      - phone
      - address
      - customer_segment
    update_only_columns:
      - last_login_date
      - total_purchases
    scd_columns:
      valid_from: valid_from_date
      valid_to: valid_to_date
      is_current: is_current_flag
      version: record_version
      hash: record_hash
```

### Silver Table Structure

```sql
CREATE TABLE cddp-dev-silver.excel_data.dim_customers_scd2 (
    -- Business columns
    customer_id STRING NOT NULL,
    customer_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    customer_segment STRING,
    
    -- Update-only columns
    last_login_date DATE,
    total_purchases DECIMAL(10,2),
    
    -- SCD2 metadata columns
    valid_from_date TIMESTAMP NOT NULL,
    valid_to_date TIMESTAMP,
    is_current_flag BOOLEAN NOT NULL,
    record_version INT NOT NULL,
    record_hash STRING NOT NULL,
    
    -- Audit columns
    source_file STRING,
    ingestion_timestamp TIMESTAMP,
    
    PRIMARY KEY (customer_id, record_version)
) USING DELTA
PARTITIONED BY (is_current_flag)
```

### Querying Historical Data

```sql
-- Current records only
SELECT * FROM cddp-prod-silver.excel_data.dim_customers_scd2
WHERE is_current_flag = true;

-- Historical view for specific date
SELECT * FROM cddp-prod-silver.excel_data.dim_customers_scd2
WHERE '2024-01-15' BETWEEN valid_from_date AND COALESCE(valid_to_date, '9999-12-31');
```

## Orchestration

### Databricks Workflows Integration

```yaml
# databricks.yml
resources:
  jobs:
    oracle_ingestion_workflow:
      name: "Oracle Data Ingestion Pipeline"
      
      tasks:
        - task_key: ingest_oracle_to_bronze
          job_cluster_key: ingestion_cluster
          python_wheel_task:
            package_name: common_data_platform
            entry_point: run_bronze_ingestion
            parameters: ["--source", "oracle_source"]
          
        - task_key: transform_to_silver
          depends_on:
            - task_key: ingest_oracle_to_bronze
          job_cluster_key: transformation_cluster
          python_wheel_task:
            package_name: common_data_platform
            entry_point: run_silver_transformation
            parameters: ["--source", "oracle_source"]
        
        - task_key: transform_to_gold
          depends_on:
            - task_key: transform_to_silver
          job_cluster_key: transformation_cluster
          python_wheel_task:
            package_name: common_data_platform
            entry_point: run_gold_transformation
            parameters: ["--source", "oracle_analytics"]
      
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"  # Daily at 6 AM
        timezone_id: "UTC"
```

### GitHub Actions CI/CD

```yaml
# .github/workflows/deploy.yml
name: Deploy to Databricks

on:
  push:
    branches: [main, develop]

jobs:
  deploy-dev:
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to Dev
        env:
          DATABRICKS_HOST_DEV: ${{ secrets.DATABRICKS_HOST_DEV }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_DEV }}
        run: |
          databricks bundle deploy -t dev
          
  deploy-prod:
    if: github.ref == 'refs/heads/main'
    needs: [deploy-test]
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to Prod
        env:
          DATABRICKS_HOST_PROD: ${{ secrets.DATABRICKS_HOST_PROD }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PROD }}
        run: |
          databricks bundle deploy -t prod
```

## Best Practices

### Data Engineering Best Practices
1. **Medallion Architecture**: Industry-standard bronze/silver/gold pattern
2. **Separation of Concerns**: Clear boundaries between layers
3. **Configuration-Driven**: No hardcoded values
4. **Idempotent Operations**: Safe to re-run
5. **Data Quality Checks**: Validation at each layer
6. **Lineage & Metadata**: Full tracking
7. **Scalable Design**: Leverages Spark's distributed computing
8. **Schema Evolution**: Handles changing data structures

### Python Best Practices
1. **Modular Structure**: Clean package organization
2. **Type Hints**: Full typing annotations
3. **Error Handling**: Comprehensive try/except blocks
4. **Factory Pattern**: Dynamic instantiation
5. **Abstract Base Classes**: Consistent interfaces
6. **Dependency Injection**: Testability
7. **PEP 8 Compliance**: Style guidelines
8. **Testing Structure**: Unit and integration tests

### Modern Engineering Standards
1. **12-Factor App Principles**: Environment-based config
2. **SOLID Principles**: Single responsibility, open/closed
3. **DRY**: Reusable components
4. **Security First**: Azure Key Vault integration
5. **CI/CD Ready**: Automated deployments
6. **Documentation**: Comprehensive docs
7. **Monitoring**: Structured logging

## Key Features Summary

1. **Simple Configuration**: YAML-driven, no code changes needed
2. **Flexible Transformations**: Support for both PySpark and SparkSQL
3. **Schema Management**: Define and enforce schemas for all sources
4. **History Tracking**: Built-in SCD Type 2 support
5. **Multi-Environment**: Dev/Test/Prod with easy promotion
6. **Extensible**: Easy to add new sources and transformations
7. **Production-Ready**: Error handling, logging, monitoring
8. **Cloud-Native**: Designed for Azure Databricks and Unity Catalog

## Usage Examples

### Ingest Oracle Tables
```bash
python scripts/run_ingestion_pipeline.py --source oracle_source --layer bronze
```

### Run Silver Transformations
```bash
python scripts/run_ingestion_pipeline.py --source oracle_source --layer silver
```

### Provision Infrastructure
```bash
python scripts/provision_infrastructure.py --environment prod
```

## Conclusion

This framework provides a production-ready, modular solution for data ingestion in Azure Databricks that:
- Follows industry best practices
- Remains simple to understand and use
- Scales with your data needs
- Maintains data quality and history
- Integrates seamlessly with modern data tools

The configuration-driven approach ensures that data engineers can focus on business logic rather than infrastructure code, while the modular design allows for easy extension and maintenance.