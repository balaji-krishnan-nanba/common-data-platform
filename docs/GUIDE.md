# Common Data Platform - Complete Guide

A simple, production-ready data ingestion framework for Azure Databricks.

## Quick Start (5 minutes)

### 1. Install
```bash
pip install common-data-platform
```

### 2. Set Environment Variables
```bash
export PROJECT_CODE=myproject
export ENVIRONMENT=dev
export AZURE_STORAGE_ACCOUNT=mystorageaccount
```

### 3. Run Your First Pipeline
```bash
# Ingest Excel file with minimal config
cdp run-bronze-ingestion --inline-config '{
  "name": "sales_data",
  "type": "excel",
  "connection": {
    "storage_account": "myaccount",
    "container": "raw-data",
    "path": "sales/*.xlsx"
  }
}'
```

That's it! Your data is now in the Bronze layer.

## Architecture Overview

The platform follows the medallion architecture:
- **Bronze**: Raw data ingestion
- **Silver**: Cleaned and standardized data
- **Gold**: Business-ready aggregates

Data flows: `Source → Bronze → Silver → Gold`

All tables are stored in Unity Catalog with the naming convention:
`{project}-{env}-{layer}.{schema}.{table}`

## Configuration

### Option 1: Inline Configuration (Simplest)
```bash
cdp run-bronze-ingestion --inline-config '{
  "name": "my_data",
  "type": "csv",
  "connection": {
    "storage_account": "account",
    "container": "data",
    "path": "files/*.csv"
  }
}'
```

### Option 2: Configuration Files
Create `devops/config/sources/excel_sources.yaml`:
```yaml
sales_excel:
  name: sales_excel
  connection:
    storage_account: myaccount
    container: raw-data
    path: sales/*.xlsx
```

Run:
```bash
cdp run-bronze-ingestion --source sales_excel
```

### Sensible Defaults
The platform provides smart defaults:
- `ingestion.mode`: "full"
- `ingestion.batch_size`: 100
- `target.catalog`: Auto-generated based on project/env
- `target.schema`: "{source_type}_data"
- `read_options.header`: true (for CSV/Excel)
- `read_options.inferSchema`: true

## Supported Sources

### Excel Files
```yaml
my_excel:
  name: my_excel
  type: excel
  connection:
    storage_account: account
    container: container
    path: path/to/*.xlsx
  read_options:
    sheet_name: "Sheet1"  # Optional
```

### CSV Files
```yaml
my_csv:
  name: my_csv
  type: csv
  connection:
    storage_account: account
    container: container
    path: path/to/*.csv
```

### Oracle Database
```yaml
my_oracle:
  name: my_oracle
  type: oracle
  connection:
    host: oracle.company.com
    port: 1521
    service_name: PROD
    schema: SALES
    table: ORDERS
    user: ${ORACLE_USER}
    password: ${ORACLE_PASSWORD}
  ingestion:
    mode: incremental
    watermark_column: last_updated
```

## Transformations

### SQL Transformations
Create `devops/config/transformations/bronze_to_silver/excel_transformations.yaml`:
```yaml
clean_sales:
  engine: sparksql
  source:
    schema: excel_data
    table: sales
  target:
    schema: silver_sales
    table: sales_cleaned
  query: |
    SELECT 
      transaction_id,
      CAST(transaction_date AS DATE) as transaction_date,
      customer_id,
      total_amount
    FROM {source_table}
    WHERE total_amount > 0
```

Run:
```bash
cdp run-silver-transformation --source excel
```

### PySpark Transformations
```yaml
aggregate_sales:
  engine: pyspark
  transform_type: aggregation
  source:
    schema: silver_sales
    table: sales_cleaned
  target:
    schema: gold_sales
    table: daily_summary
  group_by: [transaction_date]
  aggregations:
    total_amount: sum
    transaction_id: count
```

## Deployment

### Local Development
```bash
# Install in development mode
pip install -e .

# Run with local Spark
cdp run-bronze-ingestion --source my_source
```

### Databricks
1. Upload wheel to DBFS:
```bash
databricks fs cp dist/common_data_platform-*.whl dbfs:/libraries/
```

2. Create job with spark_python_task:
```json
{
  "spark_python_task": {
    "python_file": "dbfs:/libraries/common_data_platform-*.whl",
    "parameters": ["run-bronze-ingestion", "--source", "my_source"]
  }
}
```

### Unity Catalog Setup
The platform automatically:
- Creates catalogs: `{project}-{env}-{bronze|silver|gold}`
- Creates schemas based on source type
- Manages table creation with proper permissions

## Monitoring

### Check Pipeline Status
```bash
# View recent runs
cdp list-runs --limit 10

# Check specific run
cdp get-run --run-id <id>
```

### Data Quality
Built-in validations:
- Schema validation
- Null checks
- Constraint validation
- Row count verification

### Performance
Automatic optimizations:
- Delta table optimization
- Z-ordering on commonly filtered columns
- Adaptive query execution
- Dynamic file pruning

## Troubleshooting

### Common Issues

**Issue**: "Table not found"  
**Solution**: Ensure catalog and schema exist, check permissions

**Issue**: "Schema mismatch"  
**Solution**: Set `overwriteSchema: true` in write_options

**Issue**: "Out of memory"  
**Solution**: Reduce batch_size in ingestion config

**Issue**: "Permission denied"  
**Solution**: Check Unity Catalog permissions for your service principal

### Debug Mode
```bash
# Enable debug logging
cdp --log-level DEBUG run-bronze-ingestion --source my_source
```

## Security Best Practices

1. **Never hardcode credentials** - Use environment variables or secret scopes
2. **Use service principals** for production workloads
3. **Enable audit logging** in Unity Catalog
4. **Restrict catalog access** based on environment
5. **Validate all SQL inputs** to prevent injection

## Advanced Features

### Incremental Loading
```yaml
ingestion:
  mode: incremental
  watermark_column: last_modified
```

### CDC (Change Data Capture)
```yaml
ingestion:
  mode: cdc
  cdc_table: SALES.CDC_ORDERS
```

### Custom Transformations
```python
# transformations/custom/clean_data.py
def transform(df):
    return df.filter(df.amount > 0) \
             .withColumn("processed_date", current_date())
```

### Distributed Processing
```yaml
ingestion:
  batch_size: 1000
  num_partitions: 20
  partition_column: date
```

## API Reference

### CLI Commands
- `cdp run-bronze-ingestion`: Ingest data to Bronze layer
- `cdp run-silver-transformation`: Transform Bronze to Silver
- `cdp run-gold-transformation`: Create Gold layer aggregates
- `cdp validate-source-config`: Validate configuration
- `cdp list-sources`: List configured sources

### Configuration Schema
- `name`: Source identifier
- `type`: Source type (excel, csv, oracle)
- `connection`: Connection details
- `schema`: Optional schema definition
- `ingestion`: Ingestion settings
- `target`: Target table configuration

## Support

- GitHub Issues: https://github.com/company/common-data-platform/issues
- Documentation: This file
- Version: 1.0.0

---

Built for simplicity, designed for scale.