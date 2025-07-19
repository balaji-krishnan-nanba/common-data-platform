# Common Data Platform

A modular, configuration-driven data ingestion framework for Azure Databricks Lakehouse that follows industry best practices with bronze/silver/gold medallion architecture.

## Overview

This framework provides a production-ready solution for ingesting data from various sources (Excel, CSV, Oracle, etc.) into a Unity Catalog-based lakehouse architecture. It supports both PySpark and SparkSQL transformations, includes SCD Type 2 implementation, and integrates seamlessly with Azure services.

## Key Features

- **Configuration-Driven**: No hardcoded values, everything externalized to YAML
- **Modular Design**: Clean separation between connectivity, ingestion, and transformation
- **Multi-Source Support**: Excel, CSV, Oracle with easy extensibility
- **Schema Enforcement**: Define and validate schemas for all sources
- **SCD Type 2**: Built-in slowly changing dimension support
- **Data Quality**: Comprehensive validation and quality checks
- **Security**: Azure Key Vault integration for credentials
- **CI/CD Ready**: Databricks Asset Bundle and GitHub Actions integration

## Architecture

### Catalog Structure
```
Unity Catalog
├── {project-code}-{env}-bronze/     # Raw data ingestion
│   ├── excel_data/
│   ├── csv_data/
│   └── oracle_data/
├── {project-code}-{env}-silver/     # Cleansed and conformed data
│   ├── excel_data/
│   ├── csv_data/
│   └── oracle_data/
└── {project-code}-{env}-gold/       # Business-ready analytics data
    ├── analytics/
    └── reporting/
```

### Project Structure
```
common-data-platform/
├── config/                          # Configuration files
│   ├── environments/               # Environment-specific configs
│   ├── sources/                   # Source definitions
│   └── transformations/           # Transformation rules
├── devops/                         # DevOps & Databricks Asset Bundle
│   ├── databricks.yml            # Main DAB configuration
│   ├── environments/             # Environment-specific DAB configs
│   ├── resources/                # Jobs, clusters, permissions
│   ├── variables/                # Variable definitions
│   ├── templates/                # Reusable templates
│   └── scripts/                  # DevOps automation scripts
├── src/                           # Source code
│   ├── core/                     # Core framework modules
│   ├── connectivity/             # Data source connectors
│   ├── ingestion/               # Ingestion engines
│   ├── transformation/          # Transformation engines
│   └── utilities/               # Utility modules
├── scripts/                      # Deployment and execution scripts
└── tests/                       # Test suite
```

## Quick Start

### 1. Setup Environment

```bash
# Clone the repository
git clone <repository-url>
cd common-data-platform

# Install dependencies
pip install -r requirements.txt

# Install the package in development mode
pip install -e .
```

### 2. Configure Environment Variables

```bash
export PROJECT_CODE=cddp
export ENVIRONMENT=dev
export AZURE_STORAGE_ACCOUNT_DEV=mystorageaccount
export AZURE_KEY_VAULT_URL_DEV=https://myvault.vault.azure.net/
```

### 3. Provision Infrastructure

```bash
# Provision Unity Catalog infrastructure
python scripts/provision_infrastructure.py --environments dev test prod
```

### 4. Configure Data Sources

Edit configuration files in `config/sources/` to define your data sources:

```yaml
# config/sources/excel_sources.yaml
daily_sales_excel:
  name: daily_sales_excel
  type: excel
  connection:
    storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}
    container: raw-data
    path_pattern: sales/daily/{year}/{month}/{day}/sales_*.xlsx
  schema:
    columns:
      - name: transaction_id
        type: string
        nullable: false
      # ... more columns
```

### 5. Run Ingestion

```bash
# Ingest Excel files to bronze layer
python scripts/run_ingestion_pipeline.py --source daily_sales_excel --layer bronze

# Transform to silver layer
python scripts/run_ingestion_pipeline.py --source daily_sales_excel --layer silver
```

## Configuration

### Source Configuration

Define data sources in `config/sources/`:

- `excel_sources.yaml` - Excel file configurations
- `csv_sources.yaml` - CSV file configurations  
- `oracle_sources.yaml` - Oracle database configurations

### Transformation Configuration

Define transformations in `config/transformations/`:

- `bronze_to_silver/` - Bronze to silver layer transformations
- `silver_to_gold/` - Silver to gold layer transformations

### Environment Configuration

Environment-specific settings in `config/environments/`:

- `dev.yaml` - Development environment
- `test.yaml` - Test environment
- `prod.yaml` - Production environment

## Data Sources

### Supported Sources

1. **Excel Files** - From Azure Data Lake Storage Gen2
2. **CSV Files** - From Azure Data Lake Storage Gen2
3. **Oracle Database** - Via JDBC connections
4. **Extensible** - Easy to add new connectors

### Example: Oracle Multi-Table Ingestion

```yaml
oracle_source:
  connection:
    host: oracle-server.company.com
    port: 1521
    service_name: PROD
    secret_scope: databricks-secrets
  tables:
    - name: customers
      source_schema: SALES
      source_table: CUSTOMERS
    - name: orders
      source_schema: SALES
      source_table: ORDERS
    # ... more tables
```

## Transformations

### PySpark Transformations

```python
def transform(spark: SparkSession, config: Dict) -> DataFrame:
    # Complex business logic with Python
    df = spark.table(config['source_table'])
    
    # Apply transformations
    result = df.withColumn("new_column", F.col("existing") * 2)
    
    return result
```

### SparkSQL Transformations

```sql
-- sql/bronze_to_silver/customer_cleansing.sql
SELECT 
    customer_id,
    UPPER(TRIM(customer_name)) as customer_name,
    COALESCE(email, 'unknown@email.com') as email,
    current_timestamp() as processed_at
FROM ${source_table}
WHERE customer_id IS NOT NULL
```

## SCD Type 2 Implementation

```yaml
change_data_capture:
  type: scd_type_2
  business_keys: [customer_id]
  tracked_columns: [name, email, address]
  scd_columns:
    valid_from: valid_from_date
    valid_to: valid_to_date
    is_current: is_current_flag
```

## Security

### Azure Key Vault Integration

All credentials are stored securely in Azure Key Vault:

```yaml
connection:
  secret_scope: databricks-secrets
  username_key: oracle_username
  password_key: oracle_password
```

## Orchestration

### Databricks Workflows

The framework integrates with Databricks Workflows for scheduling:

```yaml
# devops/databricks.yml (modular structure)
bundle:
  name: common-data-platform

include:
  - variables/*.yml
  - resources/**/*.yml

targets:
  dev:
    mode: development
  prod:
    mode: production
```

### GitHub Actions CI/CD

Automated deployment across environments:

```yaml
# .github/workflows/deploy.yml
- name: Deploy to Production
  run: |
    cd devops
    ./scripts/deploy.sh -t prod -f
```

## Data Quality

Built-in data quality checks:

```yaml
data_quality_checks:
  - type: null_check
    columns: [customer_id, email]
  - type: duplicate_check
    columns: [customer_id]
  - type: range_check
    column: amount
    min: 0
```

## Monitoring and Logging

- Structured logging with correlation IDs
- Data lineage tracking
- Performance metrics
- Error handling and alerting

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html
```

### Code Quality

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/
```

## DevOps & Deployment

### Modular Databricks Asset Bundle

The framework uses a modular DevOps structure with Databricks Asset Bundle (DAB):

```bash
# Navigate to DevOps directory
cd devops

# Validate configuration
./scripts/validate.sh

# Deploy to development
./scripts/deploy.sh -t dev

# Run tests
./scripts/test.sh -t dev

# Deploy to production
./scripts/deploy.sh -t prod
```

**Key Benefits:**
- 🏗️ **Modular Structure**: Separate files for jobs, clusters, environments
- 🔄 **Reusable Templates**: Consistent configurations across resources
- 🚀 **DevOps Ready**: Automated validation, testing, and deployment
- 📊 **Environment Management**: Clear dev/test/prod separation

For detailed DevOps documentation, see: [`devops/README.md`](devops/README.md)

### Local Development

1. Set environment variables
2. Run `python scripts/provision_infrastructure.py`
3. Configure data sources in `config/`
4. Deploy bundle: `cd devops && ./scripts/deploy.sh -t dev`

### Production Deployment

1. Configure Azure resources
2. Set up Databricks workspace
3. Deploy using Databricks Asset Bundles
4. Schedule workflows

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and add tests
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions and support:

- Create an issue in the repository
- Contact the Data Engineering team
- Check the documentation in the `docs/` directory