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

# Choose your installation method:

# Option A: Production deployment with pinned versions
pip install -r requirements.txt

# Option B: Development environment with all tools
pip install -r requirements-dev.txt

# Option C: Databricks-specific deployment
pip install -r requirements-databricks.txt

# Option D: Package building and development (modern approach)
pip install -e .

# Option E: Development with optional dependencies groups
pip install -e ".[dev,databricks,quality,oracle]"
```

**When to use which installation:**
- **requirements.txt**: Production deployments requiring exact version reproducibility
- **requirements-dev.txt**: Local development with linting, testing, and debugging tools
- **requirements-databricks.txt**: Databricks cluster deployments with platform-specific tools
- **pyproject.toml** (pip install -e .): Package building, modern tooling, and flexible development

### 2. Configure Environment Variables

```bash
# Core Framework Configuration
export PROJECT_CODE=cddp  # Your 4-letter project identifier
export ENVIRONMENT=dev    # Current environment (dev/test/prod)

# Core Azure configuration
export AZURE_TENANT_ID=your-tenant-id

# Development Environment
export AZURE_STORAGE_ACCOUNT_DEV=yourstorageaccountdev
export AZURE_KEY_VAULT_URL_DEV=https://yourvault-dev.vault.azure.net/
export AZURE_KEY_VAULT_SCOPE_DEV=databricks-secrets-dev
export DATABRICKS_HOST_DEV=https://your-workspace.azuredatabricks.net

# Test Environment
export AZURE_STORAGE_ACCOUNT_TEST=yourstorageaccounttest
export AZURE_KEY_VAULT_URL_TEST=https://yourvault-test.vault.azure.net/
export AZURE_KEY_VAULT_SCOPE_TEST=databricks-secrets-test
export DATABRICKS_HOST_TEST=https://your-test-workspace.azuredatabricks.net

# Production Environment
export AZURE_STORAGE_ACCOUNT_PROD=yourstorageaccountprod
export AZURE_KEY_VAULT_URL_PROD=https://yourvault-prod.vault.azure.net/
export AZURE_KEY_VAULT_SCOPE_PROD=databricks-secrets-prod
export DATABRICKS_HOST_PROD=https://your-prod-workspace.azuredatabricks.net

# Notification Configuration
export NOTIFICATION_EMAILS='["your-email@company.com","team@company.com"]'

# Authentication (for local development)
export DATABRICKS_TOKEN=your-personal-access-token
```

### 3. Provision Infrastructure

```bash
# Option A: Use the Unity Catalog setup notebook (Recommended)
# Run the notebooks/setup/unity_catalog_setup.py in Databricks
# This creates catalogs, schemas, and system tables with proper parameterization

# Option B: Manual provisioning (if available)
# python scripts/provision_infrastructure.py --environments dev test prod
```

### 4. Deploy Using Databricks Asset Bundle

```bash
# Navigate to DevOps directory
cd devops

# Validate configuration
./scripts/validate.sh dev

# Deploy to development
./scripts/deploy.sh -t dev

# Run tests
./scripts/test.sh dev
```

### 5. Configure Data Sources

Edit configuration files in `devops/config/sources/` to define your data sources:

```yaml
# devops/config/sources/excel_sources.yaml
daily_sales_excel:
  name: daily_sales_excel
  type: excel
  connection:
    storage_account: ${var.azure_storage_account}
    container: raw-data
    path_pattern: sales/daily/{year}/{month}/{day}/sales_*.xlsx
    secret_scope: ${var.azure_key_vault_scope}
    service_principal:
      client_id_key: azure-client-id
      client_secret_key: azure-client-secret
      tenant_id: ${var.azure_tenant_id}
  schema:
    columns:
      - name: transaction_id
        type: string
        nullable: false
      # ... more columns
  target:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: daily_sales
```

### 6. Run Ingestion

```bash
# Using the CLI (if available)
python -m src.cli run-bronze-ingestion --source daily_sales_excel
python -m src.cli run-silver-transformation --source daily_sales_excel

# Validate source configuration
python -m src.cli validate-source-config --source daily_sales_excel

# List all configured sources
python -m src.cli list-sources

# Using Databricks Notebooks (Recommended)
# Run notebooks/examples/02_excel_pipeline_test.py in Databricks
# This provides step-by-step execution with monitoring and validation
```

## Configuration

### Source Configuration

Define data sources in `devops/config/sources/`:

- `excel_sources.yaml` - Excel file configurations (Azure Data Lake)
- `csv_sources.yaml` - CSV file configurations (Azure Data Lake)  
- `oracle_sources.yaml` - Oracle database configurations (JDBC)

### Transformation Configuration

Define transformations in `devops/config/transformations/`:

- `bronze_to_silver/` - Bronze to silver layer transformations
- `silver_to_gold/` - Silver to gold layer transformations

### Environment Configuration

Environment-specific settings in `devops/environments/`:

- `dev.yml` - Development environment
- `test.yml` - Test environment  
- `prod.yml` - Production environment
- `base.yml` - Common patterns and templates

### Databricks Asset Bundle Configuration

Main configuration in `devops/`:

- `databricks.yml` - Main DAB configuration
- `variables/` - Variable definitions (azure.yml, common.yml, spark.yml)
- `resources/` - Jobs, clusters, and permissions
- `templates/` - Reusable configuration templates

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

Automated deployment across environments with comprehensive validation:

```yaml
# .github/workflows/databricks-cicd.yml
# Automatic deployment:
# - Pull requests -> Development environment
# - Push to main -> Test environment  
# - Manual trigger -> Production environment
```

The pipeline includes:
- Configuration validation (no connectivity tests)
- Bundle deployment with Git metadata
- Smoke testing without sample data
- Environment promotion workflow

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

1. Install dependencies: `pip install -e ".[dev,databricks]"`
2. Set environment variables (see configuration section)
3. Run `python scripts/provision_infrastructure.py --environments dev`
4. Configure data sources in `config/sources/`
5. Deploy bundle: `cd devops && ./scripts/deploy.sh -t dev`

### Production Deployment

1. Configure Azure resources and service principals
2. Set up Databricks workspaces (dev/test/prod)
3. Configure GitHub secrets for CI/CD
4. Deploy via GitHub Actions workflow
5. Monitor via Databricks job runs and Delta table logs

## 🎯 Design Principles

This platform follows key design principles for simplicity and maintainability:

- **Single Configuration Pattern**: Uses only Databricks Asset Bundle (no duplicate configs)
- **Service Principal Authentication**: No storage account keys (security best practice)  
- **Delta Table Logging**: No Azure Log Analytics dependency (simplified approach)
- **Environment Variable Management**: Consistent patterns across all environments
- **Simplified Testing**: Configuration validation only (no connectivity/sample data tests)
- **Modular DevOps**: Industry-standard structure with reusable components

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