# Configuration Guide

This comprehensive guide covers all aspects of configuring the Common Data Platform framework, from data sources to transformations and environments.

## Overview

The framework uses a **configuration-driven approach** where all behavior is defined through YAML files. No hardcoded values exist in the codebase, making it highly flexible and environment-agnostic.

## Configuration Structure

```
config/
├── environments/           # Environment-specific settings
│   ├── dev.yaml
│   ├── test.yaml
│   └── prod.yaml
├── sources/               # Data source definitions
│   ├── excel_sources.yaml
│   ├── csv_sources.yaml
│   └── oracle_sources.yaml
└── transformations/       # Transformation rules
    ├── bronze_to_silver/
    │   ├── excel_transformations.yaml
    │   ├── csv_transformations.yaml
    │   └── oracle_transformations.yaml
    └── silver_to_gold/
        ├── customer_360.yaml
        ├── sales_analytics.yaml
        └── operational_metrics.yaml
```

## Environment Variables

The framework uses a layered approach for environment variable management in Databricks:

### **Variable Types and Storage**

| Variable Type | Storage Location | Example | Description |
|---------------|------------------|---------|-------------|
| **Secrets** | Databricks Secret Scopes (Key Vault) | `azure-client-secret` | Sensitive credentials |
| **Bundle Variables** | `databricks.yml` | `PROJECT_CODE`, `ENVIRONMENT` | Build-time configuration |
| **System Variables** | Environment/CI-CD | `AZURE_TENANT_ID` | Deployment-time values |

### **Environment Variables Reference**

| Variable | Description | Example | Storage |
|----------|-------------|---------|---------|
| `PROJECT_CODE` | 4-letter project identifier | `cddp` | Bundle Variable |
| `ENVIRONMENT` | Current environment | `dev`, `test`, `prod` | Bundle Variable |
| `AZURE_STORAGE_ACCOUNT_{ENV}` | Storage account per environment | `mystoragedev` | System Variable |
| `AZURE_KEY_VAULT_URL_{ENV}` | Key Vault URL per environment | `https://vault-dev.vault.azure.net/` | System Variable |
| `AZURE_TENANT_ID` | Azure AD tenant ID for Service Principal auth | `12345678-1234-1234-1234-123456789012` | System Variable |
| `DATABRICKS_HOST_{ENV}` | Databricks workspace URL | `https://adb-xxx.azuredatabricks.net` | System Variable |

### **Databricks Asset Bundle Configuration**

In `databricks.yml`, environment variables are managed through the bundle system:

```yaml
variables:
  project_code: cddp
  azure_tenant_id: ${AZURE_TENANT_ID}  # From system environment

targets:
  dev:
    variables:
      environment: dev
      azure_tenant_id: ${AZURE_TENANT_ID}
      azure_storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}
      azure_key_vault_url: ${AZURE_KEY_VAULT_URL_DEV}
  
  prod:
    variables:
      environment: prod
      azure_tenant_id: ${AZURE_TENANT_ID}
      azure_storage_account: ${AZURE_STORAGE_ACCOUNT_PROD}
      azure_key_vault_url: ${AZURE_KEY_VAULT_URL_PROD}

resources:
  jobs:
    job_clusters:
      - job_cluster_key: ingestion_cluster
        new_cluster:
          spark_env_vars:
            PROJECT_CODE: ${var.project_code}
            ENVIRONMENT: ${var.environment}
            AZURE_TENANT_ID: ${var.azure_tenant_id}
```

### **Setting Up Environment Variables**

#### **Local Development**
```bash
# Create .env file (not committed to git)
export AZURE_TENANT_ID=12345678-1234-1234-1234-123456789012
export AZURE_STORAGE_ACCOUNT_DEV=mystoragedev
export AZURE_KEY_VAULT_URL_DEV=https://vault-dev.vault.azure.net/
export DATABRICKS_HOST_DEV=https://adb-xxx.azuredatabricks.net

# Load variables
source .env

# Deploy bundle
databricks bundle deploy -t dev
```

#### **CI/CD Pipeline**
```yaml
# GitHub Actions example
env:
  AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
  AZURE_STORAGE_ACCOUNT_DEV: ${{ vars.AZURE_STORAGE_ACCOUNT_DEV }}
  DATABRICKS_HOST_DEV: ${{ vars.DATABRICKS_HOST_DEV }}

steps:
  - name: Deploy to Databricks
    run: databricks bundle deploy -t dev
```

## Variable Substitution

The framework supports variable substitution in configuration files:

```yaml
# In any YAML file, use ${variable_name} syntax
target:
  catalog: ${project_code}-${environment}-bronze  # Resolves to: cddp-dev-bronze
  
connection:
  storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}   # Resolves to environment variable
```

## Environment Configuration

### Development Environment (`config/environments/dev.yaml`)

```yaml
# Development environment - more lenient settings
azure:
  storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}
  key_vault_url: ${AZURE_KEY_VAULT_URL_DEV}
  secret_scope: databricks-secrets-dev

spark:
  max_workers: 2
  node_type: Standard_DS3_v2
  
data_quality:
  fail_on_error: false      # Continue on quality issues
  max_error_threshold: 10   # Allow more errors
  
logging:
  level: DEBUG              # Verbose logging
  
retry_policy:
  max_attempts: 2           # Fewer retries for faster feedback
  backoff_seconds: 30
```

### Production Environment (`config/environments/prod.yaml`)

```yaml
# Production environment - strict settings
azure:
  storage_account: ${AZURE_STORAGE_ACCOUNT_PROD}
  key_vault_url: ${AZURE_KEY_VAULT_URL_PROD}
  secret_scope: databricks-secrets-prod

spark:
  max_workers: 8
  node_type: Standard_DS4_v2
  
data_quality:
  fail_on_error: true       # Fail on any quality issues
  max_error_threshold: 0    # Zero tolerance for errors
  
logging:
  level: INFO               # Standard logging
  
retry_policy:
  max_attempts: 3           # More retries for reliability
  backoff_seconds: 120
```

## Catalog Naming Convention

The framework uses a consistent naming pattern:

```
{project_code}-{environment}-{layer}
```

Examples:
- `cddp-dev-bronze` - Development bronze layer
- `cddp-prod-silver` - Production silver layer
- `finp-test-gold` - Finance project test gold layer

### Changing Project Code

To use a different project code:

1. **Set environment variable**:
   ```bash
   export PROJECT_CODE=finp  # Finance Platform
   ```

2. **Or configure in databricks.yml**:
   ```yaml
   variables:
     project_code: finp
   ```

## Schema Organization

Within each catalog, schemas are organized by source system:

```
cddp-dev-bronze/
├── excel_data/           # Excel file sources
├── csv_data/            # CSV file sources
├── oracle_data/         # Oracle database sources
└── system/              # Framework system tables
```

## Configuration Validation

### Built-in Validation

The framework automatically validates:
- Required fields are present
- Data types are correct
- Secret references are valid
- Catalog/schema names follow conventions

### Manual Validation

```bash
# Validate specific source configuration
python -m src.cli validate-source-config --source my_source

# List all configured sources
python -m src.cli list-sources
```

## Configuration Best Practices

### 1. **Use Descriptive Names**

```yaml
# Good - descriptive and clear
daily_sales_excel:
  name: daily_sales_excel
  
# Bad - unclear abbreviations
dse:
  name: dse
```

### 2. **Group Related Sources**

```yaml
# Keep related sources in the same configuration file
# excel_sources.yaml for all Excel sources
# oracle_sources.yaml for all Oracle sources
```

### 3. **Use Consistent Naming**

```yaml
# Follow consistent naming patterns
source_name:
  target:
    schema: source_data      # matches source system
    table: source_table_name # descriptive table name
```

### 4. **Document Complex Configurations**

```yaml
# Add comments for complex business logic
customer_data_scd2:
  # SCD Type 2 for customer master data
  # Tracks changes in customer attributes over time
  change_data_capture:
    type: scd_type_2
```

### 5. **Use Environment-Specific Overrides**

```yaml
# Base configuration
batch_size: 10000

# Override in prod environment
# config/environments/prod.yaml
oracle:
  batch_size: 100000  # Larger batches in production
```

## Configuration Templates

### New Data Source Template

```yaml
new_source_name:
  name: new_source_name
  type: [excel|csv|oracle|...]
  
  connection:
    # Connection details specific to source type
    
  schema:
    strict_validation: true
    columns:
      - name: column_name
        type: data_type
        nullable: true/false
        
  target:
    catalog: ${project_code}-${environment}-bronze
    schema: source_data
    table: table_name
    
  # Optional configurations
  ingestion:
    mode: [incremental|full]
    
  file_tracking:
    method: database
    tracking_table: ${project_code}-${environment}-bronze.system.processed_files
```

### Transformation Template

```yaml
transformation_name:
  source:
    catalog: ${project_code}-${environment}-bronze
    schema: source_schema
    table: source_table
    
  target:
    catalog: ${project_code}-${environment}-silver
    schema: target_schema
    table: target_table
    
  engine: [sparksql|pyspark]
  
  transformations:
    - type: data_quality_checks
      checks:
        - type: null_check
          columns: [required_columns]
          
    - type: standardization
      rules:
        column_name:
          - action: specific_rule
```

## Advanced Configuration

### Custom Spark Configuration

```yaml
# In environment configuration
spark:
  # Standard settings
  max_workers: 4
  node_type: Standard_DS3_v2
  
  # Custom Spark configurations
  spark_conf:
    "spark.sql.adaptive.enabled": "true"
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
    "spark.databricks.delta.optimizeWrite.enabled": "true"
```

### Multi-Environment Secrets

```yaml
# Different secret scopes per environment
connection:
  secret_scope: databricks-secrets-${environment}  # Resolves to databricks-secrets-dev
  username_key: oracle-username
  password_key: oracle-password
```

### Conditional Configuration

```yaml
# Different settings based on environment
ingestion:
  mode: "{{ 'incremental' if environment == 'prod' else 'full' }}"
  batch_size: "{{ 100000 if environment == 'prod' else 10000 }}"
```

## Configuration Management

### Version Control

```bash
# All configuration files should be in version control
git add config/
git commit -m "Add new Oracle source configuration"
```

### Environment Promotion

```bash
# Test configuration in dev first
python -m src.cli validate-source-config --source new_source --environment dev

# Deploy to test environment
export ENVIRONMENT=test
python scripts/provision_infrastructure.py --environments test

# Finally promote to production
export ENVIRONMENT=prod
databricks bundle deploy -t prod
```

### Configuration Backup

```bash
# Backup current configuration
cp -r config/ config_backup_$(date +%Y%m%d)

# Or use git tags for versioning
git tag -a v1.2.0 -m "Configuration for release 1.2.0"
```

## Troubleshooting Configuration

### Common Issues

1. **Variable Not Resolved**
   ```bash
   # Check environment variables are set
   echo $PROJECT_CODE
   echo $ENVIRONMENT
   ```

2. **Invalid YAML Syntax**
   ```bash
   # Validate YAML syntax
   python -c "import yaml; yaml.safe_load(open('config/sources/excel_sources.yaml'))"
   ```

3. **Secret Not Found**
   ```bash
   # Check secret scope exists
   databricks secrets list-scopes
   databricks secrets list --scope databricks-secrets-dev
   ```

### Debug Mode

```bash
# Run with debug logging to see configuration resolution
python -m src.cli run-bronze-ingestion --source my_source --log-level DEBUG
```

This will show how variables are resolved and which configuration files are loaded.