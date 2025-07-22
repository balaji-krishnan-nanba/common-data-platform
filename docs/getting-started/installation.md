# Installation Guide

This comprehensive guide walks you through setting up the Common Data Platform framework from scratch.

## Prerequisites

Before installing the framework, ensure you have the following:

### Azure Resources

| Resource | Purpose | Required |
|----------|---------|----------|
| **Azure Databricks Workspace** | Primary compute platform | ✅ Required |
| **Unity Catalog** | Data governance and cataloging | ✅ Required |
| **Azure Key Vault** | Secret management | ✅ Required |
| **Azure Data Lake Storage Gen2** | File storage for sources | ✅ Required |
| **Oracle Database** | Source system (if applicable) | ⚠️ Optional |

### Development Environment

| Tool | Version | Purpose |
|------|---------|---------|
| **Python** | 3.8+ | Framework runtime |
| **Git** | Latest | Version control |
| **Azure CLI** | Latest | Azure resource management |
| **Databricks CLI** | Latest | Databricks operations |

### Permissions Required

- **Azure Subscription**: Contributor role
- **Databricks Workspace**: Admin privileges
- **Key Vault**: Key Vault Administrator
- **Storage Account**: Storage Blob Data Contributor

## Installation Steps

### Step 1: Azure Resource Setup

#### 1.1 Create Azure Databricks Workspace

```bash
# Create resource group (if not exists)
az group create --name rg-data-platform --location eastus

# Create Databricks workspace with Unity Catalog
az databricks workspace create \
  --resource-group rg-data-platform \
  --name databricks-data-platform \
  --location eastus \
  --sku premium \
  --enable-no-public-ip true
```

#### 1.2 Enable Unity Catalog

```bash
# Create Unity Catalog metastore
az databricks metastore create \
  --name metastore-data-platform \
  --storage-root abfss://unity-catalog@yourstorageaccount.dfs.core.windows.net/metastore \
  --region eastus

# Assign metastore to workspace
az databricks metastore assignment create \
  --workspace-id /subscriptions/{subscription-id}/resourceGroups/rg-data-platform/providers/Microsoft.Databricks/workspaces/databricks-data-platform \
  --metastore-id {metastore-id}
```

#### 1.3 Create Key Vault

```bash
# Create Azure Key Vault
az keyvault create \
  --name kv-data-platform-dev \
  --resource-group rg-data-platform \
  --location eastus \
  --enable-rbac-authorization true

# Get Key Vault URL for configuration
az keyvault show --name kv-data-platform-dev --query properties.vaultUri
```

#### 1.4 Create Storage Account

```bash
# Create storage account
az storage account create \
  --name stadatplatformdev \
  --resource-group rg-data-platform \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Create containers
az storage container create --name raw-data --account-name stadatplatformdev
az storage container create --name processed-data --account-name stadatplatformdev
az storage container create --name unity-catalog --account-name stadatplatformdev
```

### Step 2: Framework Installation

#### 2.1 Clone Repository

```bash
# Clone the framework repository
git clone https://github.com/your-org/common-data-platform.git
cd common-data-platform

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

#### 2.2 Install Dependencies

Choose the appropriate installation method for your use case:

```bash
# Basic installation (core dependencies only)
pip install -e .

# Development environment with tools
pip install -e ".[dev]"

# Databricks-specific deployment
pip install -e ".[databricks]"

# Full development environment
pip install -e ".[dev,databricks,quality,oracle]"

# Production deployment with exact versions
pip install -r requirements-lock.txt

# Verify installation
python -c "import src.core.config_manager; print('Framework installed successfully')"
```

**Installation Methods Explained:**
- **`pip install -e .`**: Basic installation with core dependencies
- **`pip install -e ".[dev]"`**: Development tools (linting, testing, debugging)
- **`pip install -e ".[databricks]"`**: Databricks-specific features
- **`pip install -e ".[quality]"`**: Data quality tools (Great Expectations)
- **`pip install -e ".[oracle]"`**: Oracle database support
- **`requirements-lock.txt`**: Exact versions for reproducible production deployments

#### 2.3 Install Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure Databricks CLI (you'll need a personal access token)
databricks configure --token

# Test connection
databricks workspace list
```

### Step 3: Environment Configuration

#### 3.1 Set Environment Variables

Create a `.env` file (not committed to git):

```bash
# .env file
export PROJECT_CODE=cddp
export ENVIRONMENT=dev

# Azure Storage
export AZURE_STORAGE_ACCOUNT_DEV=stadatplatformdev

# Key Vault
export AZURE_KEY_VAULT_URL_DEV=https://kv-data-platform-dev.vault.azure.net/

# Databricks
export DATABRICKS_HOST_DEV=https://adb-1234567890123456.14.azuredatabricks.net
export DATABRICKS_TOKEN=your-databricks-personal-access-token

# Oracle (if using Oracle sources)
export ORACLE_HOST=your-oracle-server.com
export ORACLE_PORT=1521
export ORACLE_SERVICE_NAME=PROD
```

Load environment variables:

```bash
source .env
```

#### 3.2 Update Configuration Files

Edit `config/environments/dev.yaml`:

```yaml
azure:
  storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}
  key_vault_url: ${AZURE_KEY_VAULT_URL_DEV}
  secret_scope: databricks-secrets-dev

spark:
  max_workers: 2
  node_type: Standard_DS3_v2
  
data_quality:
  fail_on_error: false
  max_error_threshold: 10
  
logging:
  level: DEBUG
```

### Step 4: Secret Management Setup

#### 4.1 Store Secrets in Key Vault

```bash
# Oracle credentials (if using Oracle)
az keyvault secret set \
  --vault-name kv-data-platform-dev \
  --name oracle-username \
  --value "your-oracle-username"

az keyvault secret set \
  --vault-name kv-data-platform-dev \
  --name oracle-password \
  --value "your-oracle-password"

# Storage account key
STORAGE_KEY=$(az storage account keys list \
  --account-name stadatplatformdev \
  --query '[0].value' -o tsv)

az keyvault secret set \
  --vault-name kv-data-platform-dev \
  --name storage-account-key \
  --value "$STORAGE_KEY"
```

#### 4.2 Create Databricks Secret Scope

```bash
# Create secret scope backed by Key Vault
databricks secrets create-scope \
  --scope databricks-secrets-dev \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "/subscriptions/{subscription-id}/resourceGroups/rg-data-platform/providers/Microsoft.KeyVault/vaults/kv-data-platform-dev" \
  --dns-name "https://kv-data-platform-dev.vault.azure.net/"

# Verify secret scope creation
databricks secrets list-scopes

# List secrets in scope
databricks secrets list --scope databricks-secrets-dev
```

### Step 5: Infrastructure Provisioning

#### 5.1 Provision Unity Catalog Infrastructure

```bash
# Run infrastructure provisioning script
python scripts/provision_infrastructure.py \
  --environments dev \
  --project-code cddp \
  --log-level INFO

# Verify catalog creation
databricks unity-catalog catalogs list
```

Expected output:
```
Creating catalog: cddp-dev-bronze
Creating catalog: cddp-dev-silver  
Creating catalog: cddp-dev-gold
Successfully created all system tables
Infrastructure provisioning completed successfully
```

#### 5.2 Verify Infrastructure

```sql
-- In Databricks SQL or notebook, verify catalogs exist
SHOW CATALOGS;

-- Check schemas in bronze catalog
USE CATALOG `cddp-dev-bronze`;
SHOW SCHEMAS;

-- Verify system tables
DESCRIBE TABLE `cddp-dev-bronze`.`system`.`processed_files`;
```

### Step 6: Test Installation

#### 6.1 Validate Configuration

```bash
# Test configuration loading
python -c "
from src.core.config_manager import ConfigManager
config = ConfigManager()
print('Project Code:', config.project_code)
print('Environment:', config.environment)
print('Bronze Catalog:', config.get_catalog_name('bronze'))
"
```

#### 6.2 Test Secret Access

```bash
# Test secret retrieval (requires Databricks runtime)
python -c "
from pyspark.sql import SparkSession
from src.core.secret_manager import SecretManager

spark = SparkSession.builder.appName('test').getOrCreate()
secret_manager = SecretManager(spark)

try:
    # This will work in Databricks environment
    username = secret_manager.get_secret('databricks-secrets-dev', 'oracle-username')
    print('Secret retrieval successful')
except:
    print('Secret retrieval test - run in Databricks environment')
"
```

#### 6.3 Create Test Data Source

Create a simple test data source configuration:

```yaml
# config/sources/test_sources.yaml
test_excel_source:
  name: test_excel_source
  type: excel
  
  connection:
    storage_account: ${AZURE_STORAGE_ACCOUNT_DEV}
    container: raw-data
    path: test/sample_data.xlsx
    secret_scope: databricks-secrets-dev
    account_key: storage-account-key
    
  schema:
    columns:
      - name: id
        type: string
        nullable: false
      - name: name
        type: string
        nullable: false
      - name: value
        type: decimal(10,2)
        nullable: true
        
  target:
    catalog: ${project_code}-${environment}-bronze
    schema: test_data
    table: sample_data
```

#### 6.4 Upload Test Data

Create a test Excel file and upload to storage:

```bash
# Create test directory in storage
az storage blob directory create \
  --account-name stadatplatformdev \
  --container-name raw-data \
  --directory-path test

# Upload test file (create sample_data.xlsx first)
az storage blob upload \
  --account-name stadatplatformdev \
  --container-name raw-data \
  --name test/sample_data.xlsx \
  --file sample_data.xlsx
```

Sample Excel data:
| id | name | value |
|----|------|-------|
| 1 | Test Item 1 | 100.50 |
| 2 | Test Item 2 | 250.75 |

#### 6.5 Run Test Pipeline

```bash
# Validate test source configuration
python -m src.cli validate-source-config --source test_excel_source

# Run test ingestion
python -m src.cli run-bronze-ingestion --source test_excel_source

# Verify data in Databricks
# Run this in Databricks SQL or notebook:
# SELECT * FROM `cddp-dev-bronze`.`test_data`.`sample_data`;
```

## Post-Installation Setup

### 1. Set Up Databricks Workflows

```bash
# Deploy Databricks Asset Bundle
databricks bundle deploy -t dev

# Verify job creation
databricks jobs list
```

### 2. Configure Monitoring

```sql
-- Create monitoring views
CREATE OR REPLACE VIEW pipeline_health AS
SELECT 
    source_name,
    DATE(start_time) as date,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs
FROM `cddp-dev-bronze`.`system`.`pipeline_executions`
WHERE start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY source_name, DATE(start_time);
```

### 3. Set Up CI/CD (Optional)

Configure GitHub Actions for automated deployment:

```yaml
# .github/workflows/deploy.yml
name: Deploy Data Platform

on:
  push:
    branches: [main, develop]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          pip install databricks-cli
          pip install -r requirements-lock.txt
          
      - name: Deploy to Databricks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_DEV }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_DEV }}
        run: |
          databricks bundle deploy -t dev
```

## Troubleshooting

### Common Installation Issues

#### Issue: "Catalog not found" error

**Solution**:
```bash
# Re-run infrastructure provisioning
python scripts/provision_infrastructure.py --environments dev

# Check Unity Catalog permissions
databricks unity-catalog catalogs list
```

#### Issue: "Secret scope not found"

**Solution**:
```bash
# Verify secret scope exists
databricks secrets list-scopes

# Recreate if missing
databricks secrets create-scope --scope databricks-secrets-dev ...
```

#### Issue: "Storage access denied"

**Solution**:
```bash
# Check storage account key
az storage account keys list --account-name stadatplatformdev

# Update key in Key Vault
az keyvault secret set --vault-name kv-data-platform-dev --name storage-account-key --value "new-key"
```

#### Issue: "Permission denied" in Unity Catalog

**Solution**:
```sql
-- Grant necessary permissions (run as admin)
GRANT CREATE CATALOG ON METASTORE TO `your-service-principal`;
GRANT USE CATALOG ON CATALOG `cddp-dev-bronze` TO `data-engineers`;
```

### Verification Checklist

- [ ] Azure resources created successfully
- [ ] Framework installed and importable
- [ ] Environment variables configured
- [ ] Secrets stored in Key Vault
- [ ] Databricks secret scope created
- [ ] Unity Catalog infrastructure provisioned
- [ ] Test pipeline runs successfully
- [ ] Monitoring tables populated
- [ ] CI/CD pipeline configured (if applicable)

## Next Steps

After successful installation:

1. **Configure Data Sources**: Follow the [Source Configuration Guide](../configuration/source-configuration.md)
2. **Set Up Monitoring**: Follow the [Monitoring Guide](../monitoring/monitoring-guide.md)
3. **Deploy to Higher Environments**: Follow the [Environment Management Guide](../configuration/environment-management.md)

## Getting Help

If you encounter issues during installation:

- Check the [Troubleshooting Guide](../monitoring/troubleshooting.md)
- Review Databricks workspace logs
- Contact the Data Engineering team
- Create an issue in the repository