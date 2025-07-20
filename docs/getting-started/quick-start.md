# Quick Start Guide

Get your Common Data Platform up and running in 15 minutes with this step-by-step guide.

## Prerequisites Checklist

Before starting, ensure you have:

- [ ] Azure Databricks workspace with Unity Catalog enabled
- [ ] Azure Key Vault instance
- [ ] Azure Data Lake Storage Gen2 account
- [ ] Python 3.8+ installed locally
- [ ] Git repository access

## Step 1: Environment Setup (3 minutes)

### Clone and Install

```bash
# Clone the repository
git clone <your-repo-url>
cd common-data-platform

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e .
```

### Set Environment Variables

```bash
# Required environment variables
export PROJECT_CODE=cddp
export ENVIRONMENT=dev
export AZURE_STORAGE_ACCOUNT_DEV=yourstorageaccount
export AZURE_KEY_VAULT_URL_DEV=https://yourkeyvault.vault.azure.net/
export AZURE_TENANT_ID=your-azure-tenant-id
export DATABRICKS_HOST_DEV=https://adb-xxxxx.azuredatabricks.net
export DATABRICKS_TOKEN=your-databricks-token
```

### **Quick Setup for Different Scenarios**

#### **Local Development (Manual Setup)**
```bash
# Option 1: Export variables directly
export AZURE_TENANT_ID=$(az account show --query tenantId -o tsv)
export AZURE_STORAGE_ACCOUNT_DEV=yourstorageaccount

# Option 2: Use .env file (recommended)
cat > .env << EOF
export AZURE_TENANT_ID=$(az account show --query tenantId -o tsv)
export AZURE_STORAGE_ACCOUNT_DEV=yourstorageaccount
export AZURE_KEY_VAULT_URL_DEV=https://yourkeyvault.vault.azure.net/
export DATABRICKS_HOST_DEV=https://adb-xxxxx.azuredatabricks.net
EOF
source .env
```

#### **Databricks Jobs/Workflows (Production)**
Variables are automatically available through the bundle configuration in `databricks.yml`. Just deploy:
```bash
# Deploy with environment variables from bundle
databricks bundle deploy -t dev
```

#### **CI/CD Pipeline Setup**
```bash
# Set as repository secrets/variables in GitHub:
# Secrets: AZURE_TENANT_ID, DATABRICKS_TOKEN
# Variables: AZURE_STORAGE_ACCOUNT_DEV, DATABRICKS_HOST_DEV
```

## Step 2: Infrastructure Provisioning (5 minutes)

### Create Unity Catalog Infrastructure

```bash
# Upload and run the Unity Catalog setup notebook in Databricks
# Navigate to: /Workspace/Shared/common-data-platform/notebooks/setup/unity_catalog_setup.py
# Set parameters: project_code=cddp, environment=dev, storage_account=<your-storage-account>
# Or run via Databricks CLI:
databricks notebooks run /Workspace/Shared/common-data-platform/notebooks/setup/unity_catalog_setup.py \
  --notebook-params '{"project_code": "cddp", "environment": "dev", "storage_account": "your-storage-account"}'

# Verify catalog creation
databricks unity-catalog catalogs list
```

This creates:
- `cddp-dev-bronze` catalog with schemas: `excel_data`, `csv_data`, `oracle_data`, `system`
- `cddp-dev-silver` catalog with schemas: `excel_data`, `csv_data`, `oracle_data`
- `cddp-dev-gold` catalog with schemas: `analytics`, `reporting`, `customer_360`, `sales_analytics`
- Complete system tables: `processed_files`, `pipeline_executions`, `watermarks`, `data_quality_results`, `stage_executions`

### Setup Secrets in Azure Key Vault

Add these secrets to your Azure Key Vault:

```bash
# Oracle credentials (if using Oracle source)
az keyvault secret set --vault-name yourkeyvault --name oracle-username --value "your-username"
az keyvault secret set --vault-name yourkeyvault --name oracle-password --value "your-password"

# Storage account key
az keyvault secret set --vault-name yourkeyvault --name storage-account-key --value "your-storage-key"
```

### Create Databricks Secret Scope

```bash
# Create secret scope backed by Key Vault
databricks secrets create-scope --scope databricks-secrets-dev \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/{subscription-id}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{vault-name} \
  --dns-name https://{vault-name}.vault.azure.net/
```

## Step 3: Configure Your First Data Source (3 minutes)

### Option A: Excel File Source

Edit `config/sources/excel_sources.yaml`:

```yaml
my_excel_source:
  name: my_excel_source
  type: excel
  connection:
    storage_account: yourstorageaccount
    container: raw-data
    path: sales/sample_data.xlsx
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
      - name: amount
        type: decimal(10,2)
        nullable: true
  target:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: my_sample_data
```

### Option B: Oracle Database Source

Edit `config/sources/oracle_sources.yaml`:

```yaml
my_oracle_source:
  name: my_oracle_source
  type: oracle
  connection:
    host: your-oracle-server.com
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
```

## Step 4: Run Your First Pipeline (4 minutes)

### Validate Configuration

```bash
# Check if your source configuration is valid
python -m src.cli validate-source-config --source my_excel_source
```

### Run Bronze Ingestion

```bash
# Ingest data to bronze layer
python -m src.cli run-bronze-ingestion --source my_excel_source
```

Expected output:
```
2024-01-15 10:30:00 | INFO | Starting bronze ingestion for source: my_excel_source
2024-01-15 10:30:05 | INFO | Successfully wrote 1000 records to cddp-dev-bronze.excel_data.my_sample_data
2024-01-15 10:30:05 | INFO | Bronze ingestion completed successfully
```

### Verify Data in Databricks

```sql
-- Check your data in Databricks SQL or notebook
SELECT COUNT(*) FROM `cddp-dev-bronze`.`excel_data`.`my_sample_data`;

-- View sample records
SELECT * FROM `cddp-dev-bronze`.`excel_data`.`my_sample_data` LIMIT 10;
```

## Step 5: Configure and Run Silver Transformation (Optional)

### Configure Transformation

Edit `config/transformations/bronze_to_silver/excel_transformations.yaml`:

```yaml
my_data_cleansing:
  source:
    catalog: ${project_code}-${environment}-bronze
    schema: excel_data
    table: my_sample_data
  target:
    catalog: ${project_code}-${environment}-silver
    schema: excel_data
    table: my_sample_data_clean
  engine: sparksql
  transformations:
    - type: data_quality_checks
      checks:
        - type: null_check
          columns: [id, name]
          threshold: 0
```

### Run Silver Transformation

```bash
python -m src.cli run-silver-transformation --source my_excel_source
```

## 🎉 Success! You've completed the quick start.

You now have:
- ✅ Framework installed and configured
- ✅ Unity Catalog infrastructure provisioned
- ✅ First data source configured
- ✅ Data ingested to bronze layer
- ✅ Optional silver transformation completed

## Next Steps

### Immediate Next Steps
1. **Add more data sources** - Follow the [Source Configuration Guide](../configuration/source-configuration.md)
2. **Set up monitoring** - Follow the [Monitoring Guide](../monitoring/monitoring-guide.md)
3. **Configure CI/CD** - Follow the [Environment Management Guide](../configuration/environment-management.md)

### Advanced Features
1. **Implement SCD Type 2** - For slowly changing dimensions
2. **Add custom transformations** - With PySpark or SparkSQL
3. **Set up Databricks Workflows** - For scheduling and orchestration

## Troubleshooting

### Common Issues

**Error: "Catalog not found"**
```bash
# Re-run Unity Catalog setup notebook in Databricks
# Navigate to /Workspace/Shared/common-data-platform/notebooks/setup/unity_catalog_setup.py
```

**Error: "Secret not found"**
```bash
# Verify secret scope exists
databricks secrets list-scopes

# Check secrets in scope
databricks secrets list --scope databricks-secrets-dev
```

**Error: "Storage account access denied"**
```bash
# Verify storage account key in Key Vault
az keyvault secret show --vault-name yourkeyvault --name storage-account-key
```

### Getting Help

- Check the [Troubleshooting Guide](../monitoring/troubleshooting.md)
- Review logs in Databricks workspace
- Contact the Data Engineering team

## Sample Data

For testing, you can use this sample Excel data structure:

| id | name | amount | date |
|----|------|--------|------|
| 1 | John Doe | 100.50 | 2024-01-15 |
| 2 | Jane Smith | 250.75 | 2024-01-15 |
| 3 | Bob Johnson | 75.25 | 2024-01-15 |

Save this as `sample_data.xlsx` in your Azure Storage container at `raw-data/sales/sample_data.xlsx`.