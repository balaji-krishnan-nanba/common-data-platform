# Prerequisites Guide

This guide provides a comprehensive checklist of all prerequisites needed to successfully install and operate the Common Data Platform framework.

## Azure Prerequisites

### 1. Azure Subscription Requirements

| Requirement | Details | Verification |
|-------------|---------|-------------|
| **Azure Subscription** | Active Azure subscription with sufficient credits | `az account show` |
| **Subscription Role** | Contributor or Owner role | `az role assignment list --assignee $(az account show --query user.name -o tsv)` |
| **Resource Quotas** | Sufficient compute and storage quotas | Check Azure portal quotas |
| **Region Availability** | Unity Catalog supported regions | [Unity Catalog regions](https://docs.microsoft.com/en-us/azure/databricks/administration-guide/account-settings/supported-regions) |

### 2. Azure Databricks Requirements

#### Workspace Setup
- [ ] **Azure Databricks Workspace** - Premium tier required for Unity Catalog
- [ ] **Unity Catalog Enabled** - Must be enabled at workspace level
- [ ] **Admin Access** - Workspace admin privileges required
- [ ] **Compute Access** - Ability to create and manage clusters

#### Verification Commands
```bash
# Check Databricks workspace access
databricks workspace list

# Verify Unity Catalog is enabled
databricks unity-catalog metastores list

# Check cluster creation permissions
databricks clusters list
```

### 3. Azure Key Vault Requirements

#### Setup Requirements
- [ ] **Key Vault Instance** - With RBAC authorization enabled
- [ ] **Key Vault Administrator Role** - To manage secrets
- [ ] **Network Access** - Databricks can access Key Vault
- [ ] **Secret Management** - Ability to create and read secrets

#### Verification Commands
```bash
# Check Key Vault access
az keyvault list

# Test secret operations
az keyvault secret set --vault-name your-keyvault --name test-secret --value "test"
az keyvault secret show --vault-name your-keyvault --name test-secret
az keyvault secret delete --vault-name your-keyvault --name test-secret
```

### 4. Azure Storage Requirements

#### Storage Account Setup
- [ ] **Data Lake Storage Gen2** - Hierarchical namespace enabled
- [ ] **Storage Account Access** - Contributor role or storage-specific roles
- [ ] **Container Structure** - Organized container layout
- [ ] **Network Access** - Databricks can access storage

#### Required Storage Containers
```bash
# Create required containers
az storage container create --name raw-data --account-name yourstorageaccount
az storage container create --name processed-data --account-name yourstorageaccount
az storage container create --name unity-catalog --account-name yourstorageaccount
az storage container create --name checkpoints --account-name yourstorageaccount
```

## Development Environment Prerequisites

### 1. Local Development Tools

| Tool | Version | Purpose | Installation |
|------|---------|---------|-------------|
| **Python** | 3.8+ | Framework runtime | [Python.org](https://python.org) |
| **Git** | Latest | Version control | [Git-scm.com](https://git-scm.com) |
| **Azure CLI** | Latest | Azure management | [Azure CLI docs](https://docs.microsoft.com/cli/azure/) |
| **Databricks CLI** | Latest | Databricks operations | `pip install databricks-cli` |

#### Verification Commands
```bash
# Check Python version
python --version  # Should be 3.8+

# Check Git installation
git --version

# Check Azure CLI
az version

# Check Databricks CLI
databricks --version
```

### 2. Python Environment Setup

#### Virtual Environment
```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Verify activation
which python  # Should point to .venv/bin/python
```

#### Required Python Packages
```bash
# Install framework dependencies
pip install -r requirements.txt

# Verify key packages
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
python -c "import yaml; print('PyYAML installed successfully')"
python -c "import azure.storage.blob; print('Azure Storage SDK installed')"
```

### 3. IDE and Development Tools (Optional)

#### Recommended IDEs
- [ ] **Visual Studio Code** with Python extension
- [ ] **PyCharm** (Community or Professional)
- [ ] **Jupyter Notebook** for interactive development

#### Useful Extensions (VS Code)
- [ ] Python extension
- [ ] YAML extension
- [ ] Azure Account extension
- [ ] GitLens extension

## Source System Prerequisites

### 1. Oracle Database (If Using)

#### Connection Requirements
- [ ] **Network Connectivity** - Databricks can reach Oracle server
- [ ] **Database Credentials** - Username/password with read access
- [ ] **JDBC Driver** - Oracle JDBC driver available in Databricks
- [ ] **Table Permissions** - SELECT permissions on source tables

#### Testing Oracle Connectivity
```bash
# Test Oracle connection (run in Databricks)
python -c "
import jaydebeapi
conn = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:@//host:port/service',
    ['username', 'password'],
    '/databricks/jars/ojdbc8.jar'
)
print('Oracle connection successful')
conn.close()
"
```

### 2. File Sources (Excel/CSV)

#### File Access Requirements
- [ ] **File Upload Access** - Can upload files to Azure Storage
- [ ] **File Format Standards** - Consistent file formats and structures
- [ ] **Naming Conventions** - Predictable file naming patterns
- [ ] **Data Quality** - Source files have consistent quality

#### File Structure Example
```
raw-data/
├── sales/
│   ├── daily/
│   │   ├── 2024/01/15/sales_20240115.xlsx
│   │   └── 2024/01/16/sales_20240116.xlsx
│   └── monthly/
│       └── 2024/01/sales_monthly_202401.xlsx
├── customers/
│   └── master/
│       └── customer_master.xlsx
└── products/
    └── catalog/
        └── product_catalog.csv
```

## Security Prerequisites

### 1. Authentication and Authorization

#### Azure AD Setup
- [ ] **Service Principal** - For automated operations
- [ ] **User Accounts** - For interactive development
- [ ] **Role Assignments** - Appropriate roles assigned
- [ ] **Multi-Factor Authentication** - MFA enabled where required

#### Creating Service Principal
```bash
# Create service principal for automation
az ad sp create-for-rbac --name "data-platform-sp" --role contributor

# Note the output for configuration:
# - appId (client ID)
# - password (client secret)
# - tenant
```

### 2. Network Security

#### Network Requirements
- [ ] **Virtual Network** - Databricks workspace in VNet (optional but recommended)
- [ ] **Firewall Rules** - Allow Databricks to access required services
- [ ] **Private Endpoints** - For enhanced security (optional)
- [ ] **Network Security Groups** - Proper NSG rules configured

#### Firewall Configuration
```bash
# Key ports and services that need to be accessible:
# - Azure Key Vault: 443/HTTPS
# - Azure Storage: 443/HTTPS
# - Oracle Database: 1521/TCP (or custom port)
# - Databricks Control Plane: 443/HTTPS
```

### 3. Secrets Management

#### Secret Categories
- [ ] **Database Credentials** - Oracle username/password
- [ ] **Storage Keys** - Azure Storage account keys or SAS tokens
- [ ] **Service Principal Credentials** - For automated authentication
- [ ] **API Keys** - For external service integrations

#### Secret Naming Convention
```bash
# Recommended secret naming pattern:
{environment}-{service}-{credential-type}

# Examples:
dev-oracle-username
dev-oracle-password
prod-storage-account-key
test-service-principal-secret
```

## Performance Prerequisites

### 1. Databricks Cluster Configuration

#### Minimum Cluster Requirements
| Environment | Driver Node | Worker Nodes | Worker Count |
|-------------|-------------|--------------|--------------|
| **Development** | Standard_DS3_v2 | Standard_DS3_v2 | 2-4 |
| **Test** | Standard_DS3_v2 | Standard_DS3_v2 | 4-8 |
| **Production** | Standard_DS4_v2 | Standard_DS4_v2 | 8-16 |

#### Recommended Spark Configuration
```python
# Key Spark configurations for optimal performance
spark_config = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true", 
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true"
}
```

### 2. Storage Performance

#### Storage Configuration
- [ ] **Performance Tier** - Premium or Standard_LRS for better performance
- [ ] **Hierarchical Namespace** - Required for Data Lake Gen2
- [ ] **Blob Access Tier** - Hot tier for frequently accessed data
- [ ] **Replication** - LRS minimum, consider GRS for production

## Data Prerequisites

### 1. Data Volume Estimates

#### Sizing Guidelines
| Data Volume | Recommended Cluster | Estimated Processing Time |
|-------------|-------------------|--------------------------|
| < 1 GB | 2 workers | < 5 minutes |
| 1-10 GB | 4 workers | 5-15 minutes |
| 10-100 GB | 8 workers | 15-60 minutes |
| > 100 GB | 16+ workers | 1+ hours |

### 2. Data Quality Standards

#### Minimum Data Quality Requirements
- [ ] **Schema Consistency** - Consistent column names and data types
- [ ] **Primary Keys** - Identifiable unique keys for each record
- [ ] **Data Completeness** - Critical fields populated
- [ ] **Data Validity** - Values within expected ranges
- [ ] **Referential Integrity** - Valid relationships between tables

## Monitoring Prerequisites

### 1. Observability Setup

#### Monitoring Components
- [ ] **Log Analytics Workspace** - For centralized logging
- [ ] **Application Insights** - For application monitoring
- [ ] **Azure Monitor** - For infrastructure monitoring
- [ ] **Databricks Monitoring** - Built-in Databricks metrics

### 2. Alerting Configuration

#### Alert Channels
- [ ] **Email Notifications** - For critical alerts
- [ ] **Slack/Teams Integration** - For team notifications
- [ ] **SMS Alerts** - For high-priority issues (optional)
- [ ] **PagerDuty/OnCall** - For production incidents (optional)

## Pre-Installation Checklist

### Azure Environment Checklist
- [ ] Azure subscription with appropriate permissions
- [ ] Databricks workspace with Unity Catalog enabled
- [ ] Key Vault with RBAC authorization
- [ ] Storage account with Data Lake Gen2
- [ ] Network connectivity configured
- [ ] Service principal created and configured

### Development Environment Checklist
- [ ] Python 3.8+ installed
- [ ] Git installed and configured
- [ ] Azure CLI installed and authenticated
- [ ] Databricks CLI installed and configured
- [ ] Virtual environment created and activated
- [ ] Required Python packages installed

### Source Systems Checklist
- [ ] Oracle database accessible (if applicable)
- [ ] Sample data files prepared and uploaded
- [ ] Database credentials available
- [ ] File access patterns documented
- [ ] Data quality assessment completed

### Security Checklist
- [ ] Authentication methods configured
- [ ] Secrets stored in Key Vault
- [ ] Network security rules configured
- [ ] Role assignments completed
- [ ] Service principal permissions verified

### Performance Checklist
- [ ] Cluster sizing planned based on data volume
- [ ] Storage performance tier selected
- [ ] Spark configurations optimized
- [ ] Monitoring and alerting configured

## Validation Scripts

### Complete Environment Validation

```bash
#!/bin/bash
# Complete environment validation script

echo "=== Environment Validation ==="

# Check Python version
python_version=$(python --version 2>&1)
echo "Python: $python_version"

# Check Azure CLI
az_version=$(az version --query '"azure-cli"' -o tsv 2>/dev/null)
echo "Azure CLI: $az_version"

# Check Databricks CLI
databricks_version=$(databricks --version 2>&1)
echo "Databricks CLI: $databricks_version"

# Check Azure authentication
azure_account=$(az account show --query name -o tsv 2>/dev/null)
echo "Azure Account: $azure_account"

# Check Databricks authentication
databricks_workspace=$(databricks workspace list 2>/dev/null | head -1)
echo "Databricks Access: $([ $? -eq 0 ] && echo 'Success' || echo 'Failed')"

echo "=== Validation Complete ==="
```

Run this script to verify your environment is ready for framework installation.

## Getting Help

If you encounter issues with prerequisites:

1. **Review Error Messages** - Often contain specific guidance
2. **Check Azure Portal** - Verify resource configurations
3. **Consult Documentation** - Azure and Databricks official docs
4. **Contact Support** - Azure support or Data Engineering team
5. **Community Resources** - Stack Overflow, Azure forums

## Next Steps

Once all prerequisites are met:
1. Proceed to the [Installation Guide](installation.md)
2. Follow the [Quick Start Guide](quick-start.md)
3. Create your [First Pipeline](first-pipeline.md)