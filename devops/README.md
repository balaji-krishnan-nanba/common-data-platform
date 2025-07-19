# DevOps & CI/CD for Common Data Platform

This directory contains the complete DevOps infrastructure for the Common Data Platform, including Databricks Asset Bundle (DAB) configuration, GitHub Actions CI/CD pipeline, and deployment automation.

## 🏗️ Architecture Overview

The DevOps setup follows a **modular, industry-standard approach** with clear separation of concerns:

```
devops/
├── databricks.yml              # Main DAB entry point
├── environments/               # Environment-specific configurations
│   ├── dev.yml                # Development environment
│   ├── test.yml               # Test environment
│   └── prod.yml               # Production environment
├── resources/                  # Resource definitions
│   ├── jobs/                  # Job definitions
│   └── clusters/              # Cluster configurations
├── variables/                  # Variable definitions
│   ├── common.yml             # Common variables
│   ├── cicd.yml               # CI/CD specific variables
│   └── environments.yml       # Environment-specific variables
├── templates/                  # Reusable templates
├── scripts/                    # Automation scripts
│   ├── validate.sh            # Validation script
│   ├── test.sh                # Testing script
│   └── deploy.sh              # Deployment script
└── logs/                       # Deployment logs
```

## 🚀 Quick Start

### Local Development

1. **Setup Environment Variables**:
   ```bash
   export AZURE_TENANT_ID="your-tenant-id"
   export AZURE_STORAGE_ACCOUNT_DEV="yourstoragedev"
   export AZURE_KEY_VAULT_URL_DEV="https://yourvault-dev.vault.azure.net/"
   export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
   export DATABRICKS_TOKEN="your-token"
   ```

2. **Validate Configuration**:
   ```bash
   cd devops
   ./scripts/validate.sh dev
   ```

3. **Deploy to Development**:
   ```bash
   ./scripts/deploy.sh -t dev
   ```

### CI/CD Pipeline

The GitHub Actions workflow automatically:
- ✅ Validates all environment configurations
- 🧪 Runs simplified testing strategy (no connectivity/sample data tests)
- 🚀 Deploys to appropriate environments based on branch/trigger
- 📊 Provides deployment status and logs

## 📋 Prerequisites

### Required Tools
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) (v0.18+)
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) (optional)
- `jq` for JSON processing
- `python3` with `yaml` module

### Install Databricks CLI
```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

### Required Azure Resources
- Databricks workspace (dev/test/prod)
- Azure Storage Account (dev/test/prod)
- Azure Key Vault (dev/test/prod)
- Service Principal with appropriate permissions

## 🔧 Configuration Guide

### Environment Variables

#### GitHub Secrets (Required)
```bash
# Databricks Configuration
DATABRICKS_DEV_HOST      # https://your-dev-workspace.azuredatabricks.net
DATABRICKS_DEV_TOKEN     # Personal access token for dev workspace
DATABRICKS_TEST_HOST     # https://your-test-workspace.azuredatabricks.net
DATABRICKS_TEST_TOKEN    # Personal access token for test workspace
DATABRICKS_PROD_HOST     # https://your-prod-workspace.azuredatabricks.net
DATABRICKS_PROD_TOKEN    # Personal access token for prod workspace

# Azure Configuration
AZURE_TENANT_ID              # Azure AD tenant ID
AZURE_STORAGE_ACCOUNT_DEV    # Storage account name for dev
AZURE_STORAGE_ACCOUNT_TEST   # Storage account name for test
AZURE_STORAGE_ACCOUNT_PROD   # Storage account name for prod
AZURE_KEY_VAULT_URL_DEV      # Key vault URL for dev
AZURE_KEY_VAULT_URL_TEST     # Key vault URL for test
AZURE_KEY_VAULT_URL_PROD     # Key vault URL for prod
```

### Databricks Asset Bundle Structure

#### Main Configuration (`databricks.yml`)
The main DAB file is kept minimal and modular:
```yaml
bundle:
  name: common-data-platform

include:
  - variables/*.yml
  - resources/**/*.yml

targets:
  dev:
    mode: development
  test:
    mode: staging
  prod:
    mode: production
```

#### Variable Hierarchy
Variables are resolved in this order (last wins):
1. `variables/common.yml` - Shared across all environments
2. `variables/environments.yml` - Environment-specific defaults
3. `variables/cicd.yml` - CI/CD specific variables
4. `environments/{env}.yml` - Environment overrides
5. Command line `--var` flags - Runtime overrides

## 🔍 Validation & Testing

### Validation Script (`scripts/validate.sh`)
**Purpose**: Validates DAB configuration without requiring connectivity

**Features**:
- ✅ YAML syntax validation
- ✅ DAB configuration validation
- ✅ Required variables check
- ✅ Resource definitions check
- ✅ Environment configuration validation
- ❌ No connectivity tests (per user requirement)
- ❌ No sample data tests (per user requirement)

**Usage**:
```bash
# Validate development environment
./scripts/validate.sh dev

# Validate with verbose output
./scripts/validate.sh dev true
```

### Testing Script (`scripts/test.sh`)
**Purpose**: Runs configuration and basic health checks

**Test Suite**:
1. **Bundle Validation** - Ensures DAB config is valid
2. **Configuration Health** - Checks environment-specific settings
3. **Resource Definitions** - Verifies jobs, clusters, variables exist
4. **Deployment Readiness** - Confirms bundle can be deployed
5. **Environment Variables** - Validates required variables

**Usage**:
```bash
# Run all tests for development
./scripts/test.sh dev

# Run tests with verbose output
./scripts/test.sh dev true
```

## 🚀 Deployment

### Deployment Script (`scripts/deploy.sh`)
**Purpose**: Handles complete deployment lifecycle

**Features**:
- 🔍 Pre-deployment validation
- 🚀 Automated deployment with Git metadata
- ✅ Post-deployment verification
- 🔄 CI/CD integration
- 📊 Deployment logging

**Usage**:
```bash
# Deploy to development
./scripts/deploy.sh -t dev

# Validate only (no deployment)
./scripts/deploy.sh -v

# Force deploy to production
./scripts/deploy.sh -t prod -f

# CI mode (no interactive prompts)
./scripts/deploy.sh -t test --ci
```

## 🔄 CI/CD Pipeline Details

### Workflow Triggers
```yaml
# Pull Requests to main (deploys to dev)
on:
  pull_request:
    branches: [main]

# Push to main (deploys to test)
on:
  push:
    branches: [main]

# Manual workflow dispatch (deploys to specified env)
on:
  workflow_dispatch:
    inputs:
      environment:
        type: choice
        options: [dev, test, prod]
```

### Pipeline Stages

#### 1. Validation Stage
- Validates DAB configuration for all environments
- Checks YAML syntax and variable resolution
- No connectivity or sample data tests (per user requirement)

#### 2. Development Deployment
**Triggers**: PR branches (`feature/*`, `fix/*`, `chore/*`)
- Validates and deploys to development environment
- Runs development validation tests
- Non-blocking (warnings allowed)

#### 3. Test Deployment
**Triggers**: Push to `main` branch
- Validates and deploys to test environment
- Runs comprehensive test suite
- Must pass before production eligibility

#### 4. Production Deployment
**Triggers**: Manual workflow dispatch
- Requires test deployment success
- Extracts version information from Git tags
- Runs production smoke tests
- Provides deployment success notifications

### Environment Promotion Strategy
```
feature/fix branches → Dev Environment (PR)
           ↓
    main branch → Test Environment (auto)
           ↓
   manual trigger → Production Environment (manual)
```

## 📊 Monitoring & Logging

### Deployment Logs
All deployments generate detailed logs in `devops/logs/`:
- `validation.log` - Validation results
- `test.log` - Test execution results  
- `deploy.log` - Deployment progress and results

### CI/CD Variables Tracking
The pipeline automatically tracks:
- Git branch and commit SHA
- Repository URL and run ID
- Deployment timestamp and version
- Environment and deployment source

## 🔐 Security Best Practices

### Secrets Management
- ✅ All credentials stored in GitHub Secrets
- ✅ Azure Key Vault integration for runtime secrets
- ✅ Service Principal authentication (no storage keys)
- ✅ Minimal required permissions

### Access Control
- Environment-specific GitHub environment protection
- Databricks workspace isolation
- Azure RBAC for resource access
- Review requirements for production deployments

## 🛠️ Troubleshooting

### Common Issues

#### 1. Validation Failures
```bash
# Check YAML syntax
python3 -c "import yaml; yaml.safe_load(open('databricks.yml'))"

# Validate specific environment
databricks bundle validate --target dev

# Check environment variables
env | grep -E "(AZURE|DATABRICKS)"
```

#### 2. Deployment Failures
```bash
# Check Databricks connectivity
databricks workspace current

# Verify authentication
databricks jobs list --output table

# Check bundle status
databricks bundle summary --target dev
```

#### 3. CI/CD Pipeline Issues
```bash
# Check GitHub secrets are set
# Verify Databricks CLI version
databricks --version

# Check environment variables in Actions
echo $AZURE_TENANT_ID
```

## 📚 Additional Resources

### Documentation
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [GitHub Actions for Databricks](https://github.com/databricks/setup-cli)
- [Azure Service Principal Setup](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal)

### Example Commands
```bash
# Full deployment cycle
./scripts/validate.sh dev
./scripts/deploy.sh -t dev
./scripts/test.sh dev

# CI/CD simulation
GITHUB_ACTIONS=true ./scripts/deploy.sh -t test --ci

# Production deployment
./scripts/deploy.sh -t prod -f
```

---

**Note**: This DevOps setup implements the user's requirements for:
- ✅ Modular DAB structure (industry standard)
- ✅ Simplified testing (no connectivity/sample data tests)
- ✅ Service Principal authentication
- ✅ Delta table logging (no Log Analytics)
- ✅ GitHub Actions CI/CD integration
- ✅ Environment variable management in Databricks