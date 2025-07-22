#!/bin/bash
# Template for setting up Common Data Platform environment variables
# Copy this file to setup_environment.sh and fill in your actual values

echo "🔧 Setting up Common Data Platform environment variables..."

# Databricks Configuration
export DATABRICKS_HOST="https://your-databricks-workspace.azuredatabricks.net/"
export DATABRICKS_TOKEN="your-databricks-token-here"

# Azure Service Principal Configuration
export AZURE_CLIENT_ID="your-azure-client-id"
export AZURE_CLIENT_SECRET="your-azure-client-secret"
export AZURE_TENANT_ID="your-azure-tenant-id"
export AZURE_SUBSCRIPTION_ID="your-azure-subscription-id"

# Azure Storage Configuration
export AZURE_SOURCE_STORAGE_ACCOUNT="your-source-storage-account"
export AZURE_DATALAKE_STORAGE_ACCOUNT="your-datalake-storage-account"

# Framework Configuration
export PROJECT_CODE="cddp"
export ENVIRONMENT="dev"

echo "✅ Environment variables set for Common Data Platform"
echo "🚀 Ready to run deployment scripts"

# Usage examples:
echo ""
echo "📋 Usage examples:"
echo "   cp scripts/setup_environment.template.sh scripts/setup_environment.sh"
echo "   # Edit scripts/setup_environment.sh with your actual values"
echo "   source scripts/setup_environment.sh"
echo "   python scripts/run_framework_e2e.py"
