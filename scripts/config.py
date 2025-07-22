#!/usr/bin/env python3
"""Configuration for pipeline scripts."""

import os

# Databricks Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://adb-2908121449961741.1.azuredatabricks.net/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0721-134254-9s0ph6e7")

# Azure Configuration
AZURE_SOURCE_STORAGE = os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT", "agentstge")
AZURE_SP_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_SP_SECRET = os.getenv("AZURE_CLIENT_SECRET")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")

def validate_config():
    """Validate required configuration."""
    if not DATABRICKS_TOKEN:
        raise ValueError("DATABRICKS_TOKEN environment variable is required")
    
    print(f"✅ Configuration validated")
    print(f"   Databricks Host: {DATABRICKS_HOST}")
    print(f"   Cluster ID: {CLUSTER_ID}")
    print(f"   Source Storage: {AZURE_SOURCE_STORAGE}")
    
    return True