# Databricks notebook source
# MAGIC %md
# MAGIC # Fix Package Installation Issue

# COMMAND ----------

import os
import sys

print("🔧 FIXING PACKAGE INSTALLATION")
print("=" * 50)

# COMMAND ----------

# First, let's check what's in the workspace
print("1. Checking workspace structure:")
try:
    # List the workspace directory
    files = dbutils.fs.ls("/Workspace/Users/balaji.krishnan@nanba.co.uk/")
    for f in files:
        print(f"  {f.name}")
except Exception as e:
    print(f"  Error: {str(e)}")

# COMMAND ----------

# Clone the repository to workspace if needed
print("\n2. Setting up repository in workspace:")
repo_path = "/Workspace/Users/balaji.krishnan@nanba.co.uk/common-data-platform"

# Check if repo exists
try:
    dbutils.fs.ls(repo_path)
    print("  ✅ Repository already exists")
except:
    print("  📥 Cloning repository...")
    # Clone from GitHub
    import subprocess
    
    # Set up git credentials
    subprocess.run(["git", "config", "--global", "user.email", "balaji.krishnan@nanba.co.uk"])
    subprocess.run(["git", "config", "--global", "user.name", "Balaji Krishnan"])
    
    # Clone the repo
    clone_result = subprocess.run(
        ["git", "clone", "https://github.com/balaji-krishnan-nanba/common-data-platform.git", repo_path],
        capture_output=True,
        text=True
    )
    
    if clone_result.returncode == 0:
        print("  ✅ Repository cloned successfully")
    else:
        print(f"  ❌ Clone failed: {clone_result.stderr}")

# COMMAND ----------

# Install the package properly
print("\n3. Installing package:")
%pip install -e /Workspace/Users/balaji.krishnan@nanba.co.uk/common-data-platform

# COMMAND ----------

# Restart Python to ensure clean import
dbutils.library.restartPython()

# COMMAND ----------

# Test imports
print("4. Testing imports:")
try:
    from common_data_platform.config import ConfigManager
    from common_data_platform.ingestion import FileIngestionOrchestrator
    from common_data_platform.transformation import Transformer
    print("  ✅ All imports successful!")
except Exception as e:
    print(f"  ❌ Import error: {str(e)}")
    
# COMMAND ----------

print("\n✅ Package setup complete!")
print("You can now run the pipeline notebook.")