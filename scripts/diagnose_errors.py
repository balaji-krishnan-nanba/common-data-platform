#!/usr/bin/env python3
"""Diagnose pipeline errors."""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Check required environment variables
required_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID']
for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} environment variable not set")

w = WorkspaceClient()

print("🔍 DIAGNOSING PIPELINE ERRORS")
print("=" * 60)

# Create diagnostic notebook
diagnostic_notebook = '''# Databricks notebook source
print("🔍 PIPELINE ERROR DIAGNOSIS")
print("=" * 50)

# Check if cluster has required environment variables
print("\\n1. Environment Variables:")
env_vars = ['AZURE_SOURCE_STORAGE_ACCOUNT', 'AZURE_DATALAKE_STORAGE_ACCOUNT', 
            'AZURE_KEY_VAULT_URL', 'PROJECT_CODE', 'ENVIRONMENT']

for var in env_vars:
    value = spark.conf.get(f"spark.env.{var}", "NOT SET")
    print(f"   {var}: {value}")

# Check if package can be imported
print("\\n2. Package Import Test:")
try:
    import sys
    sys.path.append('/databricks/driver')
    
    # Try installing the package
    %pip install -e /Workspace/Users/balaji.krishnan@nanba.co.uk/common-data-platform
    
    from common_data_platform.config import ConfigManager
    from common_data_platform.ingestion import FileIngestionOrchestrator
    print("   ✅ Package imports successful")
except Exception as e:
    print(f"   ❌ Import error: {str(e)}")

# Check Unity Catalog access
print("\\n3. Unity Catalog Access:")
try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    print(f"   ✅ Can access {len(catalogs)} catalogs")
    for cat in catalogs:
        if 'cddp' in cat.catalog:
            print(f"      - {cat.catalog}")
except Exception as e:
    print(f"   ❌ Catalog access error: {str(e)}")

# Check storage access
print("\\n4. Storage Access Test:")
try:
    files = dbutils.fs.ls("abfss://raw-data@agentstge.dfs.core.windows.net/")
    print(f"   ✅ Can access storage: {len(files)} items found")
    for f in files[:5]:
        print(f"      - {f.name}")
except Exception as e:
    print(f"   ❌ Storage access error: {str(e)}")

# Check if we can read a CSV file directly
print("\\n5. Direct File Read Test:")
try:
    test_df = spark.read.option("header", "true").csv("abfss://raw-data@agentstge.dfs.core.windows.net/products/catalog/product_catalog.csv")
    count = test_df.count()
    print(f"   ✅ Can read CSV directly: {count} records")
except Exception as e:
    print(f"   ❌ File read error: {str(e)}")

# Check existing tables
print("\\n6. Existing Tables Check:")
for catalog in ['cddp-dev-bronze', 'cddp-dev-silver', 'cddp-dev-gold']:
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        table_count = 0
        for schema in schemas:
            if schema.namespace not in ['information_schema']:
                tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema.namespace}`").collect()
                table_count += len(tables)
        print(f"   {catalog}: {table_count} tables")
    except Exception as e:
        print(f"   {catalog}: Error - {str(e)}")

print("\\n✅ Diagnosis complete")
'''

# Upload notebook
notebook_path = "/Users/balaji.krishnan@nanba.co.uk/diagnose_errors"
w.workspace.import_(
    path=notebook_path,
    content=base64.b64encode(diagnostic_notebook.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Run diagnostic
job = w.jobs.create(
    name="Diagnose Errors",
    tasks=[
        jobs.Task(
            task_key="diagnose",
            notebook_task=jobs.NotebookTask(
                notebook_path=notebook_path
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Diagnostic started: Run ID {run.run_id}")
print(f"📊 Monitor at: https://adb-2908121449961741.1.azuredatabricks.net/#job/{job.job_id}/run/{run.run_id}")

# Wait for completion
import time
print("\n⏳ Waiting for diagnostic to complete...")
for i in range(60):  # 5 minutes max
    run_info = w.jobs.get_run(run_id=run.run_id)
    if run_info.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        print(f"\nDiagnostic completed with status: {run_info.state.result_state}")
        if run_info.state.state_message:
            print(f"Message: {run_info.state.state_message}")
        break
    time.sleep(5)

# Clean up
try:
    w.jobs.delete(job_id=job.job_id)
except:
    pass