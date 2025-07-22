#!/usr/bin/env python3
"""Quick validation of existing tables."""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import base64

# Environment variables should be set externally
if not os.getenv('DATABRICKS_TOKEN'):
    raise ValueError("DATABRICKS_TOKEN environment variable not set")
if not os.getenv('DATABRICKS_HOST'):
    raise ValueError("DATABRICKS_HOST environment variable not set")
if not os.getenv('DATABRICKS_CLUSTER_ID'):
    raise ValueError("DATABRICKS_CLUSTER_ID environment variable not set")

w = WorkspaceClient()

print("🔍 Quick Table Validation")
print("=" * 60)

# Create validation notebook
validation_notebook = '''# Databricks notebook source
print("📊 QUICK TABLE VALIDATION")
print("=" * 50)

catalogs = ['cddp-dev-bronze', 'cddp-dev-silver', 'cddp-dev-gold']
total_tables = 0
total_records = 0

for catalog in catalogs:
    print(f"\\n{catalog.upper()}:")
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        for schema in schemas:
            schema_name = schema.namespace
            if schema_name not in ['information_schema']:
                try:
                    tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema_name}`").collect()
                    for table in tables:
                        table_name = table.tableName
                        count = spark.sql(f"SELECT COUNT(*) FROM `{catalog}`.`{schema_name}`.`{table_name}`").collect()[0][0]
                        print(f"  - {schema_name}.{table_name}: {count:,} records")
                        total_tables += 1
                        total_records += count
                except Exception as e:
                    print(f"  - Error reading {schema_name}: {str(e)}")
    except Exception as e:
        print(f"  Error accessing {catalog}: {str(e)}")

print(f"\\n📊 SUMMARY:")
print(f"   Total Tables: {total_tables}")
print(f"   Total Records: {total_records:,}")

# Check if new test files exist
print("\\n📁 Checking for Test Files:")
try:
    test_files = dbutils.fs.ls("abfss://raw-data@agentstge.dfs.core.windows.net/products/test/")
    print(f"   Found {len(test_files)} test files in products/test/")
except:
    print("   No test files found yet")

try:
    sales_test = dbutils.fs.ls("abfss://raw-data@agentstge.dfs.core.windows.net/sales/test/")
    print(f"   Found {len(sales_test)} test files in sales/test/")
except:
    print("   No sales test files found yet")

if total_tables > 0 and total_records > 0:
    print("\\n✅ TABLES EXIST - PIPELINE WORKING!")
else:
    print("\\n⚠️ NO TABLES FOUND YET")
'''

# Upload notebook
notebook_path = "/Users/balaji.krishnan@nanba.co.uk/quick_validate"
w.workspace.import_(
    path=notebook_path,
    content=base64.b64encode(validation_notebook.encode()).decode('utf-8'),
    format=workspace.ImportFormat.SOURCE,
    language=workspace.Language.PYTHON,
    overwrite=True
)

# Create and run job
job = w.jobs.create(
    name="Quick Validation",
    tasks=[
        jobs.Task(
            task_key="validate",
            notebook_task=jobs.NotebookTask(
                notebook_path=notebook_path
            ),
            existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    ]
)

run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Validation started: Run ID {run.run_id}")
print(f"📊 Monitor at: https://adb-2908121449961741.1.azuredatabricks.net/#job/{job.job_id}/run/{run.run_id}")
print("\n⏳ Validation will run in background - check Databricks UI for results")