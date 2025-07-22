#!/usr/bin/env python3
"""Check cluster status and configuration."""

import os
from databricks.sdk import WorkspaceClient

# Environment variables should be set before running:
# export DATABRICKS_TOKEN='your_token'
# export DATABRICKS_HOST='https://your-workspace.azuredatabricks.net/'

w = WorkspaceClient()

print("🔍 CHECKING CLUSTER STATUS")
print("=" * 60)

# Check cluster
cluster_id = os.getenv('DATABRICKS_CLUSTER_ID', '0721-134254-9s0ph6e7')

try:
    cluster = w.clusters.get(cluster_id=cluster_id)
    print(f"\n📊 Cluster Details:")
    print(f"   ID: {cluster.cluster_id}")
    print(f"   Name: {cluster.cluster_name}")
    print(f"   State: {cluster.state}")
    print(f"   Spark Version: {cluster.spark_version}")
    print(f"   Node Type: {cluster.node_type_id}")
    
    if cluster.spark_env_vars:
        print(f"\n📋 Environment Variables:")
        for key, value in cluster.spark_env_vars.items():
            print(f"   {key}: {value[:50]}...")
            
    print(f"\n✅ Cluster is {cluster.state}")
    
except Exception as e:
    print(f"❌ Error getting cluster: {str(e)}")
    
# List recent runs
print("\n📊 Recent Job Runs:")
runs = w.jobs.list_runs(limit=10)
for i, run in enumerate(runs):
    if i >= 5:
        break
    print(f"   {run.run_id}: {run.run_name or 'Unnamed'} - {run.state.life_cycle_state} ({run.state.result_state})")