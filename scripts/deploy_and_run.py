#!/usr/bin/env python3
"""Deploy and run the pipeline test notebook."""

import os
import base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace, jobs

# Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://adb-2908121449961741.1.azuredatabricks.net/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0721-134254-9s0ph6e7")

if not DATABRICKS_TOKEN:
    print("❌ Error: DATABRICKS_TOKEN environment variable not set")
    print("   Set it with: export DATABRICKS_TOKEN='your-token'")
    exit(1)

def deploy_and_run():
    """Deploy notebook and create a run link."""
    
    # Initialize workspace client
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    
    # Upload notebook
    notebook_path = "/Users/balaji.krishnan@nanba.co.uk/test_complete_pipeline"
    local_path = "notebooks/test_complete_pipeline.py"
    
    print(f"📤 Uploading notebook to {notebook_path}...")
    
    with open(local_path, 'rb') as f:
        content = f.read()
    
    w.workspace.import_(
        path=notebook_path,
        content=base64.b64encode(content).decode('utf-8'),
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        overwrite=True
    )
    
    print("✅ Notebook uploaded successfully!")
    
    # Also upload the e2e_pipeline_complete notebook
    notebook_path2 = "/Users/balaji.krishnan@nanba.co.uk/e2e_pipeline_complete"
    local_path2 = "notebooks/e2e_pipeline_complete.py"
    
    print(f"📤 Uploading notebook to {notebook_path2}...")
    
    with open(local_path2, 'rb') as f:
        content2 = f.read()
    
    w.workspace.import_(
        path=notebook_path2,
        content=base64.b64encode(content2).decode('utf-8'),
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        overwrite=True
    )
    
    print("✅ Both notebooks uploaded successfully!")
    
    print("\n🎯 Next Steps:")
    print("1. Open the Databricks workspace:")
    print(f"   {DATABRICKS_HOST}")
    print("\n2. Navigate to the notebooks:")
    print(f"   - {notebook_path}")
    print(f"   - {notebook_path2}")
    print("\n3. Attach to cluster and run:")
    print(f"   Cluster: {CLUSTER_ID}")
    print("\n4. The notebooks will:")
    print("   - Install the framework package")
    print("   - Process all CSV and Excel files")
    print("   - Create Bronze, Silver, and Gold tables")
    print("   - Test with new files")
    print("   - Validate the complete pipeline")

if __name__ == "__main__":
    deploy_and_run()