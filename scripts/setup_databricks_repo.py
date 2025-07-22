#!/usr/bin/env python3
"""Properly set up Databricks Repo and fix framework issues."""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import time

# Check required environment variables
required_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST']
for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} environment variable not set")

w = WorkspaceClient()

print("🔧 PROPER DATABRICKS REPO SETUP")
print("=" * 60)

# Step 1: Set up Databricks Repo
print("\n1. Setting up Databricks Repo...")

repo_path = "/Repos/balaji.krishnan@nanba.co.uk/common-data-platform"
git_url = "https://github.com/balaji-krishnan-nanba/common-data-platform.git"

try:
    # Check if repo exists
    existing_repos = w.repos.list(path_prefix="/Repos/balaji.krishnan@nanba.co.uk/")
    repo_exists = False
    repo_id = None
    
    for repo in existing_repos:
        if repo.path == repo_path:
            repo_exists = True
            repo_id = repo.id
            print(f"  ✅ Repo already exists at {repo_path}")
            break
    
    if not repo_exists:
        # Create new repo
        print("  📥 Creating new repo...")
        new_repo = w.repos.create(
            url=git_url,
            path=repo_path,
            provider="github"
        )
        repo_id = new_repo.id
        print(f"  ✅ Repo created: {new_repo.path}")
    
    # Update to latest
    print("  🔄 Updating to latest commit...")
    w.repos.update(
        repo_id=repo_id,
        branch="main"
    )
    print("  ✅ Repo updated to latest")
    
except Exception as e:
    print(f"  ❌ Repo setup error: {str(e)}")
    # Try to get repo info
    print("\n  Attempting to get repo details...")
    try:
        repo_info = w.repos.get(repo_id=repo_id)
        print(f"  Repo URL: {repo_info.url}")
        print(f"  Branch: {repo_info.branch}")
        print(f"  Head commit: {repo_info.head_commit_id}")
    except:
        pass

# Step 2: Check repo contents
print("\n2. Checking repo contents...")
try:
    # List repo contents
    contents = w.workspace.list(repo_path)
    print("  Repository structure:")
    for item in contents:
        print(f"    - {item.path.split('/')[-1]}")
        if item.path.endswith('src'):
            # Check src contents
            src_contents = w.workspace.list(item.path)
            for src_item in src_contents:
                print(f"      - {src_item.path.split('/')[-1]}")
except Exception as e:
    print(f"  ❌ Error listing contents: {str(e)}")

print("\n✅ Repo setup complete")
print(f"📁 Repository location: {repo_path}")
print("\nNext steps:")
print("1. Notebooks should use %pip install -e /Repos/balaji.krishnan@nanba.co.uk/common-data-platform")
print("2. Python files can import directly: from common_data_platform.config import ConfigManager")