# Databricks Bundle Artifact Deployment

This document explains how the Common Data Platform framework is deployed using Databricks Asset Bundles (DAB) without relying on DBFS.

## Overview

The framework uses Databricks Asset Bundles to automatically build and deploy the Python wheel package to the workspace. This approach eliminates the need for manual DBFS uploads and ensures consistent deployments across environments.

## How It Works

### 1. Artifact Configuration

The `devops/databricks.yml` file defines the wheel artifact:

```yaml
artifacts:
  common_data_platform:
    type: whl
    path: ..
    build: |
      python -m build --wheel
```

### 2. Deployment Process

When you run `databricks bundle deploy`, the following happens:

1. The wheel is built from the source code
2. The wheel is uploaded to the workspace at: `/Workspace/.bundle/{environment}/artifacts/dist/`
3. The wheel is automatically available to jobs that use `python_wheel_task`

### 3. Using the Wheel in Jobs

Jobs defined in the bundle automatically have access to the wheel:

```yaml
python_wheel_task:
  package_name: ${var.package_name}  # common_data_platform
  entry_point: run_bronze_ingestion
```

### 4. Using the Wheel in Notebooks

For interactive notebooks, you have several options:

#### Option 1: Install from Bundle Artifacts (Recommended)
After deploying the bundle, install the wheel from the workspace:
```python
%pip install /Workspace/.bundle/dev/artifacts/dist/common_data_platform-*.whl --force-reinstall
```

#### Option 2: Install by Package Name
If the wheel is registered in the workspace:
```python
%pip install common_data_platform --force-reinstall
```

## Deployment Commands

### Local Development
```bash
cd devops
databricks bundle deploy --target dev
```

### CI/CD Pipeline
The GitHub Actions workflow automatically:
1. Builds the wheel
2. Deploys the bundle to the appropriate environment
3. Makes the wheel available to all jobs and clusters

## Benefits

1. **No DBFS Dependencies**: Everything is managed through the workspace file system
2. **Version Control**: Each deployment is tracked with git metadata
3. **Consistency**: Same wheel is used across all jobs in an environment
4. **Automation**: CI/CD pipeline handles all deployments

## Troubleshooting

### Wheel Not Found
If you get an error that the wheel is not found:
1. Ensure the bundle has been deployed: `databricks bundle deploy --target {env}`
2. Check the workspace path: `/Workspace/.bundle/{env}/artifacts/dist/`
3. Verify the wheel was built successfully in the deployment logs

### Import Errors
If you get import errors after installing the wheel:
1. Restart the Python kernel: `dbutils.library.restartPython()`
2. Ensure all dependencies are installed
3. Check that the wheel version matches your code

### Job Failures
If jobs fail to find the package:
1. Verify the `package_name` in the job configuration matches `common_data_platform`
2. Check that the bundle deployment completed successfully
3. Review the job logs for specific error messages