# Common Data Platform

A simple, production-ready data ingestion framework for Azure Databricks.

## Quick Start

```bash
# Install
pip install common-data-platform

# Run your first pipeline
cdp run-bronze-ingestion --inline-config '{
  "name": "my_data",
  "type": "excel",
  "connection": {
    "storage_account": "myaccount",
    "container": "raw-data",
    "path": "data/*.xlsx"
  }
}'
```

## Documentation

See [docs/GUIDE.md](docs/GUIDE.md) for complete documentation.

## Key Features

- ✅ **Simple**: Minimal configuration with smart defaults
- ✅ **Unity Catalog**: Full support for Databricks Unity Catalog
- ✅ **Production Ready**: Built-in error handling, retries, and monitoring
- ✅ **Extensible**: Support for Excel, CSV, Oracle, and more
- ✅ **Secure**: No hardcoded credentials, SQL injection protection

## Requirements

- Python 3.8-3.12
- PySpark 4.0+ (for Python 3.12)
- Azure Databricks Runtime 16.4+

## Architecture

Follows medallion architecture with Unity Catalog:
- **Bronze**: Raw data ingestion
- **Silver**: Cleaned and standardized data
- **Gold**: Business-ready aggregates

## License

MIT