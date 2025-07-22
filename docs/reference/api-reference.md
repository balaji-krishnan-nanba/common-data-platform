# API Reference

This document provides a comprehensive reference for the Common Data Platform Python API.

## Core Components

### ConfigManager

Manages configuration for the data ingestion framework.

```python
from src.core.config_manager import ConfigManager

# Initialize with default path
config_manager = ConfigManager()

# Initialize with custom path
config_manager = ConfigManager("/custom/config/path")
```

#### Methods

**get_catalog_name(layer: str) -> str**

Get catalog name in format: `<project>-<env>-<layer>`.

```python
bronze_catalog = config_manager.get_catalog_name("bronze")
# Returns: "cddp-dev-bronze"
```

**load_source_config(source_type: str) -> Dict[str, Any]**

Load source configuration for specified type.

```python
excel_config = config_manager.load_source_config("excel")
```

### FileIngester

Handles ingestion from file-based sources (Excel, CSV, JSON, Parquet).

```python
from src.ingestion.file_ingester import FileIngester

ingester = FileIngester(spark, config_manager, catalog_manager, secret_manager)
```

#### Methods

**ingest(source_config, target_config, **kwargs) -> Dict[str, Any]**

Ingest data from file sources to bronze layer.

```python
result = ingester.ingest(
    source_config=source_def,
    target_config=target_config,
    batch_id="batch_123",
    write_mode="append",
    fail_on_error=True
)
```

## CLI Commands

### Bronze Ingestion

```bash
python -m src.cli run_bronze_ingestion --source daily_sales_excel
```

### Silver Transformation

```bash
python -m src.cli run_silver_transformation --source daily_sales_excel
```

### Source Validation

```bash
python -m src.cli validate_source_config --source daily_sales_excel
```

### List Sources

```bash
python -m src.cli list_sources
```