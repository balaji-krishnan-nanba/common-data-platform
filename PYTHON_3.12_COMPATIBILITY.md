# Python 3.12 Compatibility Guide

## Overview

This Common Data Platform now fully supports Python 3.12! Here's what you need to know:

## Updated Dependencies

To support Python 3.12, the following dependencies have been upgraded:

| Package | Old Version | New Version | Reason |
|---------|-------------|-------------|---------|
| pyspark | >=3.4.0 | >=4.0.0 | Python 3.12 support added in 4.0.0 |
| delta-spark | >=2.4.0 | >=4.0.0 | Compatibility with PySpark 4.0 |
| scipy | >=1.9.0 | >=1.10.0 | Python 3.12 support added in 1.10.0 |

## Important Requirements

### 1. Java Version
- **PySpark 4.0.0 requires Java 17 or later**
- Previous versions only required Java 8/11
- Ensure your environment has Java 17+ installed

### 2. Databricks Runtime
For Python 3.12 support in Databricks:
- **Use Databricks Runtime 17.0 or later**
- Earlier versions (including LTS versions like 15.4) use Python 3.11 or earlier
- Check your cluster configuration before deployment

### 3. Breaking Changes in PySpark 4.0
- **Drops support for Python 3.8**
- If you need Python 3.8 compatibility, you cannot use Python 3.12
- Some API changes may require code adjustments (though this codebase has been designed to be compatible)

## Installation

1. **Ensure Java 17+ is installed:**
   ```bash
   java -version  # Should show version 17 or higher
   ```

2. **Install the package with Python 3.12:**
   ```bash
   python3.12 -m pip install -e .
   ```

3. **For Databricks deployment:**
   - Create a cluster with DBR 17.0+
   - The cluster will automatically have Python 3.12

## Compatibility Matrix

| Python Version | PySpark Version | Databricks Runtime | Status |
|---------------|-----------------|-------------------|---------|
| 3.8 | 3.4.x | DBR 13.x-16.x | ❌ Not supported with current config |
| 3.9 | 4.0.x | DBR 17.0+ | ✅ Supported |
| 3.10 | 4.0.x | DBR 17.0+ | ✅ Supported |
| 3.11 | 4.0.x | DBR 17.0+ | ✅ Supported |
| 3.12 | 4.0.x | DBR 17.0+ | ✅ Supported |

## Verification

To verify your environment is correctly set up:

```python
import sys
import pyspark
import delta

print(f"Python version: {sys.version}")
print(f"PySpark version: {pyspark.__version__}")
print(f"Delta version: {delta.__version__}")

# Should show:
# Python version: 3.12.x
# PySpark version: 4.0.x or higher
# Delta version: 4.0.x or higher
```

## Rollback Option

If you need to use an older Python version or PySpark 3.x:

1. Update `pyproject.toml`:
   ```toml
   requires-python = ">=3.8,<3.12"
   dependencies = [
       "pyspark>=3.4.0,<4.0.0",
       "delta-spark>=2.4.0,<4.0.0",
       "scipy>=1.9.0,<2.0.0",
   ]
   ```

2. Remove Python 3.12 from classifiers
3. Use Databricks Runtime 13.x-16.x

## Known Issues

1. **great-expectations**: Version constraints (>=0.17.0,<0.19.0) may need testing with Python 3.12
2. **Oracle JDBC**: Ensure your Oracle JDBC driver is compatible with Java 17

## Support

For issues related to Python 3.12 compatibility:
1. Check Java version (must be 17+)
2. Verify PySpark installation
3. For Databricks, ensure DBR 17.0+ is used