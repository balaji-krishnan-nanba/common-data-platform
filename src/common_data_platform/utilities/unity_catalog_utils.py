"""Unity Catalog utilities for Databricks Runtime 16.4 compatibility."""

import logging
import re
from typing import Dict, List, Optional, Tuple, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from .error_handler import DataPipelineError, ConfigurationError

logger = logging.getLogger(__name__)


class UnityCatalogValidator:
    """Validator for Unity Catalog operations in DBR 16.4."""
    
    # Valid name pattern for Unity Catalog objects
    UC_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_]+$')
    
    # Reserved keywords that cannot be used as names
    RESERVED_KEYWORDS = {
        'select', 'from', 'where', 'insert', 'update', 'delete', 'create', 
        'drop', 'alter', 'table', 'database', 'schema', 'catalog'
    }
    
    def __init__(self, spark: SparkSession):
        """Initialize Unity Catalog validator.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self._cache_current_catalog()
        
    def _cache_current_catalog(self):
        """Cache the current catalog for reference."""
        try:
            self.current_catalog = self.spark.sql("SELECT current_catalog()").first()[0]
        except Exception as e:
            logger.warning(f"Could not get current catalog: {str(e)}")
            self.current_catalog = None
            
    def validate_three_level_namespace(self, table_name: str) -> Tuple[str, str, str]:
        """Validate and parse three-level namespace (catalog.schema.table).
        
        Args:
            table_name: Table name to validate
            
        Returns:
            Tuple of (catalog, schema, table)
            
        Raises:
            ConfigurationError: If table name is invalid
        """
        parts = table_name.split('.')
        
        if len(parts) != 3:
            raise ConfigurationError(
                f"Invalid table name '{table_name}'. Unity Catalog requires "
                f"three-level namespace: catalog.schema.table\n"
                f"Example: myproject-dev-bronze.excel_data.sales_table\n"
                f"Got {len(parts)} parts: {parts}"
            )
            
        catalog, schema, table = parts
        
        # Validate each part
        for part, part_type in [(catalog, "catalog"), (schema, "schema"), (table, "table")]:
            self._validate_name(part, part_type)
            
        return catalog, schema, table
        
    def _validate_name(self, name: str, object_type: str):
        """Validate Unity Catalog object name.
        
        Args:
            name: Name to validate
            object_type: Type of object (catalog, schema, table)
            
        Raises:
            ConfigurationError: If name is invalid
        """
        # Check if empty
        if not name:
            raise ConfigurationError(f"Empty {object_type} name is not allowed")
            
        # Check length (Unity Catalog limit is 255 characters)
        if len(name) > 255:
            raise ConfigurationError(
                f"{object_type.capitalize()} name '{name}' exceeds 255 character limit"
            )
            
        # Check pattern
        if not self.UC_NAME_PATTERN.match(name):
            raise ConfigurationError(
                f"Invalid {object_type} name '{name}'. Only alphanumeric characters "
                f"and underscores are allowed.\n"
                f"Pattern: ^[a-zA-Z0-9_]+$\n"
                f"Examples: my_table, sales_data_2024, customer_orders"
            )
            
        # Check reserved keywords
        if name.lower() in self.RESERVED_KEYWORDS:
            raise ConfigurationError(
                f"'{name}' is a reserved keyword and cannot be used as {object_type} name"
            )
            
    def ensure_catalog_exists(self, catalog_name: str, 
                            storage_root: Optional[str] = None,
                            comment: Optional[str] = None) -> bool:
        """Ensure catalog exists, create if it doesn't.
        
        Args:
            catalog_name: Catalog name
            storage_root: Optional storage location for managed tables
            comment: Optional catalog comment
            
        Returns:
            bool: True if catalog was created, False if already existed
        """
        self._validate_name(catalog_name, "catalog")
        
        try:
            # Check if catalog exists
            # Catalogs are typically limited in number, safe to collect
            existing_catalogs = [row.catalog for row in self.spark.sql("SHOW CATALOGS").collect()]
            
            if catalog_name in existing_catalogs:
                logger.info(f"Catalog '{catalog_name}' already exists")
                return False
                
            # Create catalog
            create_sql = f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`"
            
            if storage_root:
                # Validate storage path for Unity Catalog volumes
                valid_prefixes = ('abfss://', '/Volumes/', 's3://', 'gs://', 'wasbs://', 's3a://')
                if not storage_root.startswith(valid_prefixes):
                    raise ConfigurationError(
                        f"Invalid storage root '{storage_root}'. Must start with one of: "
                        f"{', '.join(valid_prefixes)}\n"
                        f"Examples:\n"
                        f"  - abfss://container@account.dfs.core.windows.net/path\n"
                        f"  - /Volumes/catalog/schema/volume/path\n"
                        f"  - s3://bucket/path"
                    )
                create_sql += f" MANAGED LOCATION '{storage_root}'"
                
            if comment:
                create_sql += f" COMMENT '{comment}'"
                
            self.spark.sql(create_sql)
            logger.info(f"Created catalog '{catalog_name}'")
            return True
            
        except Exception as e:
            raise DataPipelineError(
                f"Failed to ensure catalog '{catalog_name}' exists",
                context={"catalog": catalog_name, "error": str(e)}
            )
            
    def ensure_schema_exists(self, catalog_name: str, schema_name: str,
                           storage_root: Optional[str] = None,
                           comment: Optional[str] = None) -> bool:
        """Ensure schema exists, create if it doesn't.
        
        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            storage_root: Optional storage location for managed tables
            comment: Optional schema comment
            
        Returns:
            bool: True if schema was created, False if already existed
        """
        self._validate_name(catalog_name, "catalog")
        self._validate_name(schema_name, "schema")
        
        try:
            # Ensure catalog exists first
            self.ensure_catalog_exists(catalog_name)
            
            # Check if schema exists
            # Schemas are typically limited in number, safe to collect
            existing_schemas = [
                row.databaseName 
                for row in self.spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`").collect()
            ]
            
            if schema_name in existing_schemas:
                logger.info(f"Schema '{catalog_name}.{schema_name}' already exists")
                return False
                
            # Create schema
            create_sql = f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`"
            
            if storage_root:
                # Validate storage path
                valid_prefixes = ('abfss://', '/Volumes/', 's3://', 'gs://', 'wasbs://', 's3a://')
                if not storage_root.startswith(valid_prefixes):
                    raise ConfigurationError(
                        f"Invalid storage root '{storage_root}'. Must start with one of: "
                        f"{', '.join(valid_prefixes)}\n"
                        f"Examples:\n"
                        f"  - abfss://container@account.dfs.core.windows.net/path\n"
                        f"  - /Volumes/catalog/schema/volume/path\n"
                        f"  - s3://bucket/path"
                    )
                create_sql += f" MANAGED LOCATION '{storage_root}'"
                
            if comment:
                create_sql += f" COMMENT '{comment}'"
                
            self.spark.sql(create_sql)
            logger.info(f"Created schema '{catalog_name}.{schema_name}'")
            return True
            
        except Exception as e:
            raise DataPipelineError(
                f"Failed to ensure schema '{catalog_name}.{schema_name}' exists",
                context={"catalog": catalog_name, "schema": schema_name, "error": str(e)}
            )
            
    def validate_table_access(self, table_name: str, operation: str = "SELECT") -> bool:
        """Validate user has access to perform operation on table.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            operation: Operation to check (SELECT, INSERT, UPDATE, DELETE)
            
        Returns:
            bool: True if user has access
            
        Raises:
            DataPipelineError: If access check fails
        """
        catalog, schema, table = self.validate_three_level_namespace(table_name)
        
        try:
            # Try to get table metadata
            self.spark.sql(f"DESCRIBE TABLE `{catalog}`.`{schema}`.`{table}`")
            
            # For write operations, check if table is not read-only
            if operation in ["INSERT", "UPDATE", "DELETE"]:
                table_info = self.spark.sql(
                    f"SHOW TBLPROPERTIES `{catalog}`.`{schema}`.`{table}`"
                ).collect()
                
                # Check if table is marked as read-only
                for row in table_info:
                    if row['key'] == 'read_only' and row['value'].lower() == 'true':
                        raise DataPipelineError(
                            f"Table '{table_name}' is read-only",
                            context={"operation": operation}
                        )
                        
            return True
            
        except Exception as e:
            if "Table or view not found" in str(e):
                logger.warning(f"Table '{table_name}' does not exist")
                return False
            elif "PERMISSION_DENIED" in str(e):
                raise DataPipelineError(
                    f"Permission denied for {operation} on table '{table_name}'.\n"
                    f"Please ensure:\n"
                    f"1. Your service principal has {operation} permission on the table\n"
                    f"2. The catalog '{catalog}' exists and you have USE CATALOG permission\n"
                    f"3. The schema '{schema}' exists and you have USE SCHEMA permission\n"
                    f"4. Run: GRANT {operation} ON TABLE `{catalog}`.`{schema}`.`{table}` TO `your-principal`",
                    context={"operation": operation, "table": table_name}
                )
            else:
                raise DataPipelineError(
                    f"Failed to validate access to table '{table_name}'",
                    context={"table": table_name, "error": str(e)}
                )
                
    def create_managed_volume(self, catalog_name: str, schema_name: str, 
                            volume_name: str, comment: Optional[str] = None) -> str:
        """Create a Unity Catalog managed volume for file storage.
        
        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            volume_name: Volume name
            comment: Optional volume comment
            
        Returns:
            str: Volume path (/Volumes/catalog/schema/volume)
        """
        self._validate_name(volume_name, "volume")
        
        try:
            # Ensure schema exists
            self.ensure_schema_exists(catalog_name, schema_name)
            
            # Create volume
            create_sql = f"""
            CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{volume_name}`
            """
            
            if comment:
                create_sql += f" COMMENT '{comment}'"
                
            self.spark.sql(create_sql)
            
            volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
            logger.info(f"Created volume at {volume_path}")
            
            return volume_path
            
        except Exception as e:
            raise DataPipelineError(
                f"Failed to create volume '{catalog_name}.{schema_name}.{volume_name}'",
                context={
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "volume": volume_name,
                    "error": str(e)
                }
            )
            
    def get_table_location(self, table_name: str) -> Optional[str]:
        """Get the physical location of a table.
        
        Args:
            table_name: Full table name
            
        Returns:
            Optional[str]: Table location or None if managed table
        """
        catalog, schema, table = self.validate_three_level_namespace(table_name)
        
        try:
            result = self.spark.sql(
                f"DESCRIBE TABLE EXTENDED `{catalog}`.`{schema}`.`{table}`"
            ).collect()
            
            for row in result:
                if row['col_name'] == 'Location':
                    return row['data_type']
                    
            return None
            
        except Exception as e:
            logger.error(f"Failed to get table location: {str(e)}")
            return None
            
    def optimize_delta_table(self, table_name: str, 
                           zorder_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Optimize a Delta table with optional Z-ordering.
        
        Args:
            table_name: Full table name
            zorder_columns: Optional columns to Z-order by
            
        Returns:
            Dict with optimization statistics
        """
        catalog, schema, table = self.validate_three_level_namespace(table_name)
        
        try:
            # Run OPTIMIZE command
            optimize_sql = f"OPTIMIZE `{catalog}`.`{schema}`.`{table}`"
            
            if zorder_columns:
                # Validate column names
                for col in zorder_columns:
                    self._validate_name(col, "column")
                optimize_sql += f" ZORDER BY ({', '.join(zorder_columns)})"
                
            result = self.spark.sql(optimize_sql).collect()
            
            # Extract statistics
            stats = {
                "files_added": 0,
                "files_removed": 0,
                "bytes_added": 0,
                "bytes_removed": 0
            }
            
            for row in result:
                stats["files_added"] += row.get("numFilesAdded", 0)
                stats["files_removed"] += row.get("numFilesRemoved", 0)
                stats["bytes_added"] += row.get("bytesAdded", 0)
                stats["bytes_removed"] += row.get("bytesRemoved", 0)
                
            logger.info(f"Optimized table {table_name}: {stats}")
            return stats
            
        except Exception as e:
            raise DataPipelineError(
                f"Failed to optimize table '{table_name}'",
                context={"table": table_name, "error": str(e)}
            )