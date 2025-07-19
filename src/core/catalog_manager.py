"""Unity Catalog management for creating and managing catalogs, schemas, and tables."""

from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession
import logging

from .config_manager import ConfigManager

logger = logging.getLogger(__name__)


class CatalogManager:
    """Manages Unity Catalog operations for the data platform."""
    
    def __init__(self, spark: SparkSession, config_manager: ConfigManager):
        """
        Initialize catalog manager.
        
        Args:
            spark: Active Spark session
            config_manager: Configuration manager instance
        """
        self.spark = spark
        self.config_manager = config_manager
        
    def create_all_catalogs(self, environments: Optional[List[str]] = None) -> None:
        """
        Create all catalogs for specified environments.
        
        Args:
            environments: List of environments. Defaults to current environment only.
        """
        if environments is None:
            environments = [self.config_manager.environment]
            
        for env in environments:
            # Temporarily set environment
            original_env = self.config_manager.environment
            self.config_manager.environment = env
            
            try:
                for layer in ['bronze', 'silver', 'gold']:
                    catalog_name = self.config_manager.get_catalog_name(layer)
                    self.create_catalog(catalog_name, layer, env)
                    
                    # Create schemas for each source system
                    source_schemas = self._get_source_schemas()
                    for schema in source_schemas:
                        self.create_schema(catalog_name, schema)
                        
            finally:
                # Restore original environment
                self.config_manager.environment = original_env
                
        logger.info(f"Successfully created all catalogs for environments: {environments}")
    
    def create_catalog(self, catalog_name: str, layer: str, environment: str) -> None:
        """
        Create a Unity Catalog if it doesn't exist.
        
        Args:
            catalog_name: Name of the catalog
            layer: Medallion layer (bronze, silver, gold)
            environment: Environment (dev, test, prod)
        """
        try:
            logger.info(f"Creating catalog: {catalog_name}")
            
            create_sql = f"""
                CREATE CATALOG IF NOT EXISTS `{catalog_name}`
                COMMENT 'Medallion layer {layer} for {environment} environment'
            """
            
            self.spark.sql(create_sql)
            
            # Set catalog properties
            properties_sql = f"""
                ALTER CATALOG `{catalog_name}`
                SET PROPERTIES (
                    'layer' = '{layer}',
                    'environment' = '{environment}',
                    'project_code' = '{self.config_manager.project_code}'
                )
            """
            
            self.spark.sql(properties_sql)
            
            logger.info(f"Successfully created catalog: {catalog_name}")
            
        except Exception as e:
            logger.error(f"Error creating catalog {catalog_name}: {str(e)}")
            raise
    
    def create_schema(self, catalog_name: str, schema_name: str) -> None:
        """
        Create a schema within a catalog.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
        """
        try:
            logger.info(f"Creating schema: {catalog_name}.{schema_name}")
            
            create_sql = f"""
                CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`
                COMMENT 'Schema for {schema_name} data'
            """
            
            self.spark.sql(create_sql)
            
            logger.info(f"Successfully created schema: {catalog_name}.{schema_name}")
            
        except Exception as e:
            logger.error(f"Error creating schema {catalog_name}.{schema_name}: {str(e)}")
            raise
    
    def create_table(
        self, 
        catalog_name: str, 
        schema_name: str, 
        table_name: str,
        columns: List[Dict[str, Any]],
        partition_columns: Optional[List[str]] = None,
        table_properties: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Create a Delta table with specified schema.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            columns: List of column definitions
            partition_columns: Optional list of partition columns
            table_properties: Optional table properties
        """
        try:
            full_table_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
            logger.info(f"Creating table: {full_table_name}")
            
            # Build column definitions
            column_defs = []
            for col in columns:
                col_def = f"`{col['name']}` {col['type']}"
                if not col.get('nullable', True):
                    col_def += " NOT NULL"
                if 'comment' in col:
                    col_def += f" COMMENT '{col['comment']}'"
                column_defs.append(col_def)
            
            # Build CREATE TABLE statement
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {', '.join(column_defs)}
                ) USING DELTA
            """
            
            # Add partitioning
            if partition_columns:
                create_sql += f" PARTITIONED BY ({', '.join(partition_columns)})"
            
            # Add table properties
            if table_properties:
                props = [f"'{k}' = '{v}'" for k, v in table_properties.items()]
                create_sql += f" TBLPROPERTIES ({', '.join(props)})"
            
            self.spark.sql(create_sql)
            
            logger.info(f"Successfully created table: {full_table_name}")
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise
    
    def table_exists(self, catalog_name: str, schema_name: str, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            tables = self.spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`").collect()
            return any(row.tableName == table_name for row in tables)
        except Exception:
            return False
    
    def get_table_schema(self, catalog_name: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Get schema of an existing table.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of column definitions
        """
        full_table_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
        
        schema_df = self.spark.sql(f"DESCRIBE TABLE {full_table_name}")
        
        columns = []
        for row in schema_df.collect():
            if row.col_name and not row.col_name.startswith("#"):
                columns.append({
                    'name': row.col_name,
                    'type': row.data_type,
                    'comment': row.comment or ''
                })
        
        return columns
    
    def _get_source_schemas(self) -> List[str]:
        """
        Get list of source schemas to create.
        
        Returns:
            List of schema names
        """
        # Base schemas for all environments
        schemas = ['excel_data', 'csv_data', 'oracle_data']
        
        # Add gold layer specific schemas
        if 'gold' in self.config_manager.get_all_catalogs():
            schemas.extend(['analytics', 'reporting'])
            
        return schemas
    
    def drop_catalog(self, catalog_name: str, cascade: bool = False) -> None:
        """
        Drop a catalog.
        
        Args:
            catalog_name: Name of the catalog to drop
            cascade: Whether to drop all contents (use with caution)
        """
        try:
            cascade_clause = "CASCADE" if cascade else "RESTRICT"
            drop_sql = f"DROP CATALOG IF EXISTS `{catalog_name}` {cascade_clause}"
            
            self.spark.sql(drop_sql)
            logger.info(f"Successfully dropped catalog: {catalog_name}")
            
        except Exception as e:
            logger.error(f"Error dropping catalog {catalog_name}: {str(e)}")
            raise