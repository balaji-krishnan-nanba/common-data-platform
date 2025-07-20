"""Main transformation engine for orchestrating data transformations."""

from typing import Dict, Any, Optional
from abc import ABC, abstractmethod
import logging
from pyspark.sql import SparkSession, DataFrame

from ..core.config_manager import ConfigManager
from ..utils.logger import PipelineLogger
from ..utils.error_handler import ErrorHandler


class TransformationEngine(ABC):
    """Abstract base class for transformation engines."""
    
    def __init__(self, spark: SparkSession, config_manager: ConfigManager):
        """Initialize transformation engine.
        
        Args:
            spark: Active Spark session
            config_manager: Configuration manager instance
        """
        self.spark = spark
        self.config = config_manager
        self.logger = PipelineLogger(__name__)
        self.error_handler = ErrorHandler()
        
    @abstractmethod
    def transform(self, source_table: str, target_table: str, 
                 transformation_config: Dict[str, Any]) -> bool:
        """Execute transformation from source to target.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            transformation_config: Transformation configuration
            
        Returns:
            bool: True if successful
        """
        pass
        
    def validate_tables(self, source_table: str, target_table: str) -> bool:
        """Validate source and target tables.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            
        Returns:
            bool: True if validation passes
        """
        try:
            # Check if source table exists
            if not self.spark.catalog.tableExists(source_table):
                self.logger.log_error(f"Source table {source_table} does not exist")
                return False
                
            # Check if we can create target table
            target_db = target_table.split('.')[0] if '.' in target_table else 'default'
            if not self.spark.catalog.databaseExists(target_db):
                self.logger.log_info(f"Creating database {target_db}")
                self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")
                
            return True
            
        except Exception as e:
            self.logger.log_error(f"Table validation failed: {str(e)}")
            return False
            
    def get_table_schema(self, table_name: str) -> Optional[str]:
        """Get schema of a table.
        
        Args:
            table_name: Table name
            
        Returns:
            Optional[str]: Schema string or None
        """
        try:
            df = self.spark.table(table_name)
            return df.schema.json()
        except Exception as e:
            self.logger.log_error(f"Failed to get schema for {table_name}: {str(e)}")
            return None