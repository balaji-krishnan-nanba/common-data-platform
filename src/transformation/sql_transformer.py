"""SQL-based transformer for Spark SQL transformations."""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

from .transformation_engine import TransformationEngine
from ..core.config_manager import ConfigManager


class SQLTransformer(TransformationEngine):
    """Transformer that uses SQL queries for transformations."""
    
    def __init__(self, spark: SparkSession, config_manager: ConfigManager):
        """Initialize SQL transformer.
        
        Args:
            spark: Active Spark session
            config_manager: Configuration manager instance
        """
        super().__init__(spark, config_manager)
        
    def transform(self, source_table: str, target_table: str,
                 transformation_config: Dict[str, Any]) -> bool:
        """Execute SQL transformation.
        
        Args:
            source_table: Source table name
            target_table: Target table name  
            transformation_config: Config with 'query' key containing SQL
            
        Returns:
            bool: True if successful
        """
        operation_id = self.logger.start_operation("sql_transformation", 
                                                  {"source": source_table, 
                                                   "target": target_table})
        
        try:
            # Validate tables
            if not self.validate_tables(source_table, target_table):
                return False
                
            # Get SQL query from config
            sql_query = transformation_config.get('query')
            if not sql_query:
                self.logger.log_error("No SQL query provided in transformation config")
                return False
                
            # Replace placeholders in query
            sql_query = sql_query.replace('{source_table}', source_table)
            sql_query = sql_query.replace('{target_table}', target_table)
            
            self.logger.log_info(f"Executing SQL transformation: {target_table}")
            
            # Execute transformation
            df = self.spark.sql(sql_query)
            
            # Write results
            write_mode = transformation_config.get('write_mode', 'overwrite')
            partition_cols = transformation_config.get('partition_columns', [])
            
            writer = df.write.mode(write_mode)
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
                
            writer.saveAsTable(target_table)
            
            # Log statistics
            count = self.spark.table(target_table).count()
            self.logger.log_info(f"Transformation complete. Rows written: {count}")
            
            self.logger.end_operation(operation_id, status="success", 
                                    metrics={"rows_written": count})
            return True
            
        except Exception as e:
            self.logger.log_error(f"SQL transformation failed: {str(e)}")
            self.logger.end_operation(operation_id, status="failed", 
                                    error=str(e))
            return self.error_handler.handle_error(e)
            
    def validate_sql(self, sql_query: str) -> bool:
        """Validate SQL query syntax.
        
        Args:
            sql_query: SQL query to validate
            
        Returns:
            bool: True if valid
        """
        try:
            # Try to create execution plan without executing
            self.spark.sql(f"EXPLAIN {sql_query}")
            return True
        except Exception as e:
            self.logger.log_error(f"SQL validation failed: {str(e)}")
            return False