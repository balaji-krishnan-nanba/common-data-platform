"""PySpark-based transformer for complex transformations."""

from typing import Dict, Any, Optional, Callable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
import importlib.util
import sys

from .transformation_engine import TransformationEngine
from ..core.config_manager import ConfigManager


class PySparkTransformer(TransformationEngine):
    """Transformer that uses PySpark code for transformations."""
    
    def __init__(self, spark: SparkSession, config_manager: ConfigManager):
        """Initialize PySpark transformer.
        
        Args:
            spark: Active Spark session
            config_manager: Configuration manager instance
        """
        super().__init__(spark, config_manager)
        
    def transform(self, source_table: str, target_table: str,
                 transformation_config: Dict[str, Any]) -> bool:
        """Execute PySpark transformation.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            transformation_config: Config with transformation details
            
        Returns:
            bool: True if successful
        """
        operation_id = self.logger.start_operation("pyspark_transformation",
                                                  {"source": source_table,
                                                   "target": target_table})
        
        try:
            # Validate tables
            if not self.validate_tables(source_table, target_table):
                return False
                
            # Load source data
            self.logger.log_info(f"Loading source data from {source_table}")
            source_df = self.spark.table(source_table)
            
            # Apply transformation
            transform_type = transformation_config.get('transform_type', 'custom')
            
            if transform_type == 'custom':
                # Load custom transformation function
                transformed_df = self._apply_custom_transform(
                    source_df, transformation_config
                )
            elif transform_type == 'aggregation':
                transformed_df = self._apply_aggregation(
                    source_df, transformation_config
                )
            elif transform_type == 'filter':
                transformed_df = self._apply_filter(
                    source_df, transformation_config
                )
            elif transform_type == 'join':
                transformed_df = self._apply_join(
                    source_df, transformation_config
                )
            else:
                self.logger.log_error(f"Unknown transform type: {transform_type}")
                return False
                
            # Add audit columns
            transformed_df = self._add_audit_columns(transformed_df)
            
            # Write results
            write_mode = transformation_config.get('write_mode', 'overwrite')
            partition_cols = transformation_config.get('partition_columns', [])
            
            writer = transformed_df.write.mode(write_mode)
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
            self.logger.log_error(f"PySpark transformation failed: {str(e)}")
            self.logger.end_operation(operation_id, status="failed",
                                    error=str(e))
            return self.error_handler.handle_error(e)
            
    def _apply_custom_transform(self, df: DataFrame, 
                              config: Dict[str, Any]) -> DataFrame:
        """Apply custom transformation function.
        
        Args:
            df: Input DataFrame
            config: Transformation configuration
            
        Returns:
            DataFrame: Transformed DataFrame
        """
        # Get transformation function
        function_path = config.get('function_path')
        function_name = config.get('function_name', 'transform')
        
        if function_path:
            # Load external function
            spec = importlib.util.spec_from_file_location("transform_module", 
                                                         function_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            transform_func = getattr(module, function_name)
        else:
            # Use inline function from config
            transform_code = config.get('transform_code', '')
            local_vars = {}
            exec(transform_code, {"spark": self.spark, "df": df}, local_vars)
            transform_func = local_vars.get(function_name)
            
        if not transform_func:
            raise ValueError(f"Transform function {function_name} not found")
            
        return transform_func(df)
        
    def _apply_aggregation(self, df: DataFrame, 
                          config: Dict[str, Any]) -> DataFrame:
        """Apply aggregation transformation.
        
        Args:
            df: Input DataFrame
            config: Aggregation configuration
            
        Returns:
            DataFrame: Aggregated DataFrame
        """
        group_by_cols = config.get('group_by', [])
        agg_expressions = config.get('aggregations', {})
        
        if not group_by_cols or not agg_expressions:
            raise ValueError("group_by and aggregations required for aggregation")
            
        # Build aggregation
        agg_list = []
        for col_name, agg_func in agg_expressions.items():
            agg_list.append(f"{agg_func}({col_name}) as {col_name}_{agg_func}")
            
        agg_query = f"""
        SELECT {', '.join(group_by_cols)}, {', '.join(agg_list)}
        FROM input_table
        GROUP BY {', '.join(group_by_cols)}
        """
        
        df.createOrReplaceTempView("input_table")
        return self.spark.sql(agg_query)
        
    def _apply_filter(self, df: DataFrame, 
                     config: Dict[str, Any]) -> DataFrame:
        """Apply filter transformation.
        
        Args:
            df: Input DataFrame
            config: Filter configuration
            
        Returns:
            DataFrame: Filtered DataFrame
        """
        filter_condition = config.get('filter_condition')
        if not filter_condition:
            raise ValueError("filter_condition required for filter transformation")
            
        return df.filter(filter_condition)
        
    def _apply_join(self, df: DataFrame, 
                   config: Dict[str, Any]) -> DataFrame:
        """Apply join transformation.
        
        Args:
            df: Input DataFrame
            config: Join configuration
            
        Returns:
            DataFrame: Joined DataFrame
        """
        join_table = config.get('join_table')
        join_keys = config.get('join_keys', [])
        join_type = config.get('join_type', 'inner')
        
        if not join_table or not join_keys:
            raise ValueError("join_table and join_keys required for join")
            
        right_df = self.spark.table(join_table)
        return df.join(right_df, on=join_keys, how=join_type)
        
    def _add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Add audit columns to DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with audit columns
        """
        return df.withColumn("etl_processed_time", current_timestamp()) \
                 .withColumn("etl_source", lit("pyspark_transformer"))