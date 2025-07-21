"""PySpark-based transformer for complex transformations."""

from typing import Dict, Any, Optional, Callable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
import importlib.util
import sys

from .transformation_engine import TransformationEngine
from ..core.config_manager import ConfigManager
from ..utilities.sql_utils import SQLSanitizer
from ..utilities.decorators import handle_errors, log_performance
import ast


class PySparkTransformer(TransformationEngine):
    """Transformer that uses PySpark code for transformations."""
    
    def __init__(self, spark: SparkSession, config_manager: ConfigManager):
        """Initialize PySpark transformer.
        
        Args:
            spark: Active Spark session
            config_manager: Configuration manager instance
        """
        super().__init__(spark, config_manager)
        self.sql_sanitizer = SQLSanitizer()
        
    @handle_errors("pyspark_transformation", raise_on_error=False, default_return=False)
    @log_performance("transformation")
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
            
        # Validate target table name
        safe_target_table = self.sql_sanitizer.validate_table_name(target_table)
        writer.saveAsTable(safe_target_table)
        
        # Log statistics
        count = self.spark.table(target_table).count()
        self.logger.log_info(f"Transformation complete. Rows written: {count}")
        
        return True
            
    @handle_errors("custom_transform", raise_on_error=True)
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
            # Use inline function from config with safe execution
            transform_code = config.get('transform_code', '')
            
            # Parse and validate the code first
            try:
                tree = ast.parse(transform_code, mode='exec')
                # Check for dangerous operations
                for node in ast.walk(tree):
                    if isinstance(node, (ast.Import, ast.ImportFrom)):
                        # Allow only safe imports
                        if node.module not in ['pyspark.sql.functions', 'datetime', 'math']:
                            raise ValueError(f"Import of {node.module} not allowed")
                    elif isinstance(node, ast.Name) and node.id in ['__import__', 'eval', 'exec', 'compile']:
                        raise ValueError(f"Use of {node.id} not allowed")
            except Exception as e:
                raise ValueError(f"Invalid transform code: {str(e)}")
            
            # Execute in restricted environment
            safe_globals = {
                'spark': self.spark,
                'df': df,
                '__builtins__': {
                    'len': len,
                    'range': range,
                    'str': str,
                    'int': int,
                    'float': float,
                    'bool': bool,
                    'list': list,
                    'dict': dict,
                    'tuple': tuple,
                    'set': set,
                    'print': print,
                }
            }
            local_vars = {}
            exec(compile(tree, '<transform>', 'exec'), safe_globals, local_vars)
            transform_func = local_vars.get(function_name)
            
        if not transform_func:
            raise ValueError(f"Transform function {function_name} not found")
            
        return transform_func(df)
        
    @handle_errors("aggregation_transform", raise_on_error=True)
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
            
        # Build aggregation with safe column names
        safe_group_by = []
        for col in group_by_cols:
            safe_col = self.sql_sanitizer.validate_identifier(col, "group by column")
            safe_group_by.append(safe_col)
            
        agg_list = []
        allowed_agg_funcs = ['sum', 'avg', 'count', 'min', 'max', 'first', 'last', 'stddev', 'variance']
        
        for col_name, agg_func in agg_expressions.items():
            safe_col = self.sql_sanitizer.validate_identifier(col_name, "aggregation column")
            if agg_func.lower() not in allowed_agg_funcs:
                raise ValueError(f"Aggregation function {agg_func} not allowed")
            agg_list.append(f"{agg_func}({safe_col}) as {safe_col}_{agg_func}")
            
        agg_query = f"""
        SELECT {', '.join(safe_group_by)}, {', '.join(agg_list)}
        FROM input_table
        GROUP BY {', '.join(safe_group_by)}
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