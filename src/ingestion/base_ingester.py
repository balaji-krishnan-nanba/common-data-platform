"""Base ingester class for all data ingestion operations."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
import logging
from datetime import datetime

from ..utilities.error_handler import retry_on_failure, DataPipelineError
from ..utilities.logger import DataPipelineLogger

logger = logging.getLogger(__name__)


class BaseIngester(ABC):
    """Abstract base class for all data ingesters."""
    
    def __init__(
        self, 
        spark: SparkSession, 
        config_manager,
        secret_manager
    ):
        """
        Initialize base ingester.
        
        Args:
            spark: Active Spark session
            config_manager: Configuration manager instance
            secret_manager: Secret manager instance
        """
        self.spark = spark
        self.config_manager = config_manager
        self.secret_manager = secret_manager
        self.pipeline_logger = DataPipelineLogger(__name__)
        
    @abstractmethod
    def ingest(
        self, 
        source_config: Dict[str, Any], 
        target_config: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """
        Ingest data from source to target.
        
        Args:
            source_config: Source configuration
            target_config: Target configuration
            
        Returns:
            Ingestion results dictionary
        """
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate ingestion configuration.
        
        Args:
            config: Configuration to validate
        """
        required_fields = self.get_required_config_fields()
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required configuration field: {field}")
    
    @abstractmethod
    def get_required_config_fields(self) -> List[str]:
        """
        Get list of required configuration fields.
        
        Returns:
            List of required field names
        """
        pass
    
    def add_audit_columns(self, df: DataFrame, source_info: Dict[str, Any]) -> DataFrame:
        """
        Add audit columns to DataFrame.
        
        Args:
            df: DataFrame to augment
            source_info: Source information for audit columns
            
        Returns:
            DataFrame with audit columns added
        """
        try:
            from pyspark.sql.functions import current_timestamp, lit
            
            # Add standard audit columns
            df = df.withColumn("ingestion_timestamp", current_timestamp())
            df = df.withColumn("source_system", lit(source_info.get("source_system", "unknown")))
            
            # Add source file path if available
            if "source_file" in source_info:
                df = df.withColumn("source_file", lit(source_info["source_file"]))
            
            # Add batch/run identifier
            if "batch_id" in source_info:
                df = df.withColumn("batch_id", lit(source_info["batch_id"]))
            
            return df
            
        except Exception as e:
            logger.error(f"Error adding audit columns: {str(e)}")
            raise
    
    def create_target_table_if_not_exists(
        self, 
        target_config: Dict[str, Any],
        sample_df: Optional[DataFrame] = None
    ) -> None:
        """
        Create target table if it doesn't exist.
        
        Args:
            target_config: Target table configuration
            sample_df: Sample DataFrame to infer schema from
        """
        try:
            catalog_name = target_config["catalog"]
            schema_name = target_config["schema"]
            table_name = target_config["table"]
            
            # Check if table exists and create if needed
            full_table_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
            
            if not self._table_exists(catalog_name, schema_name, table_name):
                logger.info(f"Creating table: {full_table_name}")
                
                # For Delta tables, we can use the sample DataFrame to create the table
                if sample_df:
                    # Create table using DataFrame write operation (Unity Catalog will manage the path)
                    sample_df.limit(0).write \
                        .mode("ignore") \
                        .saveAsTable(full_table_name)
                    logger.info(f"Successfully created table: {full_table_name}")
                else:
                    logger.warning(f"No sample data available to create table schema for {full_table_name}")
                
        except Exception as e:
            logger.error(f"Error creating target table: {str(e)}")
            raise
    
    def _table_exists(self, catalog_name: str, schema_name: str, table_name: str) -> bool:
        """
        Check if a table exists using Spark SQL.
        
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
            # Schema might not exist yet
            return False
    
    def _infer_columns_from_dataframe(self, df: DataFrame) -> List[Dict[str, Any]]:
        """
        Infer column definitions from DataFrame schema.
        
        Args:
            df: DataFrame to infer schema from
            
        Returns:
            List of column definitions
        """
        columns = []
        
        for field in df.schema.fields:
            col_def = {
                "name": field.name,
                "type": str(field.dataType).lower(),
                "nullable": field.nullable
            }
            columns.append(col_def)
        
        return columns
    
    @retry_on_failure(max_attempts=3, backoff_seconds=2)
    def write_to_target(
        self, 
        df: DataFrame, 
        target_config: Dict[str, Any],
        mode: str = "append"
    ) -> None:
        """
        Write DataFrame to target table.
        
        Args:
            df: DataFrame to write
            target_config: Target configuration
            mode: Write mode (append, overwrite, etc.)
        """
        try:
            catalog_name = target_config["catalog"]
            schema_name = target_config["schema"]
            table_name = target_config["table"]
            
            full_table_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
            
            logger.info(f"Writing {df.count()} rows to {full_table_name}")
            
            # Write DataFrame
            writer = df.write.mode(mode)
            
            # Add partition columns if specified
            partition_cols = target_config.get("partition_columns")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            # Add write options
            write_options = target_config.get("write_options", {})
            for key, value in write_options.items():
                writer = writer.option(key, value)
            
            # Perform the write
            writer.saveAsTable(full_table_name)
            
            logger.info(f"Successfully wrote data to {full_table_name}")
            
        except Exception as e:
            logger.error(f"Error writing to target: {str(e)}")
            raise
    
    def get_ingestion_metadata(
        self, 
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        records_processed: int,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Generate ingestion metadata.
        
        Args:
            source_config: Source configuration
            target_config: Target configuration
            records_processed: Number of records processed
            start_time: Ingestion start time
            end_time: Ingestion end time
            
        Returns:
            Metadata dictionary
        """
        duration = (end_time - start_time).total_seconds()
        
        return {
            "source": {
                "type": source_config.get("type"),
                "name": source_config.get("name"),
                "location": source_config.get("path", source_config.get("table"))
            },
            "target": {
                "catalog": target_config["catalog"],
                "schema": target_config["schema"],
                "table": target_config["table"]
            },
            "execution": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "records_processed": records_processed
            },
            "environment": self.config_manager.environment,
            "project_code": self.config_manager.project_code
        }