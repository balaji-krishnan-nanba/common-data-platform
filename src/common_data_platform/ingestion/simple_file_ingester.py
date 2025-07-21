"""Simplified file ingester for Unity Catalog volumes."""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
import logging
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class SimpleFileIngester:
    """Simplified ingester for file-based data sources using Unity Catalog volumes."""
    
    def __init__(self, spark: SparkSession, config_manager, secret_manager=None):
        """Initialize simple file ingester."""
        self.spark = spark
        self.config_manager = config_manager
        self.dbutils = spark._jvm.com.databricks.service.DBUtils.dbutils(spark)
    
    def read_csv(self, file_path: str, options: Dict[str, Any] = None) -> DataFrame:
        """Read CSV file with options."""
        options = options or {}
        reader = self.spark.read
        
        # Apply common CSV options
        reader = reader.option("header", options.get("header", "true"))
        reader = reader.option("inferSchema", options.get("inferSchema", "true"))
        
        # Apply additional options
        for key, value in options.items():
            if key not in ["header", "inferSchema"]:
                reader = reader.option(key, value)
        
        return reader.csv(file_path)
    
    def read_excel(self, file_path: str, options: Dict[str, Any] = None) -> DataFrame:
        """Read Excel file using pandas."""
        options = options or {}
        
        try:
            # Copy file to local temp
            local_path = f"/tmp/{os.path.basename(file_path)}"
            self.dbutils.fs.cp(file_path, f"file:{local_path}")
            
            # Read with pandas
            import pandas as pd
            sheet_name = options.get("sheet_name", 0)
            excel_df = pd.read_excel(local_path, sheet_name=sheet_name, engine='openpyxl')
            
            # Convert to Spark DataFrame
            df = self.spark.createDataFrame(excel_df)
            
            # Clean up
            os.remove(local_path)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {str(e)}")
            raise
    
    def add_metadata_columns(self, df: DataFrame, file_info: Dict[str, Any]) -> DataFrame:
        """Add metadata columns to DataFrame."""
        df = df.withColumn("_file_name", lit(file_info.get("file_name", "")))
        df = df.withColumn("_file_path", lit(file_info.get("file_path", "")))
        df = df.withColumn("_ingestion_timestamp", current_timestamp())
        df = df.withColumn("_batch_id", lit(file_info.get("batch_id", datetime.now().strftime("%Y%m%d_%H%M%S"))))
        return df
    
    def create_table_if_not_exists(self, catalog: str, schema: str, table: str) -> None:
        """Create schema and prepare for table creation."""
        try:
            # Create schema if not exists
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            logger.info(f"Schema {catalog}.{schema} ready")
        except Exception as e:
            logger.warning(f"Schema creation warning: {str(e)}")
    
    def write_to_table(self, df: DataFrame, catalog: str, schema: str, table: str, 
                      mode: str = "append", partition_by: List[str] = None) -> int:
        """Write DataFrame to Unity Catalog table."""
        try:
            # Ensure schema exists
            self.create_table_if_not_exists(catalog, schema, table)
            
            # Full table name
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"
            
            # Get row count
            row_count = df.count()
            logger.info(f"Writing {row_count} rows to {full_table_name}")
            
            # Write to table
            writer = df.write.mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.saveAsTable(full_table_name)
            
            logger.info(f"Successfully wrote {row_count} rows to {full_table_name}")
            return row_count
            
        except Exception as e:
            logger.error(f"Failed to write to table {catalog}.{schema}.{table}: {str(e)}")
            raise
    
    def ingest(self, source_config: Dict[str, Any], target_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Simplified ingestion from file sources.
        
        Args:
            source_config: Source file configuration
            target_config: Target table configuration
            
        Returns:
            Ingestion results
        """
        start_time = datetime.now()
        results = {
            "status": "failed",
            "records_processed": 0,
            "files_processed": 0,
            "errors": []
        }
        
        try:
            # Get file type and path
            file_type = source_config.get("type", "csv").lower()
            connection = source_config.get("connection", {})
            
            # Build file path
            storage_account = connection.get("storage_account", self.config_manager.get_env_var("AZURE_SOURCE_STORAGE_ACCOUNT"))
            container = connection.get("container", "raw-data")
            path_pattern = connection.get("path_pattern", "")
            
            # Construct ABFSS path
            base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path_pattern}"
            
            # List files to process
            files_to_process = []
            try:
                if "*" in path_pattern:
                    # Handle wildcard patterns
                    parent_path = base_path.rsplit("/", 1)[0]
                    all_items = self.dbutils.fs.ls(parent_path)
                    pattern = os.path.basename(path_pattern)
                    
                    for item in all_items:
                        if not item.isDir() and self._matches_pattern(item.name, pattern):
                            files_to_process.append(item.path)
                else:
                    # Direct path or directory
                    items = self.dbutils.fs.ls(base_path)
                    for item in items:
                        if not item.isDir() and item.name.endswith(f".{file_type}"):
                            files_to_process.append(item.path)
            except Exception as e:
                logger.warning(f"Error listing files at {base_path}: {str(e)}")
                # Try as single file
                files_to_process = [base_path]
            
            if not files_to_process:
                logger.warning(f"No {file_type} files found at {base_path}")
                results["status"] = "success"
                results["message"] = "No files found to process"
                return results
            
            # Process each file
            total_records = 0
            successful_files = 0
            
            for file_path in files_to_process:
                try:
                    logger.info(f"Processing file: {file_path}")
                    
                    # Read file based on type
                    if file_type == "csv":
                        df = self.read_csv(file_path, source_config.get("read_options", {}))
                    elif file_type in ["excel", "xlsx", "xls"]:
                        df = self.read_excel(file_path, source_config.get("read_options", {}))
                    else:
                        logger.warning(f"Unsupported file type: {file_type}")
                        continue
                    
                    # Add metadata
                    file_info = {
                        "file_name": os.path.basename(file_path),
                        "file_path": file_path,
                        "batch_id": datetime.now().strftime("%Y%m%d_%H%M%S")
                    }
                    df = self.add_metadata_columns(df, file_info)
                    
                    # Write to target
                    row_count = self.write_to_table(
                        df,
                        target_config["catalog"],
                        target_config["schema"],
                        target_config["table"],
                        mode="append",
                        partition_by=target_config.get("partition_columns")
                    )
                    
                    total_records += row_count
                    successful_files += 1
                    
                except Exception as e:
                    error_msg = f"Failed to process {file_path}: {str(e)}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
            
            # Update results
            results["status"] = "success" if successful_files > 0 else "failed"
            results["records_processed"] = total_records
            results["files_processed"] = successful_files
            results["total_files"] = len(files_to_process)
            results["target_table"] = f"{target_config['catalog']}.{target_config['schema']}.{target_config['table']}"
            
            end_time = datetime.now()
            results["duration_seconds"] = (end_time - start_time).total_seconds()
            
            logger.info(f"Ingestion completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Ingestion failed: {str(e)}")
            results["errors"].append(str(e))
            return results
    
    def _matches_pattern(self, filename: str, pattern: str) -> bool:
        """Simple pattern matching for wildcards."""
        import fnmatch
        return fnmatch.fnmatch(filename, pattern)