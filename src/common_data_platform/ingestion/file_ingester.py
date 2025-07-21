"""File ingester for Excel, CSV, and other file-based sources."""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
import logging
from datetime import datetime
import fnmatch

from ..connectivity.adls_service import ADLSService
from ..utilities.schema_validator import SchemaValidator
from ..utilities.error_handler import DataPipelineError
from ..utilities.batch_processor import BatchProcessor
from ..utilities.distributed_lock import DistributedLock
from ..utilities.decorators import handle_errors, with_retry, validate_config, log_performance
from ..utilities.logger import DataPipelineLogger
from ..utilities.unity_catalog_utils import UnityCatalogValidator

logger = logging.getLogger(__name__)


class FileIngester:
    """Ingester for file-based data sources (Excel, CSV, JSON, Parquet)."""
    
    def __init__(self, spark: SparkSession, config_manager, secret_manager):
        """Initialize file ingester."""
        self.spark = spark
        self.config_manager = config_manager
        self.secret_manager = secret_manager
        self.schema_validator = SchemaValidator()
        self.pipeline_logger = DataPipelineLogger(__name__)
        self.uc_validator = UnityCatalogValidator(spark)
    
    def get_required_config_fields(self) -> List[str]:
        """Get required configuration fields for file ingestion."""
        return ["type", "connection", "target"]
    
    def validate_config(self, config: Dict[str, Any]) -> None:
        """Validate ingestion configuration."""
        required_fields = self.get_required_config_fields()
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required configuration field: {field}")
    
    def add_audit_columns(self, df: DataFrame, source_info: Dict[str, Any]) -> DataFrame:
        """Add audit columns to DataFrame."""
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
    
    def create_target_table_if_not_exists(
        self, 
        target_config: Dict[str, Any],
        sample_df: Optional[DataFrame] = None
    ) -> None:
        """Create target table if it doesn't exist."""
        catalog_name = target_config["catalog"]
        schema_name = target_config["schema"]
        table_name = target_config["table"]
        
        # Validate Unity Catalog namespace
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        self.uc_validator.validate_three_level_namespace(full_table_name)
        
        # Ensure catalog and schema exist
        self.uc_validator.ensure_schema_exists(catalog_name, schema_name)
        
        # Check if table exists and create if needed
        full_table_name_quoted = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
        
        if not self._table_exists(catalog_name, schema_name, table_name):
            logger.info(f"Creating table: {full_table_name}")
            
            # For Delta tables, we can use the sample DataFrame to create the table
            if sample_df:
                sample_df.limit(0).write \
                    .mode("ignore") \
                    .saveAsTable(full_table_name_quoted)
                logger.info(f"Successfully created table: {full_table_name}")
    
    def _table_exists(self, catalog_name: str, schema_name: str, table_name: str) -> bool:
        """Check if a table exists using Spark SQL."""
        try:
            tables = self.spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`").collect()
            return any(row.tableName == table_name for row in tables)
        except Exception:
            # Schema might not exist yet
            return False
    
    @with_retry(max_attempts=3, backoff_seconds=2)
    def write_to_target(
        self, 
        df: DataFrame, 
        target_config: Dict[str, Any],
        write_mode: str = "append"
    ) -> None:
        """Write DataFrame to target table."""
        catalog_name = target_config["catalog"]
        schema_name = target_config["schema"]
        table_name = target_config["table"]
        
        # Validate Unity Catalog namespace
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        self.uc_validator.validate_three_level_namespace(full_table_name)
        
        # Validate table access
        operation = "INSERT" if write_mode == "append" else "UPDATE"
        self.uc_validator.validate_table_access(full_table_name, operation)
        
        full_table_name_quoted = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
        
        # Get row count efficiently
        row_count = df.cache().count()
        logger.info(f"Writing {row_count} rows to {full_table_name}")
        
        # Write DataFrame
        writer = df.write.mode(write_mode)
        
        # Add partition columns if specified
        partition_cols = target_config.get("partition_columns")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        # Perform the write
        writer.saveAsTable(full_table_name_quoted)
        
        # Unpersist cached DataFrame
        df.unpersist()
        
        # Optimize table if it's Delta format and optimization is enabled
        if target_config.get("optimize_after_write", False):
            zorder_columns = target_config.get("zorder_columns", [])
            self.uc_validator.optimize_delta_table(full_table_name, zorder_columns)
    
    def get_ingestion_metadata(
        self, 
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        records_processed: int,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Generate ingestion metadata."""
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
    
    @handle_errors("file_ingestion", raise_on_error=False, default_return=None)
    @log_performance("ingestion")
    def ingest(
        self, 
        source_config: Dict[str, Any], 
        target_config: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """
        Ingest data from file sources to bronze layer.
        
        Args:
            source_config: File source configuration
            target_config: Target table configuration
            
        Returns:
            Ingestion results
        """
        start_time = datetime.now()
        
        # Validate configurations
        self.validate_config(source_config)
        self.validate_config(target_config)
        
        # Create ADLS service
        adls_service = ADLSService(
            spark=self.spark,
            config=source_config["connection"],
            secret_manager=self.secret_manager
        )
        
        # Test connection
        if not adls_service.test_connection():
            raise DataPipelineError(f"Failed to connect to ADLS for source {source_config.get('name')}")
        
        # Get list of files to process
        files_to_process = self._get_files_to_process(source_config, adls_service)
        
        if not files_to_process:
            logger.warning("No files found to process")
            return self._create_empty_result(source_config, target_config, start_time)
        
        # Initialize batch processor
        batch_size = source_config.get("ingestion", {}).get("batch_size", 100)
        batch_processor = BatchProcessor(self.spark, batch_size=batch_size)
        
        # Create read function for batch processor
        batch_id = kwargs.get("batch_id", start_time.strftime("%Y%m%d_%H%M%S"))
        
        def read_and_transform_file(file_path: str) -> DataFrame:
            # Read file
            df = self._read_file(adls_service, source_config, file_path)
            
            # Validate schema if defined
            if "schema" in source_config:
                df = self._validate_and_enforce_schema(df, source_config["schema"])
            
            # Add audit columns
            source_info = {
                "source_system": source_config.get("name", "file_system"),
                "source_file": file_path,
                "batch_id": batch_id
            }
            df = self.add_audit_columns(df, source_info)
            
            return df
        
        # Process files in batches
        fail_on_error = kwargs.get("fail_on_error", True)
        batch_iterator = batch_processor.process_files_in_batches(
            files_to_process,
            read_and_transform_file,
            fail_on_error=fail_on_error
        )
        
        # Create target table if needed (use first batch to get schema)
        first_batch = None
        try:
            first_batch = next(batch_iterator)
            self.create_target_table_if_not_exists(target_config, first_batch)
        except StopIteration:
            raise DataPipelineError("No files processed successfully")
        
        # Create new iterator that includes the first batch
        def batch_iterator_with_first():
            yield first_batch
            yield from batch_iterator
        
        # Write batches to target
        write_mode = kwargs.get("write_mode", "append")
        partition_cols = target_config.get("partition_by", [])
        
        target_table = f"{target_config['catalog']}.{target_config['schema']}.{target_config['table']}"
        write_stats = batch_processor.write_batches_to_target(
            batch_iterator_with_first(),
            target_table,
            write_mode=write_mode,
            partition_columns=partition_cols
        )
        
        total_records = write_stats["total_records"]
        
        # Mark files as processed with distributed locking
        if source_config.get("track_processed_files", True):
            lock_table = f"{target_config['catalog']}.{target_config['schema']}_tracking.file_process_locks"
            lock = DistributedLock(self.spark, lock_table)
            
            with lock.with_lock(f"file_tracking_{source_config['name']}", timeout_seconds=300):
                self._mark_files_processed(files_to_process, source_config)
        
        end_time = datetime.now()
        
        # Generate results
        result = {
            "status": "success",
            "files_processed": len(files_to_process),
            "records_processed": total_records,
            "metadata": self.get_ingestion_metadata(
                source_config, target_config, total_records, start_time, end_time
            )
        }
        
        return result or {
            "status": "failed",
            "error": "Unknown error",
            "metadata": self.get_ingestion_metadata(
                source_config, target_config, 0, start_time, datetime.now()
            )
        }
    
    @handle_errors("get_files", raise_on_error=True)
    def _get_files_to_process(
        self, 
        source_config: Dict[str, Any], 
        adls_service: ADLSService
    ) -> List[str]:
        """
        Get list of files to process based on configuration.
        
        Args:
            source_config: Source configuration
            adls_service: ADLS service instance
            
        Returns:
            List of file paths to process
        """
        ingestion_config = source_config.get("ingestion", {})
        mode = ingestion_config.get("mode", "full")
        
        if "path_pattern" in source_config["connection"]:
            # Handle date-based path patterns
            path_pattern = source_config["connection"]["path_pattern"]
            resolved_path = self._resolve_path_pattern(path_pattern)
            files = adls_service.list_files(resolved_path)
        else:
            # Static path
            path = source_config["connection"]["path"]
            files = adls_service.list_files(path)
        
        # Filter files by pattern if specified
        file_pattern = source_config["connection"].get("file_pattern")
        if file_pattern:
            files = [f for f in files if fnmatch.fnmatch(f.split("/")[-1], file_pattern)]
        
        # Handle incremental mode
        if mode == "incremental":
            files = self._filter_unprocessed_files(files, source_config)
        
        logger.info(f"Found {len(files)} files to process")
        return files
    
    def _resolve_path_pattern(self, path_pattern: str) -> str:
        """
        Resolve date-based path patterns.
        
        Args:
            path_pattern: Path pattern with date placeholders
            
        Returns:
            Resolved path
        """
        from datetime import datetime
        
        today = datetime.now()
        
        # Replace common date patterns
        resolved = path_pattern.format(
            year=today.year,
            month=f"{today.month:02d}",
            day=f"{today.day:02d}",
            date=today.strftime("%Y%m%d")
        )
        
        return resolved
    
    @handle_errors("read_file", raise_on_error=True)
    @with_retry(max_attempts=3, backoff_seconds=2)
    def _read_file(
        self, 
        adls_service: ADLSService, 
        source_config: Dict[str, Any], 
        file_path: str
    ) -> DataFrame:
        """
        Read individual file using ADLS service.
        
        Args:
            adls_service: ADLS service instance
            source_config: Source configuration
            file_path: Path to file
            
        Returns:
            DataFrame containing file data
        """
        file_type = source_config["type"]
        read_options = source_config.get("read_options", {})
        
        # Add file-specific options
        if file_type == "excel":
            if "sheet_name" in source_config:
                read_options["sheetName"] = source_config["sheet_name"]
            if "header_row" in source_config:
                read_options["header"] = str(source_config["header_row"] > 0).lower()
        
        # Read the file
        df = adls_service.read(
            path=file_path,
            file_format=file_type,
            options=read_options
        )
        
        logger.info(f"Read {df.count()} records from {file_path}")
        return df
    
    @handle_errors("schema_validation", raise_on_error=True)
    def _validate_and_enforce_schema(
        self, 
        df: DataFrame, 
        schema_config: Dict[str, Any]
    ) -> DataFrame:
        """
        Validate and enforce schema on DataFrame.
        
        Args:
            df: DataFrame to validate
            schema_config: Schema configuration
            
        Returns:
            DataFrame with enforced schema
        """
        # Validate schema
        validation_result = self.schema_validator.validate_schema(df, schema_config)
        
        if not validation_result["valid"]:
            if schema_config.get("strict_validation", True):
                raise DataPipelineError(f"Schema validation failed: {validation_result['errors']}")
            else:
                logger.warning(f"Schema validation warnings: {validation_result['warnings']}")
        
        # Enforce schema
        df = self.schema_validator.enforce_schema(df, schema_config)
        
        return df
    
    def _filter_unprocessed_files(
        self, 
        files: List[str], 
        source_config: Dict[str, Any]
    ) -> List[str]:
        """
        Filter out already processed files for incremental loading.
        
        Args:
            files: List of all files
            source_config: Source configuration
            
        Returns:
            List of unprocessed files
        """
        try:
            # This is a simplified implementation
            # In production, you might want to:
            # 1. Maintain a processed files table in the catalog
            # 2. Check file modification timestamps
            # 3. Use watermarks for date-based filtering
            
            tracking_config = source_config.get("file_tracking", {})
            
            if tracking_config.get("method") == "database":
                # Query processed files from tracking table
                tracking_table = tracking_config.get("tracking_table")
                if tracking_table:
                    # Validate and escape table name
                    if not tracking_table or ".." in tracking_table:
                        raise ValueError(f"Invalid tracking table name: {tracking_table}")
                    # Split and escape table name parts
                    parts = tracking_table.split(".")
                    if len(parts) == 3:
                        escaped_table = f"`{parts[0]}`.`{parts[1]}`.`{parts[2]}`"
                    else:
                        escaped_table = f"`{tracking_table}`"
                    processed_files_df = self.spark.sql(f"SELECT file_path FROM {escaped_table}")
                    # Limit collect to avoid OOM on large datasets
                    max_files = 10000  # Reasonable limit for file tracking
                    processed_files = [row.file_path for row in processed_files_df.limit(max_files).collect()]
                    unprocessed = [f for f in files if f not in processed_files]
                    logger.info(f"Filtered to {len(unprocessed)} unprocessed files")
                    return unprocessed
            
            # Default: process all files
            return files
            
        except Exception as e:
            logger.warning(f"Error filtering processed files: {str(e)}")
            return files
    
    @handle_errors("mark_files_processed", raise_on_error=False)
    def _mark_files_processed(
        self, 
        files: List[str], 
        source_config: Dict[str, Any]
    ) -> None:
        """
        Mark files as processed in tracking system.
        
        Args:
            files: List of processed files
            source_config: Source configuration
        """
        tracking_config = source_config.get("file_tracking", {})
        
        if tracking_config.get("method") == "database":
            tracking_table = tracking_config.get("tracking_table")
            if tracking_table:
                # Create DataFrame with processed files
                from pyspark.sql.functions import current_timestamp, lit
                
                files_df = self.spark.createDataFrame(
                    [(f,) for f in files], 
                    ["file_path"]
                )
                files_df = files_df.withColumn("processed_timestamp", current_timestamp())
                files_df = files_df.withColumn("source_name", lit(source_config.get("name")))
                
                # Write to tracking table
                files_df.write.mode("append").saveAsTable(tracking_table)
                
                logger.info(f"Marked {len(files)} files as processed")
    
    def _create_empty_result(
        self, 
        source_config: Dict[str, Any], 
        target_config: Dict[str, Any], 
        start_time: datetime
    ) -> Dict[str, Any]:
        """Create empty result when no files are processed."""
        end_time = datetime.now()
        
        return {
            "status": "success",
            "files_processed": 0,
            "records_processed": 0,
            "message": "No files found to process",
            "metadata": self.get_ingestion_metadata(
                source_config, target_config, 0, start_time, end_time
            )
        }