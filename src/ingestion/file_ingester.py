"""File ingester for Excel, CSV, and other file-based sources."""

from typing import Dict, Any, List
from pyspark.sql import DataFrame
import logging
from datetime import datetime
import fnmatch

from .base_ingester import BaseIngester
from ..connectivity.adls_service import ADLSService
from ..utilities.schema_validator import SchemaValidator
from ..utilities.error_handler import DataPipelineError

logger = logging.getLogger(__name__)


class FileIngester(BaseIngester):
    """Ingester for file-based data sources (Excel, CSV, JSON, Parquet)."""
    
    def __init__(self, spark, config_manager, secret_manager):
        """Initialize file ingester."""
        super().__init__(spark, config_manager, secret_manager)
        self.schema_validator = SchemaValidator()
    
    def get_required_config_fields(self) -> List[str]:
        """Get required configuration fields for file ingestion."""
        return ["type", "connection", "target"]
    
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
        
        try:
            # Validate configurations
            self.validate_config(source_config)
            self.validate_config(target_config)
            
            logger.info(f"Starting file ingestion for {source_config.get('name', 'unnamed')}")
            
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
            
            # Process files
            all_dataframes = []
            total_records = 0
            
            for file_path in files_to_process:
                logger.info(f"Processing file: {file_path}")
                
                try:
                    # Read file
                    df = self._read_file(adls_service, source_config, file_path)
                    
                    # Validate schema if defined
                    if "schema" in source_config:
                        df = self._validate_and_enforce_schema(df, source_config["schema"])
                    
                    # Add audit columns
                    source_info = {
                        "source_system": source_config.get("name", "file_system"),
                        "source_file": file_path,
                        "batch_id": kwargs.get("batch_id", start_time.strftime("%Y%m%d_%H%M%S"))
                    }
                    df = self.add_audit_columns(df, source_info)
                    
                    all_dataframes.append(df)
                    total_records += df.count()
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {str(e)}")
                    if kwargs.get("fail_on_error", True):
                        raise
                    continue
            
            if not all_dataframes:
                raise DataPipelineError("No files processed successfully")
            
            # Union all DataFrames
            final_df = all_dataframes[0]
            for df in all_dataframes[1:]:
                final_df = final_df.unionByName(df, allowMissingColumns=True)
            
            # Create target table if needed
            self.create_target_table_if_not_exists(target_config, final_df)
            
            # Write to target
            write_mode = kwargs.get("write_mode", "append")
            self.write_to_target(final_df, target_config, write_mode)
            
            # Mark files as processed (if tracking enabled)
            if source_config.get("track_processed_files", True):
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
            
            logger.info(f"File ingestion completed: {result}")
            return result
            
        except Exception as e:
            end_time = datetime.now()
            logger.error(f"File ingestion failed: {str(e)}")
            
            return {
                "status": "failed",
                "error": str(e),
                "metadata": self.get_ingestion_metadata(
                    source_config, target_config, 0, start_time, end_time
                )
            }
    
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
        try:
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
            
        except Exception as e:
            logger.error(f"Error getting files to process: {str(e)}")
            raise
    
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
        try:
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
            
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
    
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
        try:
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
            
        except Exception as e:
            logger.error(f"Schema validation/enforcement error: {str(e)}")
            raise
    
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
                    processed_files_df = self.spark.sql(f"SELECT file_path FROM {tracking_table}")
                    processed_files = [row.file_path for row in processed_files_df.collect()]
                    unprocessed = [f for f in files if f not in processed_files]
                    logger.info(f"Filtered to {len(unprocessed)} unprocessed files")
                    return unprocessed
            
            # Default: process all files
            return files
            
        except Exception as e:
            logger.warning(f"Error filtering processed files: {str(e)}")
            return files
    
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
        try:
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
            
        except Exception as e:
            logger.warning(f"Error marking files as processed: {str(e)}")
    
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