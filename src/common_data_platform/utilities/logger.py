"""Logging configuration for the data ingestion framework."""

import logging
import logging.config
import os
from typing import Optional, Dict, Any
from datetime import datetime
import json


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    structured: bool = True
) -> None:
    """
    Setup logging configuration for the framework.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        structured: Whether to use structured logging format
    """
    
    # Log format for structured logging
    structured_format = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "structured": {
                "format": "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            },
            "simple": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "structured" if structured else "simple",
                "stream": "ext://sys.stdout"
            }
        },
        "loggers": {
            "": {  # Root logger
                "level": log_level,
                "handlers": ["console"],
                "propagate": False
            }
        }
    }
    
    # Add file handler if log file specified
    if log_file:
        # Ensure log directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        structured_format["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": log_level,
            "formatter": "structured" if structured else "simple",
            "filename": log_file,
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5
        }
        
        structured_format["loggers"][""]["handlers"].append("file")
    
    # Apply logging configuration
    logging.config.dictConfig(structured_format)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class DeltaTableLogHandler:
    """Custom handler to write logs directly to Delta tables."""
    
    def __init__(self, spark, config_manager):
        """
        Initialize Delta table log handler.
        
        Args:
            spark: Spark session
            config_manager: Configuration manager instance
        """
        self.spark = spark
        self.config_manager = config_manager
        self.environment = config_manager.environment
        self.bronze_catalog = config_manager.get_catalog_name("bronze")
        
    def write_pipeline_log(self, log_data: Dict[str, Any]) -> None:
        """
        Write pipeline execution log to Delta table.
        
        Args:
            log_data: Pipeline log data
        """
        try:
            # Create DataFrame from log data
            df = self.spark.createDataFrame([log_data])
            
            # Write to pipeline_executions table
            table_name = f"`{self.bronze_catalog}`.`system`.`pipeline_executions`"
            df.write.mode("append").saveAsTable(table_name)
            
        except Exception as e:
            # Fallback to standard logging if Delta write fails
            logging.getLogger(__name__).warning(
                f"Failed to write pipeline log to Delta table: {str(e)}"
            )
    
    def write_stage_log(self, log_data: Dict[str, Any]) -> None:
        """
        Write stage execution log to Delta table.
        
        Args:
            log_data: Stage log data
        """
        try:
            # Create DataFrame from log data
            df = self.spark.createDataFrame([log_data])
            
            # Write to stage_executions table
            table_name = f"`{self.bronze_catalog}`.`system`.`stage_executions`"
            df.write.mode("append").saveAsTable(table_name)
            
        except Exception as e:
            # Fallback to standard logging if Delta write fails
            logging.getLogger(__name__).warning(
                f"Failed to write stage log to Delta table: {str(e)}"
            )


class DataPipelineLogger:
    """Enhanced logger for data pipeline operations with Delta table integration."""
    
    def __init__(self, name: str, spark=None, config_manager=None):
        """
        Initialize pipeline logger.
        
        Args:
            name: Logger name
            spark: Optional Spark session for Delta table logging
            config_manager: Optional configuration manager for Delta table logging
        """
        self.logger = logging.getLogger(name)
        self.pipeline_id = None
        self.start_time = None
        self.environment = os.getenv("ENVIRONMENT", "dev")
        
        # Initialize Delta table handler if Spark is available
        self.delta_handler = None
        if spark and config_manager:
            try:
                self.delta_handler = DeltaTableLogHandler(spark, config_manager)
            except Exception as e:
                self.logger.warning(f"Delta table logging not available: {str(e)}")
        
    def start_pipeline(self, pipeline_id: str, pipeline_name: str = None, source_name: str = None) -> None:
        """
        Start pipeline logging session.
        
        Args:
            pipeline_id: Unique pipeline identifier
            pipeline_name: Human-readable pipeline name
            source_name: Source being processed
        """
        self.pipeline_id = pipeline_id
        self.start_time = datetime.now()
        
        # Standard logging
        self.logger.info(
            f"Pipeline started: {pipeline_name or pipeline_id}",
            extra={
                "pipeline_id": self.pipeline_id,
                "start_time": self.start_time.isoformat(),
                "event_type": "pipeline_start"
            }
        )
        
        # Delta table logging
        if self.delta_handler:
            log_data = {
                "pipeline_id": self.pipeline_id,
                "pipeline_name": pipeline_name or pipeline_id,
                "status": "running",
                "start_time": self.start_time,
                "end_time": None,
                "duration_seconds": None,
                "records_processed": None,
                "files_processed": None,
                "error_message": None,
                "source_name": source_name,
                "target_layer": None,
                "batch_id": None,
                "environment": self.environment,
                "user_name": os.getenv("USER"),
                "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID")
            }
            self.delta_handler.write_pipeline_log(log_data)
    
    def end_pipeline(self, status: str = "success", error: Optional[str] = None, 
                    records_processed: Optional[int] = None, files_processed: Optional[int] = None) -> None:
        """
        End pipeline logging session.
        
        Args:
            status: Pipeline status (success, failure, failed)
            error: Error message if status is failure
            records_processed: Total records processed
            files_processed: Total files processed
        """
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds() if self.start_time else 0
        
        # Standard logging
        log_data = {
            "pipeline_id": self.pipeline_id,
            "status": status,
            "duration_seconds": duration,
            "end_time": end_time.isoformat(),
            "event_type": "pipeline_end"
        }
        
        if error:
            log_data["error"] = error
            
        if status == "success":
            self.logger.info("Pipeline completed successfully", extra=log_data)
        else:
            self.logger.error("Pipeline failed", extra=log_data)
        
        # Delta table logging (update the existing record)
        if self.delta_handler:
            delta_log_data = {
                "pipeline_id": self.pipeline_id,
                "pipeline_name": self.pipeline_id,  # Will be updated if we have the name
                "status": status,
                "start_time": self.start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "records_processed": records_processed,
                "files_processed": files_processed,
                "error_message": error,
                "source_name": None,
                "target_layer": None,
                "batch_id": None,
                "environment": self.environment,
                "user_name": os.getenv("USER"),
                "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID")
            }
            self.delta_handler.write_pipeline_log(delta_log_data)
    
    def log_stage(
        self, 
        stage: str, 
        status: str, 
        records_processed: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        error_message: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        Log pipeline stage information.
        
        Args:
            stage: Stage name
            status: Stage status (running, success, failed)
            records_processed: Number of records processed
            start_time: Stage start time
            end_time: Stage end time
            error_message: Error message if status is failed
            **kwargs: Additional stage-specific data
        """
        current_time = datetime.now()
        if not start_time:
            start_time = current_time
        if not end_time and status in ["success", "failed"]:
            end_time = current_time
            
        duration = None
        if start_time and end_time:
            duration = (end_time - start_time).total_seconds()
        
        # Standard logging
        log_data = {
            "pipeline_id": self.pipeline_id,
            "stage": stage,
            "status": status,
            "event_type": "stage_update"
        }
        
        if records_processed is not None:
            log_data["records_processed"] = records_processed
        if error_message:
            log_data["error"] = error_message
            
        log_data.update(kwargs)
        
        if status == "success":
            self.logger.info(f"Stage {stage} completed", extra=log_data)
        elif status == "failed":
            self.logger.error(f"Stage {stage} failed", extra=log_data)
        else:
            self.logger.info(f"Stage {stage} {status}", extra=log_data)
        
        # Delta table logging
        if self.delta_handler:
            stage_details = {k: str(v) for k, v in kwargs.items()}
            delta_log_data = {
                "pipeline_id": self.pipeline_id,
                "stage_name": stage,
                "status": status,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "records_processed": records_processed,
                "stage_details": json.dumps(stage_details) if stage_details else None,
                "error_message": error_message,
                "environment": self.environment
            }
            self.delta_handler.write_stage_log(delta_log_data)
    
    def log_data_quality(
        self, 
        table: str, 
        checks: dict, 
        passed: bool,
        records_checked: Optional[int] = None,
        records_failed: Optional[int] = None
    ) -> None:
        """
        Log data quality check results.
        
        Args:
            table: Table name
            checks: Data quality check results
            passed: Whether all checks passed
            records_checked: Total records checked
            records_failed: Number of records that failed checks
        """
        check_timestamp = datetime.now()
        
        # Standard logging
        log_data = {
            "pipeline_id": self.pipeline_id,
            "table": table,
            "data_quality_checks": checks,
            "all_checks_passed": passed,
            "event_type": "data_quality"
        }
        
        if passed:
            self.logger.info(f"Data quality checks passed for {table}", extra=log_data)
        else:
            self.logger.warning(f"Data quality checks failed for {table}", extra=log_data)
        
        # Delta table logging for each check
        if self.delta_handler:
            for check_name, check_result in checks.items():
                status = "passed" if check_result.get("passed", False) else "failed"
                delta_log_data = {
                    "check_id": f"{self.pipeline_id}_{table}_{check_name}_{int(check_timestamp.timestamp())}",
                    "table_name": table,
                    "check_type": check_result.get("type", "unknown"),
                    "check_name": check_name,
                    "status": status,
                    "result_details": json.dumps(check_result),
                    "records_checked": records_checked,
                    "records_failed": records_failed if not check_result.get("passed", False) else 0,
                    "check_timestamp": check_timestamp,
                    "batch_id": None,
                    "environment": self.environment
                }
                
                try:
                    # Create DataFrame from log data
                    df = self.delta_handler.spark.createDataFrame([delta_log_data])
                    
                    # Write to data_quality_results table
                    table_name = f"`{self.delta_handler.bronze_catalog}`.`system`.`data_quality_results`"
                    df.write.mode("append").saveAsTable(table_name)
                    
                except Exception as e:
                    # Fallback to standard logging if Delta write fails
                    self.logger.warning(
                        f"Failed to write data quality log to Delta table: {str(e)}"
                    )
    
    def start_operation(self, operation_name: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Start an operation and return an operation ID.
        
        Args:
            operation_name: Name of the operation
            context: Optional context information
            
        Returns:
            str: Operation ID
        """
        operation_id = f"{operation_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(
            f"Operation started: {operation_name}",
            extra={
                "operation_id": operation_id,
                "operation_name": operation_name,
                "context": context or {}
            }
        )
        return operation_id
    
    def end_operation(self, operation_id: str, status: str = "success", 
                     metrics: Optional[Dict[str, Any]] = None, error: Optional[str] = None) -> None:
        """
        End an operation.
        
        Args:
            operation_id: Operation ID
            status: Operation status
            metrics: Optional metrics
            error: Optional error message
        """
        log_level = logging.INFO if status == "success" else logging.ERROR
        self.logger.log(
            log_level,
            f"Operation ended: {operation_id} - Status: {status}",
            extra={
                "operation_id": operation_id,
                "status": status,
                "metrics": metrics or {},
                "error": error
            }
        )
    
    def log_info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self.logger.info(message, extra=kwargs)
    
    def log_error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self.logger.error(message, extra=kwargs)
    
    def log_warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self.logger.warning(message, extra=kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """Alias for log_info."""
        self.log_info(message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Alias for log_error."""
        self.log_error(message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Alias for log_warning."""
        self.log_warning(message, **kwargs)