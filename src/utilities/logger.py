"""Logging configuration for the data ingestion framework."""

import logging
import logging.config
import os
from typing import Optional
from datetime import datetime


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


class DataPipelineLogger:
    """Enhanced logger for data pipeline operations."""
    
    def __init__(self, name: str):
        """
        Initialize pipeline logger.
        
        Args:
            name: Logger name
        """
        self.logger = logging.getLogger(name)
        self.pipeline_id = None
        self.start_time = None
        
    def start_pipeline(self, pipeline_id: str) -> None:
        """
        Start pipeline logging session.
        
        Args:
            pipeline_id: Unique pipeline identifier
        """
        self.pipeline_id = pipeline_id
        self.start_time = datetime.now()
        
        self.logger.info(
            f"Pipeline started",
            extra={
                "pipeline_id": self.pipeline_id,
                "start_time": self.start_time.isoformat(),
                "event_type": "pipeline_start"
            }
        )
    
    def end_pipeline(self, status: str = "success", error: Optional[str] = None) -> None:
        """
        End pipeline logging session.
        
        Args:
            status: Pipeline status (success, failure)
            error: Error message if status is failure
        """
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds() if self.start_time else 0
        
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
    
    def log_stage(
        self, 
        stage: str, 
        status: str, 
        records_processed: Optional[int] = None,
        **kwargs
    ) -> None:
        """
        Log pipeline stage information.
        
        Args:
            stage: Stage name
            status: Stage status
            records_processed: Number of records processed
            **kwargs: Additional stage-specific data
        """
        log_data = {
            "pipeline_id": self.pipeline_id,
            "stage": stage,
            "status": status,
            "event_type": "stage_update"
        }
        
        if records_processed is not None:
            log_data["records_processed"] = records_processed
            
        log_data.update(kwargs)
        
        if status == "success":
            self.logger.info(f"Stage {stage} completed", extra=log_data)
        elif status == "error":
            self.logger.error(f"Stage {stage} failed", extra=log_data)
        else:
            self.logger.info(f"Stage {stage} {status}", extra=log_data)
    
    def log_data_quality(
        self, 
        table: str, 
        checks: dict, 
        passed: bool
    ) -> None:
        """
        Log data quality check results.
        
        Args:
            table: Table name
            checks: Data quality check results
            passed: Whether all checks passed
        """
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