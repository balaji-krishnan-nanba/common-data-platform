"""Simplified decorators for common patterns in the data platform."""

import logging
import time
from functools import wraps
from typing import Any, Callable, Optional

from .error_handler import DataPipelineError, ErrorSeverity

logger = logging.getLogger(__name__)


def handle_errors(operation_name: str, 
                 raise_on_error: bool = True,
                 default_return: Any = None):
    """
    Simplified error handling decorator that replaces repetitive try-except blocks.
    
    Args:
        operation_name: Name of the operation for logging
        raise_on_error: Whether to raise exceptions or return default
        default_return: Value to return if error occurs and raise_on_error is False
        
    Usage:
        @handle_errors("data_ingestion")
        def ingest_data():
            # Your code here
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                logger.info(f"Starting {operation_name}")
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(f"Completed {operation_name} in {duration:.2f}s")
                return result
            except DataPipelineError:
                # Re-raise our own exceptions
                raise
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Error in {operation_name} after {duration:.2f}s: {str(e)}")
                
                if raise_on_error:
                    raise DataPipelineError(
                        f"{operation_name} failed: {str(e)}",
                        severity=ErrorSeverity.HIGH,
                        context={
                            "operation": operation_name,
                            "function": func.__name__,
                            "error_type": type(e).__name__
                        }
                    )
                else:
                    return default_return
                    
        return wrapper
    return decorator


def with_retry(max_attempts: int = 3, 
               backoff_seconds: float = 1.0,
               exponential: bool = True):
    """
    Simplified retry decorator for transient failures.
    
    Args:
        max_attempts: Maximum number of retry attempts
        backoff_seconds: Initial wait time between retries
        exponential: Whether to use exponential backoff
        
    Usage:
        @with_retry(max_attempts=3, backoff_seconds=2)
        def connect_to_database():
            # Your connection code here
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt < max_attempts - 1:
                        wait_time = backoff_seconds * (2 ** attempt if exponential else 1)
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}: "
                            f"{str(e)}. Retrying in {wait_time}s..."
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
            
            raise last_exception
                        
        return wrapper
    return decorator


def validate_config(required_fields: list):
    """
    Simplified configuration validation decorator.
    
    Args:
        required_fields: List of required field names
        
    Usage:
        @validate_config(['source', 'target', 'connection'])
        def process_config(config: dict):
            # Your code here
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, config: dict, *args, **kwargs):
            # Validate required fields
            missing_fields = [field for field in required_fields if field not in config]
            if missing_fields:
                raise DataPipelineError(
                    f"Missing required configuration fields: {missing_fields}",
                    severity=ErrorSeverity.HIGH,
                    error_code="CONFIG_VALIDATION_ERROR",
                    context={"missing_fields": missing_fields}
                )
            
            return func(self, config, *args, **kwargs)
            
        return wrapper
    return decorator


def log_performance(operation_type: str = "operation"):
    """
    Simplified performance logging decorator.
    
    Args:
        operation_type: Type of operation for logging context
        
    Usage:
        @log_performance("transformation")
        def transform_data(df):
            # Your transformation code
            return df
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            start_memory = 0  # Would need psutil for actual memory tracking
            
            try:
                result = func(*args, **kwargs)
                
                duration = time.time() - start_time
                logger.info(
                    f"{operation_type} '{func.__name__}' completed",
                    extra={
                        "duration_seconds": duration,
                        "operation_type": operation_type,
                        "function": func.__name__
                    }
                )
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    f"{operation_type} '{func.__name__}' failed after {duration:.2f}s",
                    extra={
                        "duration_seconds": duration,
                        "operation_type": operation_type,
                        "function": func.__name__,
                        "error": str(e)
                    }
                )
                raise
                
        return wrapper
    return decorator