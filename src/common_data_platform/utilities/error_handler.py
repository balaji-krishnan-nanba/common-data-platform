"""Error handling utilities for the data ingestion framework."""

import logging
import traceback
from typing import Any, Callable, Optional, Dict
from functools import wraps
from enum import Enum
import time

logger = logging.getLogger(__name__)


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DataPipelineError(Exception):
    """Base exception for data pipeline errors."""
    
    def __init__(
        self, 
        message: str, 
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize pipeline error.
        
        Args:
            message: Error message
            severity: Error severity level
            error_code: Optional error code
            context: Additional error context
        """
        super().__init__(message)
        self.severity = severity
        self.error_code = error_code
        self.context = context or {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary format."""
        return {
            "message": str(self),
            "severity": self.severity.value,
            "error_code": self.error_code,
            "context": self.context,
            "traceback": traceback.format_exc()
        }
        
    def __str__(self):
        """Return formatted error message with context."""
        base_msg = super().__str__()
        if self.error_code:
            base_msg = f"[{self.error_code}] {base_msg}"
        if self.context:
            context_str = "\n".join(f"  {k}: {v}" for k, v in self.context.items())
            base_msg = f"{base_msg}\nContext:\n{context_str}"
        return base_msg


class ConnectionError(DataPipelineError):
    """Error connecting to data source."""
    pass


class SchemaValidationError(DataPipelineError):
    """Error validating data schema."""
    pass


class DataQualityError(DataPipelineError):
    """Error in data quality validation."""
    pass


class TransformationError(DataPipelineError):
    """Error during data transformation."""
    pass


class ConfigurationError(DataPipelineError):
    """Error in configuration."""
    
    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        """Initialize configuration error with helpful context."""
        # Add helpful suggestions based on common issues
        if "table name" in message.lower():
            message += "\nTip: Ensure your table name follows the pattern: catalog.schema.table"
        elif "storage" in message.lower():
            message += "\nTip: Check that your storage path is accessible and properly formatted"
        elif "missing" in message.lower():
            message += "\nTip: Check the documentation for required configuration fields"
            
        super().__init__(message, ErrorSeverity.HIGH, "CONFIG_ERROR", context)


class SecurityError(DataPipelineError):
    """Security related errors (SQL injection, unauthorized access, etc.)."""
    
    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        """Initialize security error as critical."""
        super().__init__(message, ErrorSeverity.CRITICAL, "SECURITY_ERROR", context)


def retry_on_failure(
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
    exponential_backoff: bool = True,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying function calls on failure.
    
    Args:
        max_attempts: Maximum number of retry attempts
        backoff_seconds: Initial backoff time
        exponential_backoff: Whether to use exponential backoff
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts - 1:
                        # Last attempt, don't wait
                        break
                    
                    # Calculate wait time
                    if exponential_backoff:
                        wait_time = backoff_seconds * (2 ** attempt)
                    else:
                        wait_time = backoff_seconds
                    
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {wait_time} seconds..."
                    )
                    
                    time.sleep(wait_time)
            
            # All attempts failed
            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
            raise last_exception
            
        return wrapper
    return decorator


def handle_errors(
    error_policy: str = "raise",
    default_return: Any = None,
    log_errors: bool = True
):
    """
    Decorator for handling errors in functions.
    
    Args:
        error_policy: How to handle errors ('raise', 'return_default', 'log_and_continue')
        default_return: Default value to return on error
        log_errors: Whether to log errors
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_errors:
                    logger.error(
                        f"Error in {func.__name__}: {str(e)}",
                        exc_info=True
                    )
                
                if error_policy == "raise":
                    raise
                elif error_policy == "return_default":
                    return default_return
                elif error_policy == "log_and_continue":
                    return None
                else:
                    raise ValueError(f"Unknown error policy: {error_policy}")
                    
        return wrapper
    return decorator


class ErrorCollector:
    """Collects errors during pipeline execution."""
    
    def __init__(self):
        """Initialize error collector."""
        self.errors = []
        self.warnings = []
        
    def add_error(
        self, 
        error: Exception, 
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Add an error to the collection.
        
        Args:
            error: Exception that occurred
            context: Additional context information
        """
        error_info = {
            "timestamp": time.time(),
            "error_type": type(error).__name__,
            "message": str(error),
            "context": context or {},
            "traceback": traceback.format_exc()
        }
        
        if isinstance(error, DataPipelineError):
            error_info.update(error.to_dict())
            
        self.errors.append(error_info)
        
    def add_warning(
        self, 
        message: str, 
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Add a warning to the collection.
        
        Args:
            message: Warning message
            context: Additional context information
        """
        warning_info = {
            "timestamp": time.time(),
            "message": message,
            "context": context or {}
        }
        
        self.warnings.append(warning_info)
        
    def has_errors(self) -> bool:
        """Check if any errors were collected."""
        return len(self.errors) > 0
        
    def has_warnings(self) -> bool:
        """Check if any warnings were collected."""
        return len(self.warnings) > 0
        
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of collected errors and warnings."""
        return {
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": self.errors,
            "warnings": self.warnings
        }
        
    def raise_if_errors(self, message: str = "Pipeline execution failed") -> None:
        """Raise exception if any errors were collected."""
        if self.has_errors():
            error_details = {
                "error_count": len(self.errors),
                "errors": self.errors
            }
            raise DataPipelineError(
                message,
                severity=ErrorSeverity.HIGH,
                context=error_details
            )
            
    def clear(self) -> None:
        """Clear all collected errors and warnings."""
        self.errors.clear()
        self.warnings.clear()


def safe_execute(
    func: Callable,
    error_collector: Optional[ErrorCollector] = None,
    context: Optional[Dict[str, Any]] = None,
    reraise: bool = True
) -> Any:
    """
    Safely execute a function with error collection.
    
    Args:
        func: Function to execute
        error_collector: Optional error collector
        context: Additional context for errors
        reraise: Whether to reraise exceptions
        
    Returns:
        Function result or None if error occurred
    """
    try:
        return func()
    except Exception as e:
        if error_collector:
            error_collector.add_error(e, context)
        
        logger.error(f"Error executing {func.__name__}: {str(e)}", exc_info=True)
        
        if reraise:
            raise
        return None


class ErrorHandler:
    """Central error handling for the Data Platform."""
    
    def __init__(self, raise_on_error: bool = True):
        """Initialize error handler.
        
        Args:
            raise_on_error: Whether to raise exceptions or just log them
        """
        self.raise_on_error = raise_on_error
        self.error_collector = ErrorCollector()
        
    def handle_error(self, error: Exception, 
                    severity: ErrorSeverity = ErrorSeverity.HIGH,
                    context: Optional[Dict[str, Any]] = None) -> bool:
        """Handle an error with appropriate logging and actions.
        
        Args:
            error: The original exception
            severity: Error severity level
            context: Additional context information
            
        Returns:
            bool: False always (for convenience in error flows)
            
        Raises:
            DataPipelineError: If raise_on_error is True
        """
        # Add to error collector
        self.error_collector.add_error(error, context)
        
        # Log based on severity
        if severity == ErrorSeverity.CRITICAL:
            logger.critical(f"CRITICAL ERROR: {error}", exc_info=True)
        elif severity == ErrorSeverity.HIGH:
            logger.error(f"ERROR: {error}", exc_info=True)
        elif severity == ErrorSeverity.MEDIUM:
            logger.warning(f"WARNING: {error}", exc_info=True)
        else:
            logger.info(f"INFO: {error}", exc_info=True)
            
        # Raise if configured
        if self.raise_on_error:
            if isinstance(error, DataPipelineError):
                raise error
            else:
                raise DataPipelineError(
                    str(error),
                    severity=severity,
                    context=context
                )
            
        return False