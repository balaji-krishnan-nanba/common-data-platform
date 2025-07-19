"""Base connector class for all data source connectors."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
import logging

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """Abstract base class for all data source connectors."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize base connector.
        
        Args:
            spark: Active Spark session
            config: Connector configuration
        """
        self.spark = spark
        self.config = config
        self._validate_config()
        
    @abstractmethod
    def _validate_config(self) -> None:
        """Validate connector configuration."""
        pass
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to data source."""
        pass
    
    @abstractmethod
    def read(self, **kwargs) -> DataFrame:
        """
        Read data from source.
        
        Returns:
            Spark DataFrame containing the data
        """
        pass
    
    @abstractmethod
    def write(self, df: DataFrame, **kwargs) -> None:
        """
        Write data to destination.
        
        Args:
            df: DataFrame to write
        """
        pass
    
    def test_connection(self) -> bool:
        """
        Test connection to data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connect()
            logger.info(f"Successfully connected to {self.__class__.__name__}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to {self.__class__.__name__}: {str(e)}")
            return False
    
    def get_connection_string(self) -> str:
        """
        Get connection string for the data source.
        
        Returns:
            Connection string
        """
        return ""
    
    def close(self) -> None:
        """Close connection to data source."""
        pass