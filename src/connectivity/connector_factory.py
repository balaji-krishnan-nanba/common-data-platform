"""Factory for creating data source connectors."""

from typing import Dict, Any
from pyspark.sql import SparkSession
import logging

from .base_connector import BaseConnector
from .adls_connector import ADLSConnector
from .oracle_connector import OracleConnector

logger = logging.getLogger(__name__)


class ConnectorFactory:
    """Factory class for creating data source connectors."""
    
    # Registry of available connectors
    _connectors = {
        'adls': ADLSConnector,
        'oracle': OracleConnector,
        'csv': ADLSConnector,  # CSV files use ADLS connector
        'excel': ADLSConnector,  # Excel files use ADLS connector
        'json': ADLSConnector,  # JSON files use ADLS connector
        'parquet': ADLSConnector,  # Parquet files use ADLS connector
    }
    
    @classmethod
    def create_connector(
        cls, 
        connector_type: str, 
        spark: SparkSession, 
        config: Dict[str, Any],
        secret_manager
    ) -> BaseConnector:
        """
        Create a connector instance based on type.
        
        Args:
            connector_type: Type of connector to create
            spark: Active Spark session
            config: Connector configuration
            secret_manager: Secret manager instance
            
        Returns:
            Configured connector instance
        """
        try:
            connector_type = connector_type.lower()
            
            if connector_type not in cls._connectors:
                raise ValueError(f"Unsupported connector type: {connector_type}")
            
            connector_class = cls._connectors[connector_type]
            
            logger.info(f"Creating {connector_type} connector")
            
            # Create connector instance
            connector = connector_class(spark, config, secret_manager)
            
            # Test connection
            if connector.test_connection():
                logger.info(f"Successfully created and tested {connector_type} connector")
                return connector
            else:
                raise Exception(f"Connection test failed for {connector_type}")
                
        except Exception as e:
            logger.error(f"Failed to create {connector_type} connector: {str(e)}")
            raise
    
    @classmethod
    def register_connector(cls, connector_type: str, connector_class: type) -> None:
        """
        Register a new connector type.
        
        Args:
            connector_type: Name of the connector type
            connector_class: Connector class to register
        """
        if not issubclass(connector_class, BaseConnector):
            raise ValueError("Connector class must inherit from BaseConnector")
        
        cls._connectors[connector_type.lower()] = connector_class
        logger.info(f"Registered new connector type: {connector_type}")
    
    @classmethod
    def get_supported_types(cls) -> list:
        """
        Get list of supported connector types.
        
        Returns:
            List of supported connector type names
        """
        return list(cls._connectors.keys())
    
    @classmethod
    def create_file_connector(
        cls,
        file_type: str,
        spark: SparkSession,
        storage_config: Dict[str, Any],
        secret_manager
    ) -> ADLSConnector:
        """
        Create a file-based connector (convenience method).
        
        Args:
            file_type: Type of file (csv, excel, json, parquet)
            spark: Active Spark session
            storage_config: Storage configuration
            secret_manager: Secret manager instance
            
        Returns:
            ADLS connector configured for file access
        """
        return cls.create_connector('adls', spark, storage_config, secret_manager)
    
    @classmethod
    def create_database_connector(
        cls,
        db_type: str,
        spark: SparkSession,
        db_config: Dict[str, Any],
        secret_manager
    ) -> BaseConnector:
        """
        Create a database connector (convenience method).
        
        Args:
            db_type: Type of database (oracle, postgres, sqlserver, etc.)
            spark: Active Spark session
            db_config: Database configuration
            secret_manager: Secret manager instance
            
        Returns:
            Database connector instance
        """
        return cls.create_connector(db_type, spark, db_config, secret_manager)