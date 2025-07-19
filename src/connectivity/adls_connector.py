"""ADLS Gen2 connector for reading files from Azure Data Lake Storage."""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
import logging
from pathlib import Path

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class ADLSConnector(BaseConnector):
    """Connector for Azure Data Lake Storage Gen2."""
    
    def __init__(self, spark, config: Dict[str, Any], secret_manager):
        """
        Initialize ADLS connector.
        
        Args:
            spark: Active Spark session
            config: ADLS configuration
            secret_manager: Secret manager for credentials
        """
        self.secret_manager = secret_manager
        super().__init__(spark, config)
        
    def _validate_config(self) -> None:
        """Validate ADLS configuration."""
        required_fields = ['storage_account', 'container']
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required configuration field: {field}")
    
    def connect(self) -> None:
        """Configure Spark session for ADLS access."""
        try:
            # Configure Spark with storage credentials
            self.secret_manager.configure_spark_for_storage(self.config)
            logger.info(f"Successfully configured ADLS access for {self.config['storage_account']}")
            
        except Exception as e:
            logger.error(f"Failed to configure ADLS access: {str(e)}")
            raise
    
    def read(
        self, 
        path: str, 
        file_format: str = "parquet",
        options: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read data from ADLS.
        
        Args:
            path: File path within the container
            file_format: File format (parquet, csv, json, excel, etc.)
            options: Format-specific options
            
        Returns:
            Spark DataFrame
        """
        try:
            full_path = self._build_full_path(path)
            logger.info(f"Reading {file_format} from {full_path}")
            
            # Default options
            if options is None:
                options = {}
            
            # Read based on file format
            if file_format.lower() == "csv":
                return self._read_csv(full_path, options)
            elif file_format.lower() == "excel":
                return self._read_excel(full_path, options)
            elif file_format.lower() == "json":
                return self._read_json(full_path, options)
            elif file_format.lower() == "parquet":
                return self._read_parquet(full_path, options)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
        except Exception as e:
            logger.error(f"Error reading from ADLS: {str(e)}")
            raise
    
    def write(
        self, 
        df: DataFrame, 
        path: str,
        file_format: str = "delta",
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> None:
        """
        Write DataFrame to ADLS.
        
        Args:
            df: DataFrame to write
            path: Destination path
            file_format: Output format
            mode: Write mode
            options: Format-specific options
        """
        try:
            full_path = self._build_full_path(path)
            logger.info(f"Writing {file_format} to {full_path}")
            
            if options is None:
                options = {}
            
            writer = df.write.mode(mode)
            
            # Apply options
            for key, value in options.items():
                writer = writer.option(key, value)
            
            # Write based on format
            if file_format.lower() == "delta":
                writer.format("delta").save(full_path)
            elif file_format.lower() == "parquet":
                writer.parquet(full_path)
            elif file_format.lower() == "csv":
                writer.csv(full_path)
            elif file_format.lower() == "json":
                writer.json(full_path)
            else:
                raise ValueError(f"Unsupported output format: {file_format}")
                
            logger.info(f"Successfully wrote data to {full_path}")
            
        except Exception as e:
            logger.error(f"Error writing to ADLS: {str(e)}")
            raise
    
    def list_files(self, path: str, pattern: Optional[str] = None) -> List[str]:
        """
        List files in ADLS path.
        
        Args:
            path: Directory path
            pattern: Optional file pattern filter
            
        Returns:
            List of file paths
        """
        try:
            full_path = self._build_full_path(path)
            
            # Use dbutils to list files
            if hasattr(self.spark, '_jvm'):
                dbutils = self.spark._jvm.com.databricks.service.DBUtils.get(self.spark._jsc)
                files = dbutils.fs.ls(full_path)
                file_paths = [f.path for f in files if not f.isDir()]
                
                # Apply pattern filter if provided
                if pattern:
                    import fnmatch
                    file_paths = [f for f in file_paths if fnmatch.fnmatch(f, pattern)]
                
                return file_paths
            else:
                logger.warning("dbutils not available - returning empty list")
                return []
                
        except Exception as e:
            logger.error(f"Error listing files from {path}: {str(e)}")
            return []
    
    def _build_full_path(self, path: str) -> str:
        """
        Build full ABFSS path.
        
        Args:
            path: Relative path within container
            
        Returns:
            Full ABFSS path
        """
        storage_account = self.config['storage_account']
        container = self.config['container']
        
        # Clean up path
        path = path.lstrip('/')
        
        return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"
    
    def _read_csv(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read CSV files."""
        default_options = {
            "header": "true",
            "inferSchema": "true",
            "delimiter": ",",
            "quote": '"',
            "escape": '"'
        }
        default_options.update(options)
        
        reader = self.spark.read.format("csv")
        for key, value in default_options.items():
            reader = reader.option(key, value)
        
        return reader.load(path)
    
    def _read_excel(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read Excel files using com.crealytics.spark.excel."""
        default_options = {
            "header": "true",
            "inferSchema": "true",
            "treatEmptyValuesAsNulls": "true"
        }
        default_options.update(options)
        
        reader = self.spark.read.format("com.crealytics.spark.excel")
        for key, value in default_options.items():
            reader = reader.option(key, value)
        
        return reader.load(path)
    
    def _read_json(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read JSON files."""
        default_options = {
            "multiLine": "true"
        }
        default_options.update(options)
        
        reader = self.spark.read.format("json")
        for key, value in default_options.items():
            reader = reader.option(key, value)
        
        return reader.load(path)
    
    def _read_parquet(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read Parquet files."""
        reader = self.spark.read.format("parquet")
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load(path)