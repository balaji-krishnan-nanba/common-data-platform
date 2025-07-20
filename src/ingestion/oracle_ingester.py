"""Oracle database ingester for Common Data Platform."""

from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
from pyspark.sql import SparkSession, DataFrame

from .base_ingester import BaseIngester
from ..connectivity.database_service import DatabaseService
from ..utils.logger import PipelineLogger
from ..utils.error_handler import ErrorHandler


class OracleIngester(BaseIngester):
    """Ingester for Oracle database sources."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """Initialize Oracle ingester.
        
        Args:
            spark: Active Spark session
            config: Configuration dictionary
        """
        super().__init__(spark, config)
        self.logger = PipelineLogger(__name__)
        self.error_handler = ErrorHandler()
        
    def ingest(self, source_config: Dict[str, Any], 
              target_config: Dict[str, Any]) -> Dict[str, Any]:
        """Ingest data from Oracle database.
        
        Args:
            source_config: Source configuration
            target_config: Target configuration
            
        Returns:
            Dict containing ingestion results
        """
        operation_id = self.logger.start_operation(
            "oracle_ingestion",
            {"source": source_config.get("name"), 
             "target": target_config.get("table")}
        )
        
        start_time = datetime.now()
        
        try:
            # Create database service
            db_service = DatabaseService(source_config['connection'])
            
            # Get ingestion configuration
            ingestion_config = source_config.get('ingestion', {})
            mode = ingestion_config.get('mode', 'full')
            
            # Build query based on mode
            if mode == 'full':
                df = self._full_load(db_service, source_config)
            elif mode == 'incremental':
                df = self._incremental_load(db_service, source_config, target_config)
            elif mode == 'cdc':
                df = self._cdc_load(db_service, source_config, target_config)
            else:
                raise ValueError(f"Unsupported ingestion mode: {mode}")
            
            # Add metadata columns
            df = self.add_metadata_columns(df, source_config.get("name", "oracle"))
            
            # Write to target
            self.write_to_target(df, target_config)
            
            # Get record count
            count = df.count()
            
            end_time = datetime.now()
            
            result = {
                "status": "success",
                "records_processed": count,
                "metadata": self.get_ingestion_metadata(
                    source_config, target_config, count, start_time, end_time
                )
            }
            
            self.logger.end_operation(operation_id, status="success", 
                                    metrics={"records": count})
            
            return result
            
        except Exception as e:
            self.logger.log_error(f"Oracle ingestion failed: {str(e)}")
            self.logger.end_operation(operation_id, status="failed", error=str(e))
            
            return {
                "status": "failed",
                "error": str(e),
                "metadata": self.get_ingestion_metadata(
                    source_config, target_config, 0, start_time, datetime.now()
                )
            }
            
    def _full_load(self, db_service: DatabaseService, 
                   source_config: Dict[str, Any]) -> DataFrame:
        """Perform full load from Oracle table.
        
        Args:
            db_service: Database service instance
            source_config: Source configuration
            
        Returns:
            DataFrame with loaded data
        """
        table_name = source_config['connection']['table']
        schema_name = source_config['connection'].get('schema')
        
        if schema_name:
            full_table_name = f"{schema_name}.{table_name}"
        else:
            full_table_name = table_name
            
        self.logger.log_info(f"Performing full load from {full_table_name}")
        
        # Use Spark JDBC reader
        jdbc_options = {
            "url": db_service.get_jdbc_url(),
            "dbtable": full_table_name,
            "user": db_service.config['user'],
            "password": db_service.config['password'],
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
        
        # Add optional configurations
        if 'fetch_size' in source_config.get('ingestion', {}):
            jdbc_options['fetchsize'] = str(source_config['ingestion']['fetch_size'])
            
        if 'partition_column' in source_config.get('ingestion', {}):
            partition_config = source_config['ingestion']
            jdbc_options.update({
                'partitionColumn': partition_config['partition_column'],
                'lowerBound': str(partition_config.get('lower_bound', 0)),
                'upperBound': str(partition_config.get('upper_bound', 1000000)),
                'numPartitions': str(partition_config.get('num_partitions', 10))
            })
            
        return self.spark.read.format("jdbc").options(**jdbc_options).load()
        
    def _incremental_load(self, db_service: DatabaseService,
                         source_config: Dict[str, Any],
                         target_config: Dict[str, Any]) -> DataFrame:
        """Perform incremental load based on watermark.
        
        Args:
            db_service: Database service instance
            source_config: Source configuration
            target_config: Target configuration
            
        Returns:
            DataFrame with incremental data
        """
        table_name = source_config['connection']['table']
        schema_name = source_config['connection'].get('schema')
        watermark_column = source_config['ingestion'].get('watermark_column')
        
        if not watermark_column:
            raise ValueError("watermark_column required for incremental mode")
            
        if schema_name:
            full_table_name = f"{schema_name}.{table_name}"
        else:
            full_table_name = table_name
            
        # Get last watermark value
        last_watermark = self._get_last_watermark(target_config)
        
        # Build query with watermark filter
        if last_watermark:
            query = f"""
            (SELECT * FROM {full_table_name} 
             WHERE {watermark_column} > '{last_watermark}') t
            """
        else:
            # First run - get all data
            query = f"(SELECT * FROM {full_table_name}) t"
            
        self.logger.log_info(f"Incremental load with watermark: {last_watermark}")
        
        jdbc_options = {
            "url": db_service.get_jdbc_url(),
            "dbtable": query,
            "user": db_service.config['user'],
            "password": db_service.config['password'],
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
        
        return self.spark.read.format("jdbc").options(**jdbc_options).load()
        
    def _cdc_load(self, db_service: DatabaseService,
                  source_config: Dict[str, Any],
                  target_config: Dict[str, Any]) -> DataFrame:
        """Perform CDC (Change Data Capture) load.
        
        Args:
            db_service: Database service instance
            source_config: Source configuration  
            target_config: Target configuration
            
        Returns:
            DataFrame with CDC data
        """
        cdc_table = source_config['ingestion'].get('cdc_table')
        if not cdc_table:
            raise ValueError("cdc_table required for CDC mode")
            
        # Get last CDC timestamp
        last_cdc_timestamp = self._get_last_cdc_timestamp(target_config)
        
        # Build CDC query
        query = f"""
        (SELECT * FROM {cdc_table}
         WHERE COMMIT_TIMESTAMP > '{last_cdc_timestamp}'
         ORDER BY COMMIT_TIMESTAMP) t
        """
        
        self.logger.log_info(f"CDC load from timestamp: {last_cdc_timestamp}")
        
        jdbc_options = {
            "url": db_service.get_jdbc_url(),
            "dbtable": query,
            "user": db_service.config['user'], 
            "password": db_service.config['password'],
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
        
        return self.spark.read.format("jdbc").options(**jdbc_options).load()
        
    def _get_last_watermark(self, target_config: Dict[str, Any]) -> Optional[str]:
        """Get last watermark value from target table.
        
        Args:
            target_config: Target configuration
            
        Returns:
            Last watermark value or None
        """
        try:
            target_table = f"{target_config['catalog']}.{target_config['schema']}.{target_config['table']}"
            
            # Check if target table exists
            if not self.spark.catalog.tableExists(target_table):
                return None
                
            # Get max watermark value
            watermark_col = target_config.get('watermark_column', 'etl_loaded_timestamp')
            result = self.spark.sql(f"SELECT MAX({watermark_col}) as max_watermark FROM {target_table}").collect()
            
            if result and result[0]['max_watermark']:
                return str(result[0]['max_watermark'])
                
            return None
            
        except Exception as e:
            self.logger.log_warning(f"Failed to get last watermark: {str(e)}")
            return None
            
    def _get_last_cdc_timestamp(self, target_config: Dict[str, Any]) -> str:
        """Get last CDC timestamp from target.
        
        Args:
            target_config: Target configuration
            
        Returns:
            Last CDC timestamp
        """
        last_timestamp = self._get_last_watermark(target_config)
        if last_timestamp:
            return last_timestamp
            
        # Default to 30 days ago if no previous data
        from datetime import timedelta
        default_date = datetime.now() - timedelta(days=30)
        return default_date.strftime('%Y-%m-%d %H:%M:%S')