"""Oracle database ingester for Common Data Platform."""

from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
from pyspark.sql import SparkSession, DataFrame

from ..connectivity.database_service import DatabaseService
from ..utilities.error_handler import ErrorHandler
from ..utilities.sql_utils import SparkSQLBuilder
from ..utilities.decorators import handle_errors, with_retry, log_performance
from ..utilities.logger import DataPipelineLogger
from ..utilities.unity_catalog_utils import UnityCatalogValidator

logger = logging.getLogger(__name__)


class OracleIngester:
    """Ingester for Oracle database sources."""
    
    def __init__(self, spark: SparkSession, config_manager, secret_manager):
        """Initialize Oracle ingester.
        
        Args:
            spark: Active Spark session
            config_manager: Configuration manager instance
            secret_manager: Secret manager instance
        """
        self.spark = spark
        self.config_manager = config_manager
        self.secret_manager = secret_manager
        self.error_handler = ErrorHandler()
        self.sql_builder = SparkSQLBuilder()
        self.pipeline_logger = DataPipelineLogger(__name__)
        self.uc_validator = UnityCatalogValidator(spark)
    
    def get_required_config_fields(self) -> List[str]:
        """Get required configuration fields for Oracle ingestion."""
        return ["connection", "target"]
    
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
        
        # Add batch/run identifier
        if "batch_id" in source_info:
            df = df.withColumn("batch_id", lit(source_info["batch_id"]))
        
        return df
    
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
        
    @handle_errors("oracle_ingestion", raise_on_error=False, default_return=None)
    @log_performance("ingestion")
    def ingest(self, source_config: Dict[str, Any], 
              target_config: Dict[str, Any]) -> Dict[str, Any]:
        """Ingest data from Oracle database.
        
        Args:
            source_config: Source configuration
            target_config: Target configuration
            
        Returns:
            Dict containing ingestion results
        """
        start_time = datetime.now()
        
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
        df = self.add_audit_columns(df, {"source_system": source_config.get("name", "oracle")})
        
        # Write to target
        self.write_to_target(df, target_config, write_mode="append")
        
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
        
        return result or {
            "status": "failed",
            "error": "Unknown error",
            "metadata": self.get_ingestion_metadata(
                source_config, target_config, 0, start_time, datetime.now()
            )
        }
            
    @handle_errors("oracle_full_load", raise_on_error=True)
    @with_retry(max_attempts=3, backoff_seconds=5)
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
            
        self.pipeline_logger.log_info(f"Performing full load from {full_table_name}")
        
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
        
    @handle_errors("oracle_incremental_load", raise_on_error=True)
    @with_retry(max_attempts=3, backoff_seconds=5)
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
        
        # Build query with watermark filter using safe SQL builder
        query = self.sql_builder.build_incremental_query(
            table_name=full_table_name,
            watermark_column=watermark_column,
            last_watermark=last_watermark
        )
            
        self.pipeline_logger.log_info(f"Incremental load with watermark: {last_watermark}")
        
        jdbc_options = {
            "url": db_service.get_jdbc_url(),
            "dbtable": query,
            "user": db_service.config['user'],
            "password": db_service.config['password'],
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
        
        return self.spark.read.format("jdbc").options(**jdbc_options).load()
        
    @handle_errors("oracle_cdc_load", raise_on_error=True)
    @with_retry(max_attempts=3, backoff_seconds=5)
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
        
        # Build CDC query using safe SQL builder
        conditions = {'COMMIT_TIMESTAMP': None}  # Will be handled separately
        query = self.sql_builder.build_incremental_query(
            table_name=cdc_table,
            watermark_column='COMMIT_TIMESTAMP',
            last_watermark=last_cdc_timestamp
        )
        # Add ORDER BY manually (safe since column is hardcoded)
        query = query.replace(') AS t', ' ORDER BY COMMIT_TIMESTAMP) AS t')
        
        self.pipeline_logger.log_info(f"CDC load from timestamp: {last_cdc_timestamp}")
        
        jdbc_options = {
            "url": db_service.get_jdbc_url(),
            "dbtable": query,
            "user": db_service.config['user'], 
            "password": db_service.config['password'],
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
        
        return self.spark.read.format("jdbc").options(**jdbc_options).load()
        
    @handle_errors("get_watermark", raise_on_error=False, default_return=None)
    def _get_last_watermark(self, target_config: Dict[str, Any]) -> Optional[str]:
        """Get last watermark value from target table.
        
        Args:
            target_config: Target configuration
            
        Returns:
            Last watermark value or None
        """
        target_table = f"{target_config['catalog']}.{target_config['schema']}.{target_config['table']}"
        
        # Check if target table exists
        if not self.spark.catalog.tableExists(target_table):
            return None
            
        # Get max watermark value with proper column validation
        watermark_col = target_config.get('watermark_column', 'etl_loaded_timestamp')
        # Validate column name to prevent SQL injection
        if not watermark_col.replace('_', '').isalnum():
            raise ValueError(f"Invalid watermark column name: {watermark_col}")
        # Use first() instead of collect() for single row result
        result = self.spark.sql(f"SELECT MAX(`{watermark_col}`) as max_watermark FROM {target_table}").first()
        
        if result and result['max_watermark']:
            return str(result['max_watermark'])
            
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