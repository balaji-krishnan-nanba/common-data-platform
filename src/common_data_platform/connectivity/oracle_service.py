"""Oracle database service for JDBC connections."""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


class OracleService:
    """Service for Oracle database operations via JDBC."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], secret_manager):
        """
        Initialize Oracle service.
        
        Args:
            spark: Active Spark session
            config: Oracle configuration
            secret_manager: Secret manager for credentials
        """
        self.spark = spark
        self.config = config
        self.secret_manager = secret_manager
        self.credentials = {}
        self._validate_config()
        
    def _validate_config(self) -> None:
        """Validate Oracle configuration."""
        required_fields = ['host', 'port', 'service_name']
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required configuration field: {field}")
    
    def configure_connection(self) -> None:
        """Establish connection by retrieving credentials."""
        try:
            # Get credentials from secret manager
            self.credentials = self.secret_manager.get_connection_credentials(
                'oracle', self.config
            )
            
            # Validate credentials
            if 'username' not in self.credentials or 'password' not in self.credentials:
                raise ValueError("Missing username or password credentials")
            
            logger.info(f"Successfully retrieved Oracle credentials for {self.config['host']}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test connection to Oracle database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.configure_connection()
            # Test with a simple query
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.get_connection_string()) \
                .option("query", "SELECT 1 FROM DUAL") \
                .option("user", self.credentials["username"]) \
                .option("password", self.credentials["password"]) \
                .load()
            test_df.count()  # Force execution
            logger.info("Successfully connected to Oracle database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Oracle database: {str(e)}")
            return False
    
    def read(
        self, 
        table: Optional[str] = None,
        query: Optional[str] = None,
        schema: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read data from Oracle database.
        
        Args:
            table: Table name to read from
            query: Custom SQL query
            schema: Schema/owner name
            options: JDBC options
            
        Returns:
            Spark DataFrame
        """
        try:
            if not self.credentials:
                self.configure_connection()
            
            # Build JDBC URL
            jdbc_url = self.get_connection_string()
            
            # Determine what to read
            if query:
                dbtable = f"({query}) as subquery"
            elif table:
                if schema:
                    dbtable = f"{schema}.{table}"
                else:
                    dbtable = table
            else:
                raise ValueError("Either table or query must be specified")
            
            # Default JDBC properties
            jdbc_properties = {
                "user": self.credentials["username"],
                "password": self.credentials["password"],
                "driver": "oracle.jdbc.driver.OracleDriver"
            }
            
            # Add custom options
            if options:
                jdbc_properties.update(options)
            
            logger.info(f"Reading from Oracle: {dbtable}")
            
            # Read using JDBC
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", dbtable) \
                .options(**jdbc_properties) \
                .load()
            
            logger.info(f"Successfully read {df.count()} rows from Oracle")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from Oracle: {str(e)}")
            raise
    
    def write(
        self, 
        df: DataFrame, 
        table: str,
        schema: Optional[str] = None,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> None:
        """
        Write DataFrame to Oracle database.
        
        Args:
            df: DataFrame to write
            table: Target table name
            schema: Schema/owner name
            mode: Write mode
            options: JDBC options
        """
        try:
            if not self.credentials:
                self.configure_connection()
            
            # Build JDBC URL
            jdbc_url = self.get_connection_string()
            
            # Build full table name
            if schema:
                full_table = f"{schema}.{table}"
            else:
                full_table = table
            
            # Default JDBC properties
            jdbc_properties = {
                "user": self.credentials["username"],
                "password": self.credentials["password"],
                "driver": "oracle.jdbc.driver.OracleDriver"
            }
            
            # Add custom options
            if options:
                jdbc_properties.update(options)
            
            logger.info(f"Writing {df.count()} rows to Oracle table: {full_table}")
            
            # Write using JDBC
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", full_table) \
                .mode(mode) \
                .options(**jdbc_properties) \
                .save()
            
            logger.info(f"Successfully wrote data to Oracle table: {full_table}")
            
        except Exception as e:
            logger.error(f"Error writing to Oracle: {str(e)}")
            raise
    
    def read_multiple_tables(
        self, 
        tables: List[Dict[str, Any]],
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, DataFrame]:
        """
        Read multiple tables from Oracle.
        
        Args:
            tables: List of table configurations
            options: Common JDBC options
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        dataframes = {}
        
        for table_config in tables:
            table_name = table_config['name']
            
            try:
                df = self.read(
                    table=table_config.get('source_table', table_name),
                    schema=table_config.get('source_schema'),
                    options=options
                )
                
                dataframes[table_name] = df
                
            except Exception as e:
                logger.error(f"Error reading table {table_name}: {str(e)}")
                # Continue with other tables
                continue
        
        return dataframes
    
    def get_connection_string(self) -> str:
        """
        Build Oracle JDBC connection string.
        
        Returns:
            JDBC URL
        """
        host = self.config['host']
        port = self.config['port']
        service_name = self.config['service_name']
        
        return f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
    
    def get_table_schema(self, table: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get schema information for a table.
        
        Args:
            table: Table name
            schema: Schema/owner name
            
        Returns:
            List of column definitions
        """
        try:
            # Query to get table structure
            if schema:
                query = f"""
                    SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
                    FROM all_tab_columns 
                    WHERE owner = '{schema.upper()}' AND table_name = '{table.upper()}'
                    ORDER BY column_id
                """
            else:
                query = f"""
                    SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
                    FROM user_tab_columns 
                    WHERE table_name = '{table.upper()}'
                    ORDER BY column_id
                """
            
            schema_df = self.read(query=query)
            
            columns = []
            for row in schema_df.collect():
                col_def = {
                    'name': row.COLUMN_NAME,
                    'type': self._map_oracle_type(row),
                    'nullable': row.NULLABLE == 'Y'
                }
                columns.append(col_def)
            
            return columns
            
        except Exception as e:
            logger.error(f"Error getting table schema: {str(e)}")
            raise
    
    def _map_oracle_type(self, row) -> str:
        """
        Map Oracle data types to Spark SQL types.
        
        Args:
            row: Row containing Oracle column information
            
        Returns:
            Spark SQL type string
        """
        oracle_type = row.DATA_TYPE.upper()
        
        if oracle_type in ['VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR', 'CLOB', 'NCLOB']:
            return 'string'
        elif oracle_type == 'NUMBER':
            if row.DATA_SCALE and row.DATA_SCALE > 0:
                return f'decimal({row.DATA_PRECISION or 38},{row.DATA_SCALE})'
            else:
                if row.DATA_PRECISION and row.DATA_PRECISION <= 10:
                    return 'int'
                else:
                    return 'bigint'
        elif oracle_type == 'DATE':
            return 'timestamp'
        elif oracle_type in ['TIMESTAMP', 'TIMESTAMP(6)']:
            return 'timestamp'
        elif oracle_type == 'BLOB':
            return 'binary'
        else:
            return 'string'  # Default fallback