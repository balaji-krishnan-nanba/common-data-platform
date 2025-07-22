"""Secret management using Azure Key Vault-backed Databricks secret scopes."""

from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


class SecretManager:
    """Manages secrets using Databricks secret scopes backed by Azure Key Vault."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize secret manager.
        
        Args:
            spark: Active Spark session with dbutils access
        """
        self.spark = spark
        
        # Get dbutils from spark session
        try:
            self.dbutils = spark._jvm.com.databricks.service.DBUtils.get(spark._jsc)
        except Exception:
            # For local testing or when dbutils is not available
            logger.warning("dbutils not available - using mock implementation")
            self.dbutils = None
    
    def get_secret(self, scope: str, key: str) -> str:
        """
        Retrieve a secret from Databricks secret scope.
        
        Args:
            scope: Secret scope name
            key: Secret key within the scope
            
        Returns:
            Secret value
        """
        try:
            if self.dbutils:
                return self.dbutils.secrets.get(scope, key)
            else:
                # Mock implementation for testing
                logger.warning(f"Mock secret retrieval: {scope}/{key}")
                return f"mock_secret_{key}"
                
        except Exception as e:
            logger.error(f"Error retrieving secret {key} from scope {scope}: {str(e)}")
            raise
    
    def get_connection_credentials(self, source_type: str, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Get connection credentials for a data source.
        
        Args:
            source_type: Type of data source (oracle, postgres, etc.)
            config: Source configuration containing secret references
            
        Returns:
            Dictionary with actual credential values
        """
        credentials = {}
        
        try:
            # Extract secret scope and keys from config
            if 'secret_scope' in config:
                scope = config['secret_scope']
                
                # Common credential mappings
                credential_mappings = {
                    'username_key': 'username',
                    'password_key': 'password',
                    'api_key': 'api_key',
                    'client_id_key': 'client_id',
                    'client_secret_key': 'client_secret',
                    'storage_account_key': 'account_key'
                }
                
                for config_key, cred_key in credential_mappings.items():
                    if config_key in config:
                        secret_key = config[config_key]
                        credentials[cred_key] = self.get_secret(scope, secret_key)
                        
            return credentials
            
        except Exception as e:
            logger.error(f"Error retrieving credentials for {source_type}: {str(e)}")
            raise
    
    def get_storage_credentials(self, storage_config: Dict[str, Any]) -> Dict[str, str]:
        """
        Get Azure storage credentials.
        
        Args:
            storage_config: Storage configuration
            
        Returns:
            Dictionary with storage credentials
        """
        credentials = {}
        
        try:
            if 'storage_account' in storage_config:
                storage_account = storage_config['storage_account']
                
                # Check for different authentication methods
                if 'secret_scope' in storage_config:
                    scope = storage_config['secret_scope']
                    
                    if 'account_key' in storage_config:
                        key = self.get_secret(scope, storage_config['account_key'])
                        credentials['account_key'] = key
                        
                    elif 'sas_token_key' in storage_config:
                        sas = self.get_secret(scope, storage_config['sas_token_key'])
                        credentials['sas_token'] = sas
                        
                    elif 'service_principal' in storage_config:
                        sp_config = storage_config['service_principal']
                        credentials['client_id'] = self.get_secret(scope, sp_config['client_id_key'])
                        credentials['client_secret'] = self.get_secret(scope, sp_config['client_secret_key'])
                        credentials['tenant_id'] = sp_config.get('tenant_id', '')
                        
            return credentials
            
        except Exception as e:
            logger.error(f"Error retrieving storage credentials: {str(e)}")
            raise
    
    def configure_spark_for_storage(self, storage_config: Dict[str, Any]) -> None:
        """
        Configure Spark session with storage credentials.
        
        Args:
            storage_config: Storage configuration
        """
        try:
            storage_account = storage_config.get('storage_account')
            if not storage_account:
                return
                
            credentials = self.get_storage_credentials(storage_config)
            
            # Configure based on authentication type
            if 'account_key' in credentials:
                config_key = f"fs.azure.account.key.{storage_account}.dfs.core.windows.net"
                self.spark.conf.set(config_key, credentials['account_key'])
                
            elif 'sas_token' in credentials:
                config_key = f"fs.azure.sas.{storage_account}.dfs.core.windows.net"
                self.spark.conf.set(config_key, credentials['sas_token'])
                
            elif 'client_id' in credentials:
                # Service principal authentication
                self.spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
                self.spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", 
                                  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
                self.spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", 
                                  credentials['client_id'])
                self.spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", 
                                  credentials['client_secret'])
                self.spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
                                  f"https://login.microsoftonline.com/{credentials['tenant_id']}/oauth2/token")
                
            logger.info(f"Successfully configured Spark for storage account: {storage_account}")
            
        except Exception as e:
            logger.error(f"Error configuring Spark for storage: {str(e)}")
            raise
    
    def list_scopes(self) -> list:
        """
        List available secret scopes.
        
        Returns:
            List of secret scope names
        """
        try:
            if self.dbutils:
                return [scope.name for scope in self.dbutils.secrets.listScopes()]
            else:
                logger.warning("Mock scope listing")
                return ["mock-scope"]
                
        except Exception as e:
            logger.error(f"Error listing secret scopes: {str(e)}")
            raise