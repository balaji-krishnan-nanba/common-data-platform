"""Configuration management for the data ingestion framework."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages configuration for the data ingestion framework."""
    
    def __init__(self, config_base_path: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_base_path: Base path for configuration files. 
                            Defaults to 'config' directory in project root.
        """
        self.project_code = os.getenv("PROJECT_CODE", "cddp")
        self.environment = os.getenv("ENVIRONMENT", "dev")
        
        if config_base_path is None:
            # Check if we're running on Databricks
            if os.path.exists("/Workspace"):
                # Running on Databricks - check common workspace locations
                workspace_paths = [
                    f"/Workspace/Users/{os.getenv('USER', 'balaji.krishnan@nanba.co.uk')}/common-data-platform/devops/config",
                    f"/Workspace/Shared/common-data-platform/devops/config",
                    f"/Workspace/common-data-platform/devops/config",
                    # Bundle deployment path
                    f"/Workspace/Users/{os.getenv('USER', 'balaji.krishnan@nanba.co.uk')}/.bundle/common-data-platform/{self.environment}/files/devops/config"
                ]
                
                config_base_path = None
                for path in workspace_paths:
                    if os.path.exists(path):
                        config_base_path = path
                        logger.info(f"Found config directory at: {path}")
                        break
                
                if config_base_path is None:
                    # If not found in workspace, check if configs are in bundle artifacts
                    bundle_config_path = f"/Workspace/Users/{os.getenv('USER', 'balaji.krishnan@nanba.co.uk')}/.bundle/common-data-platform/{self.environment}/artifacts/config"
                    if os.path.exists(bundle_config_path):
                        config_base_path = bundle_config_path
                    else:
                        raise FileNotFoundError(
                            f"Configuration directory not found in any expected Databricks workspace location. "
                            f"Searched paths: {workspace_paths}"
                        )
            else:
                # Local development - find project root
                current_path = Path(__file__).parent.parent.parent
                self.devops_path = current_path / "devops"
                config_base_path = self.devops_path / "config"
        else:
            # If custom path provided, assume devops is at same level
            self.devops_path = Path(config_base_path).parent
        
        self.config_base_path = Path(config_base_path)
        self._validate_config_path()
        
        # Cache for loaded configurations
        self._config_cache: Dict[str, Any] = {}
        
    def _validate_config_path(self) -> None:
        """Validate that configuration path exists."""
        if not self.config_base_path.exists():
            raise FileNotFoundError(
                f"Configuration directory not found: {self.config_base_path}"
            )
    
    def get_catalog_name(self, layer: str) -> str:
        """
        Get catalog name in format: <project>-<env>-<layer>.
        
        Args:
            layer: Data layer (bronze, silver, gold)
            
        Returns:
            Formatted catalog name
        """
        return f"{self.project_code}-{self.environment}-{layer}"
    
    def get_schema_name(self, source_type: str) -> str:
        """
        Get schema name based on source type.
        
        Args:
            source_type: Type of source (excel, csv, json, oracle, etc.)
            
        Returns:
            Schema name
        """
        # Map source types to schema names
        schema_mapping = {
            "excel": "excel_data",
            "csv": "csv_data", 
            "json": "json_data",
            "oracle": "oracle_data",
            "api": "api_data",
            "sftp": "sftp_data"
        }
        
        return schema_mapping.get(source_type, f"{source_type}_data")
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load YAML configuration file.
        
        Args:
            config_path: Path to config file relative to config base path
            
        Returns:
            Configuration dictionary
        """
        cache_key = str(config_path)
        
        if cache_key in self._config_cache:
            return self._config_cache[cache_key]
        
        full_path = self.config_base_path / config_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {full_path}")
        
        with open(full_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Resolve environment variables
        config = self._resolve_env_vars(config)
        
        self._config_cache[cache_key] = config
        return config
    
    def _resolve_env_vars(self, config: Any) -> Any:
        """
        Recursively resolve environment variables in configuration.
        
        Args:
            config: Configuration dictionary or value
            
        Returns:
            Configuration with resolved environment variables
        """
        if isinstance(config, dict):
            return {k: self._resolve_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._resolve_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
            # Extract variable name
            var_name = config[2:-1]
            
            # Handle nested variables like ${var.key}
            if "." in var_name:
                parts = var_name.split(".")
                if parts[0] == "var":
                    # This is a bundle variable reference
                    return os.getenv(parts[1], config)
            
            return os.getenv(var_name, config)
        else:
            return config
    
    def load_source_config(self, source_type: str) -> Dict[str, Any]:
        """
        Load source configuration for a specific type.
        
        Args:
            source_type: Type of source (excel, csv, json, etc.)
            
        Returns:
            Source configuration dictionary
        """
        config_file = f"sources/{source_type}_sources.yaml"
        return self.load_config(config_file)
    
    def load_transformation_config(self, transformation_name: str) -> Dict[str, Any]:
        """
        Load transformation configuration.
        
        Args:
            transformation_name: Name of transformation
            
        Returns:
            Transformation configuration dictionary
        """
        config_file = f"transformations/{transformation_name}.yaml"
        return self.load_config(config_file)
    
    def get_quality_rules(self, rule_set: str) -> Dict[str, Any]:
        """
        Load data quality rules.
        
        Args:
            rule_set: Name of rule set
            
        Returns:
            Quality rules dictionary
        """
        config_file = f"quality/{rule_set}_rules.yaml"
        return self.load_config(config_file)
    
    def get_notification_config(self) -> Dict[str, Any]:
        """
        Load notification configuration.
        
        Returns:
            Notification configuration dictionary
        """
        return self.load_config("notifications/config.yaml")
    
    def list_sources(self, source_type: str) -> List[str]:
        """
        List all configured sources of a specific type.
        
        Args:
            source_type: Type of source
            
        Returns:
            List of source names
        """
        sources = self.load_source_config(source_type)
        return list(sources.keys())
    
    def get_database_config(self, db_name: str) -> Dict[str, Any]:
        """
        Get database connection configuration.
        
        Args:
            db_name: Database name
            
        Returns:
            Database configuration
        """
        databases = self.load_config("databases/connections.yaml")
        
        if db_name not in databases:
            raise ValueError(f"Database configuration not found: {db_name}")
        
        return databases[db_name]
    
    def get_azure_config(self) -> Dict[str, Any]:
        """
        Get Azure-specific configuration.
        
        Returns:
            Azure configuration dictionary
        """
        return {
            "tenant_id": os.getenv("AZURE_TENANT_ID"),
            "subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID"),
            "resource_group": os.getenv("AZURE_RESOURCE_GROUP"),
            "key_vault_url": os.getenv("AZURE_KEY_VAULT_URL"),
            "storage_account": os.getenv("AZURE_STORAGE_ACCOUNT"),
            "datalake_storage_account": os.getenv("AZURE_DATALAKE_STORAGE_ACCOUNT"),
            "source_storage_account": os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT")
        }