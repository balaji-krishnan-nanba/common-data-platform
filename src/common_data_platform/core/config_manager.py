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
            # Find project root (where devops directory exists)
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
            layer: Medallion layer (bronze, silver, gold)
            
        Returns:
            Formatted catalog name (e.g., cddp-dev-bronze)
        """
        return f"{self.project_code}-{self.environment}-{layer}"
    
    def get_all_catalogs(self) -> List[str]:
        """
        Get all catalog names for current environment.
        
        Returns:
            List of catalog names for bronze, silver, and gold layers
        """
        return [
            self.get_catalog_name("bronze"),
            self.get_catalog_name("silver"),
            self.get_catalog_name("gold")
        ]
    
    def load_environment_config(self) -> Dict[str, Any]:
        """
        Load environment-specific configuration.
        
        Returns:
            Dictionary containing environment configuration
        """
        config_file = self.devops_path / "environments" / f"{self.environment}.yml"
        return self._load_yaml(config_file)
    
    def load_source_config(self, source_type: str) -> Dict[str, Any]:
        """
        Load source-specific configuration.
        
        Args:
            source_type: Type of source (excel, csv, oracle, etc.)
            
        Returns:
            Dictionary containing source configuration
        """
        config_file = self.config_base_path / "sources" / f"{source_type}_sources.yaml"
        config = self._load_yaml(config_file)
        
        # Apply defaults for each source
        for source_name, source_config in config.items():
            config[source_name] = self._apply_source_defaults(source_config, source_type)
        
        return config
    
    def load_transformation_config(self, layer_transition: str, source_type: str) -> Dict[str, Any]:
        """
        Load transformation configuration for a specific layer transition.
        
        Args:
            layer_transition: Transition type (e.g., bronze_to_silver, silver_to_gold)
            source_type: Source type for transformation
            
        Returns:
            Dictionary containing transformation configuration
        """
        config_file = (
            self.config_base_path / "transformations" / 
            layer_transition / f"{source_type}_transformations.yaml"
        )
        return self._load_yaml(config_file)
    
    def _load_yaml(self, file_path: Path) -> Dict[str, Any]:
        """
        Load YAML configuration file.
        
        Args:
            file_path: Path to YAML file
            
        Returns:
            Dictionary containing configuration
        """
        # Check cache first
        cache_key = str(file_path)
        if cache_key in self._config_cache:
            return self._config_cache[cache_key]
        
        if not file_path.exists():
            logger.warning(f"Configuration file not found: {file_path}")
            return {}
        
        try:
            with open(file_path, 'r') as f:
                config = yaml.safe_load(f) or {}
                
            # Substitute environment variables
            config = self._substitute_env_vars(config)
            
            # Cache the configuration
            self._config_cache[cache_key] = config
            
            return config
            
        except Exception as e:
            logger.error(f"Error loading configuration from {file_path}: {str(e)}")
            raise
    
    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in configuration.
        
        Args:
            config: Configuration dictionary or value
            
        Returns:
            Configuration with environment variables substituted
        """
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Replace ${VAR} patterns with environment variable values
            if config.startswith("${") and config.endswith("}"):
                var_name = config[2:-1]
                if var_name == "project_code":
                    return self.project_code
                elif var_name == "environment":
                    return self.environment
                else:
                    return os.getenv(var_name, config)
            return config
        else:
            return config
    
    def get_full_table_name(self, layer: str, schema: str, table: str) -> str:
        """
        Get fully qualified table name.
        
        Args:
            layer: Medallion layer (bronze, silver, gold)
            schema: Schema/database name
            table: Table name
            
        Returns:
            Fully qualified table name
        """
        catalog = self.get_catalog_name(layer)
        return f"`{catalog}`.`{schema}`.`{table}`"
    
    def clear_cache(self) -> None:
        """Clear configuration cache."""
        self._config_cache.clear()
        logger.info("Configuration cache cleared")
    
    def _apply_source_defaults(self, config: Dict[str, Any], source_type: str) -> Dict[str, Any]:
        """Apply default values to source configuration."""
        # Common defaults for all sources
        defaults = {
            "type": source_type,
            "ingestion": {
                "mode": "full",
                "batch_size": 100,
                "fail_on_error": True
            },
            "read_options": {},
            "write_options": {
                "overwriteSchema": "true"
            }
        }
        
        # Type-specific defaults
        if source_type in ["excel", "csv"]:
            defaults["read_options"] = {
                "header": "true",
                "inferSchema": "true",
                "treatEmptyValuesAsNulls": "true"
            }
            defaults["connection"]["file_pattern"] = "*.*"
            
        elif source_type == "oracle":
            defaults["ingestion"]["fetch_size"] = "10000"
            defaults["ingestion"]["num_partitions"] = "10"
            
        # Apply target defaults if not specified
        if "target" not in config:
            config["target"] = {}
        
        if "catalog" not in config["target"]:
            config["target"]["catalog"] = self.get_catalog_name("bronze")
        
        if "schema" not in config["target"]:
            config["target"]["schema"] = f"{source_type}_data"
            
        if "table" not in config["target"] and "name" in config:
            # Use source name as table name by default
            config["target"]["table"] = config["name"].replace("-", "_")
        
        # Deep merge defaults with config
        return self._deep_merge(defaults, config)
    
    def _deep_merge(self, default: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries, with override taking precedence."""
        merged = default.copy()
        
        for key, value in override.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = self._deep_merge(merged[key], value)
            else:
                merged[key] = value
                
        return merged