"""Common Data Platform - A modular data ingestion framework for Azure Databricks."""

__version__ = "0.1.0"
__author__ = "Your Organization"
__description__ = "A comprehensive data platform for building medallion architectures in Azure Databricks"

# Make key components available at package level
from .core.config_manager import ConfigManager
from .core.secret_manager import SecretManager

__all__ = ["ConfigManager", "SecretManager"]