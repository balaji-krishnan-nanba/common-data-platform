"""Ingestion module for data pipeline operations."""

from .base_ingester import BaseIngester
from .file_ingester import FileIngester
from .oracle_ingester import OracleIngester

__all__ = ['BaseIngester', 'FileIngester', 'OracleIngester']