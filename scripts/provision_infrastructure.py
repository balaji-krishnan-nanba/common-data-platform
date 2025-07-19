#!/usr/bin/env python3
"""
Provision Unity Catalog infrastructure for the data platform.

This script creates all required catalogs, schemas, and system tables
for the modular data ingestion framework.
"""

import sys
import os
import argparse
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from src.core.config_manager import ConfigManager
from src.core.catalog_manager import CatalogManager
from src.core.secret_manager import SecretManager
from src.utilities.logger import setup_logging, DataPipelineLogger

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session for infrastructure provisioning."""
    return SparkSession.builder \
        .appName("DataPlatform-Infrastructure-Provisioning") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def provision_catalogs(catalog_manager: CatalogManager, environments: list) -> None:
    """
    Provision Unity Catalog catalogs for specified environments.
    
    Args:
        catalog_manager: Catalog manager instance
        environments: List of environments to provision
    """
    try:
        logger.info(f"Provisioning catalogs for environments: {environments}")
        
        catalog_manager.create_all_catalogs(environments)
        
        logger.info("Successfully provisioned all catalogs")
        
    except Exception as e:
        logger.error(f"Error provisioning catalogs: {str(e)}")
        raise


def create_system_tables(spark: SparkSession, config_manager: ConfigManager) -> None:
    """
    Create system tables for tracking and metadata.
    
    Args:
        spark: Spark session
        config_manager: Configuration manager
    """
    try:
        logger.info("Creating system tables")
        
        # Create processed files tracking table
        bronze_catalog = config_manager.get_catalog_name("bronze")
        
        processed_files_ddl = f"""
        CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.`system`.`processed_files` (
            file_path STRING NOT NULL,
            source_name STRING NOT NULL,
            processed_timestamp TIMESTAMP NOT NULL,
            file_size_bytes BIGINT,
            record_count BIGINT,
            checksum STRING,
            processing_duration_seconds DOUBLE,
            status STRING,
            error_message STRING,
            batch_id STRING,
            environment STRING,
            created_by STRING
        ) USING DELTA
        PARTITIONED BY (source_name, DATE(processed_timestamp))
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        spark.sql(processed_files_ddl)
        logger.info(f"Created processed_files table in {bronze_catalog}.system")
        
        # Create watermarks table for incremental loads
        watermarks_ddl = f"""
        CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.`system`.`watermarks` (
            source_name STRING NOT NULL,
            table_name STRING NOT NULL,
            watermark_column STRING NOT NULL,
            watermark_value STRING NOT NULL,
            last_updated TIMESTAMP NOT NULL,
            environment STRING NOT NULL
        ) USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
        """
        
        spark.sql(watermarks_ddl)
        logger.info(f"Created watermarks table in {bronze_catalog}.system")
        
        # Create data quality results table
        dq_results_ddl = f"""
        CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.`system`.`data_quality_results` (
            check_id STRING NOT NULL,
            table_name STRING NOT NULL,
            check_type STRING NOT NULL,
            check_name STRING NOT NULL,
            status STRING NOT NULL,
            result_details STRING,
            records_checked BIGINT,
            records_failed BIGINT,
            check_timestamp TIMESTAMP NOT NULL,
            batch_id STRING,
            environment STRING
        ) USING DELTA
        PARTITIONED BY (DATE(check_timestamp))
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
        """
        
        spark.sql(dq_results_ddl)
        logger.info(f"Created data_quality_results table in {bronze_catalog}.system")
        
        # Create pipeline executions table for logging
        pipeline_executions_ddl = f"""
        CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.`system`.`pipeline_executions` (
            pipeline_id STRING NOT NULL,
            pipeline_name STRING NOT NULL,
            status STRING NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            duration_seconds DOUBLE,
            records_processed BIGINT,
            files_processed INT,
            error_message STRING,
            source_name STRING,
            target_layer STRING,
            batch_id STRING,
            environment STRING NOT NULL,
            user_name STRING,
            cluster_id STRING
        ) USING DELTA
        PARTITIONED BY (DATE(start_time), environment)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        spark.sql(pipeline_executions_ddl)
        logger.info(f"Created pipeline_executions table in {bronze_catalog}.system")
        
        # Create stage executions table for granular logging
        stage_executions_ddl = f"""
        CREATE TABLE IF NOT EXISTS `{bronze_catalog}`.`system`.`stage_executions` (
            pipeline_id STRING NOT NULL,
            stage_name STRING NOT NULL,
            status STRING NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            duration_seconds DOUBLE,
            records_processed BIGINT,
            stage_details STRING,
            error_message STRING,
            environment STRING NOT NULL
        ) USING DELTA
        PARTITIONED BY (DATE(start_time), environment)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
        """
        
        spark.sql(stage_executions_ddl)
        logger.info(f"Created stage_executions table in {bronze_catalog}.system")
        
        logger.info("Successfully created all system tables")
        
    except Exception as e:
        logger.error(f"Error creating system tables: {str(e)}")
        raise


def setup_permissions(spark: SparkSession, config_manager: ConfigManager) -> None:
    """
    Setup Unity Catalog permissions (placeholder for actual implementation).
    
    Args:
        spark: Spark session
        config_manager: Configuration manager
    """
    try:
        logger.info("Setting up Unity Catalog permissions")
        
        # Note: In a real implementation, you would:
        # 1. Create service principals
        # 2. Assign appropriate roles (SELECT, MODIFY, etc.)
        # 3. Set up data lineage and governance
        
        # Example permission setup (commented out as it requires admin privileges)
        """
        for catalog in config_manager.get_all_catalogs():
            # Grant usage on catalog
            spark.sql(f"GRANT USAGE ON CATALOG `{catalog}` TO `data-engineers`")
            
            # Grant permissions on schemas
            spark.sql(f"GRANT CREATE, USAGE ON SCHEMA `{catalog}`.`*` TO `data-engineers`")
            
            # Grant select permissions to analysts
            spark.sql(f"GRANT SELECT ON SCHEMA `{catalog}`.`*` TO `data-analysts`")
        """
        
        logger.info("Permissions setup completed")
        
    except Exception as e:
        logger.warning(f"Error setting up permissions: {str(e)}")
        # Don't fail the entire provisioning for permission errors


def main():
    """Main provisioning function."""
    parser = argparse.ArgumentParser(description="Provision data platform infrastructure")
    parser.add_argument(
        "--environments", 
        nargs="+", 
        default=["dev"], 
        help="Environments to provision (default: dev)"
    )
    parser.add_argument(
        "--project-code", 
        default="cddp", 
        help="Project code for catalog naming (default: cddp)"
    )
    parser.add_argument(
        "--skip-system-tables", 
        action="store_true", 
        help="Skip creation of system tables"
    )
    parser.add_argument(
        "--skip-permissions", 
        action="store_true", 
        help="Skip permission setup"
    )
    parser.add_argument(
        "--log-level", 
        default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(log_level=args.log_level)
    pipeline_logger = DataPipelineLogger(__name__)
    
    # Set environment variables
    os.environ["PROJECT_CODE"] = args.project_code
    
    try:
        pipeline_logger.start_pipeline("infrastructure_provisioning")
        
        logger.info("Starting infrastructure provisioning")
        logger.info(f"Project code: {args.project_code}")
        logger.info(f"Environments: {args.environments}")
        
        # Create Spark session
        spark = create_spark_session()
        
        # Initialize managers
        config_manager = ConfigManager()
        catalog_manager = CatalogManager(spark, config_manager)
        secret_manager = SecretManager(spark)
        
        # Provision catalogs for all environments
        provision_catalogs(catalog_manager, args.environments)
        
        # Create system tables (only for first environment to avoid duplicates)
        if not args.skip_system_tables:
            os.environ["ENVIRONMENT"] = args.environments[0]
            config_manager = ConfigManager()  # Reload with first environment
            create_system_tables(spark, config_manager)
        
        # Setup permissions
        if not args.skip_permissions:
            setup_permissions(spark, config_manager)
        
        logger.info("Infrastructure provisioning completed successfully")
        pipeline_logger.end_pipeline("success")
        
    except Exception as e:
        logger.error(f"Infrastructure provisioning failed: {str(e)}")
        pipeline_logger.end_pipeline("failed", str(e))
        sys.exit(1)
        
    finally:
        # Stop Spark session
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()