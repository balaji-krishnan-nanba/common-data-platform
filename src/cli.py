#!/usr/bin/env python3
"""
Command-line interface for the Common Data Platform.

This CLI provides entry points for running ingestion pipelines and transformations.
"""

import sys
import os
import click
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from common_data_platform.core.config_manager import ConfigManager
from common_data_platform.core.secret_manager import SecretManager
from common_data_platform.ingestion.file_ingester import FileIngester
from common_data_platform.utilities.logger import setup_logging, DataPipelineLogger

logger = logging.getLogger(__name__)


def create_spark_session():
    """Get or create Spark session for pipeline operations."""
    try:
        from pyspark.sql import SparkSession
        
        # First try to get active session (for Databricks)
        spark = SparkSession.getActiveSession()
        if spark:
            logger.info("Using existing active Spark session")
            return spark
        
        # If no active session, create one (for local testing)
        logger.info("Creating new Spark session")
        return SparkSession.builder \
            .appName("CommonDataPlatform-CLI") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
            .config("spark.databricks.optimizer.dynamicFilePruning", "true") \
            .getOrCreate()
    except ImportError:
        logger.error("PySpark not available. This CLI requires PySpark to run.")
        sys.exit(1)


@click.group()
@click.option('--log-level', default='INFO', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Set the logging level')
@click.option('--project-code', default='cddp', help='Project code for catalog naming')
@click.option('--environment', default='dev', help='Environment (dev/test/prod)')
@click.pass_context
def cli(ctx, log_level, project_code, environment):
    """Common Data Platform CLI."""
    # Ensure context object exists
    ctx.ensure_object(dict)
    
    # Set environment variables
    os.environ['PROJECT_CODE'] = project_code
    os.environ['ENVIRONMENT'] = environment
    
    # Setup logging
    setup_logging(log_level=log_level)
    
    # Store common parameters in context
    ctx.obj['project_code'] = project_code
    ctx.obj['environment'] = environment
    ctx.obj['log_level'] = log_level


@cli.command()
@click.option('--source', help='Source configuration name')
@click.option('--inline-config', help='Inline JSON configuration')
@click.option('--batch-id', help='Optional batch identifier')
@click.option('--write-mode', default='append', 
              type=click.Choice(['append', 'overwrite']),
              help='Write mode for target table')
@click.option('--fail-on-error/--continue-on-error', default=True,
              help='Whether to fail pipeline on individual file errors')
@click.pass_context
def run_bronze_ingestion(ctx, source, inline_config, batch_id, write_mode, fail_on_error):
    """Run bronze layer ingestion for specified source."""
    pipeline_logger = DataPipelineLogger(__name__)
    
    try:
        # Handle inline configuration
        if inline_config and source:
            raise click.BadParameter("Cannot specify both --source and --inline-config")
        
        if not inline_config and not source:
            raise click.BadParameter("Must specify either --source or --inline-config")
            
        # Initialize components
        spark = create_spark_session()
        config_manager = ConfigManager()
        secret_manager = SecretManager(spark)
        
        # Load or parse configuration
        if inline_config:
            import json
            source_def = json.loads(inline_config)
            source_type = source_def.get('type', 'file')
            source_name = source_def.get('name', 'inline_source')
            # Apply defaults to inline config
            source_def = config_manager._apply_source_defaults(source_def, source_type)
        else:
            # Load from configured sources
            source_def, source_type = _find_source_config(config_manager, source)
            source_name = source
            
        pipeline_id = f"bronze_ingestion_{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        pipeline_logger.start_pipeline(pipeline_id)
        
        logger.info(f"Starting bronze ingestion for source: {source_name}")
        
        # Prepare target configuration
        target_config = source_def.get('target', {})
        target_config['catalog'] = config_manager.get_catalog_name('bronze')
        
        # Create appropriate ingester
        if source_type in ['excel', 'csv', 'parquet', 'json', 'file']:
            ingester = FileIngester(spark, config_manager, secret_manager)
        elif source_type in ['oracle', 'postgresql', 'mysql', 'sqlserver', 'database']:
            from common_data_platform.ingestion.oracle_ingester import OracleIngester
            ingester = OracleIngester(spark, config_manager, secret_manager)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # Run ingestion
        result = ingester.ingest(
            source_config=source_def,
            target_config=target_config,
            batch_id=batch_id or pipeline_id,
            write_mode=write_mode,
            fail_on_error=fail_on_error
        )
        
        # Log results
        if result['status'] == 'success':
            pipeline_logger.log_stage(
                stage='bronze_ingestion',
                status='success',
                records_processed=result['records_processed'],
                files_processed=result.get('files_processed', 0)
            )
            logger.info(f"Bronze ingestion completed successfully: {result}")
            pipeline_logger.end_pipeline('success')
        else:
            pipeline_logger.log_stage(
                stage='bronze_ingestion',
                status='error',
                error_message=result.get('error', 'Unknown error')
            )
            pipeline_logger.end_pipeline('failed', result.get('error'))
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {str(e)}")
        pipeline_logger.end_pipeline('failed', str(e))
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()


@cli.command()
@click.option('--source', required=True, help='Source configuration name')
@click.option('--transformation', help='Specific transformation to run')
@click.option('--write-mode', default='overwrite',
              type=click.Choice(['append', 'overwrite']),
              help='Write mode for target table')
@click.pass_context
def run_silver_transformation(ctx, source, transformation, write_mode):
    """Run silver layer transformations for specified source."""
    pipeline_logger = DataPipelineLogger(__name__)
    
    try:
        pipeline_id = f"silver_transformation_{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        pipeline_logger.start_pipeline(pipeline_id)
        
        logger.info(f"Starting silver transformation for source: {source}")
        
        # Initialize components
        spark = create_spark_session()
        config_manager = ConfigManager()
        
        # Find source configuration to determine type
        _, source_type = _find_source_config(config_manager, source)
        
        # Load transformation configuration
        transform_config = config_manager.load_transformation_config(
            'bronze_to_silver', source_type
        )
        
        # Run transformations
        transformations_run = 0
        
        for transform_name, transform_def in transform_config.items():
            if transformation and transform_name != transformation:
                continue
                
            logger.info(f"Running transformation: {transform_name}")
            
            # This is a simplified version - in the full implementation,
            # you would have a TransformationEngine that handles both
            # PySpark and SparkSQL transformations
            
            engine = transform_def.get('engine', 'sparksql')
            
            if engine == 'sparksql':
                _run_sql_transformation(spark, config_manager, transform_def)
            elif engine == 'pyspark':
                _run_pyspark_transformation(spark, config_manager, transform_def)
            else:
                raise ValueError(f"Unsupported transformation engine: {engine}")
            
            transformations_run += 1
            
            pipeline_logger.log_stage(
                stage=f'silver_transformation_{transform_name}',
                status='success',
                transformation_name=transform_name,
                engine=engine
            )
        
        logger.info(f"Silver transformation completed: {transformations_run} transformations run")
        pipeline_logger.end_pipeline('success')
        
    except Exception as e:
        logger.error(f"Silver transformation failed: {str(e)}")
        pipeline_logger.end_pipeline('failed', str(e))
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()


@cli.command()
@click.option('--source', required=True, help='Source configuration name')
@click.pass_context
def run_gold_transformation(ctx, source):
    """Run gold layer transformations for specified source."""
    logger.info(f"Gold transformation for {source} - Implementation pending")
    # This would be implemented similar to silver transformations
    # but reading from silver layer and writing to gold layer


@cli.command()
@click.option('--source', required=True, help='Source configuration name')
@click.pass_context
def validate_source_config(ctx, source):
    """Validate source configuration."""
    try:
        config_manager = ConfigManager()
        
        # Find source configuration
        source_def, source_type = _find_source_config(config_manager, source)
        
        # Basic validation
        required_fields = ['name', 'type', 'connection', 'target']
        missing_fields = [field for field in required_fields if field not in source_def]
        
        if missing_fields:
            click.echo(f"❌ Missing required fields: {missing_fields}", err=True)
            sys.exit(1)
        
        click.echo(f"✅ Source configuration '{source}' is valid")
        
    except Exception as e:
        click.echo(f"❌ Configuration validation failed: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def list_sources(ctx):
    """List all configured data sources."""
    try:
        config_manager = ConfigManager()
        
        source_types = ['excel', 'csv', 'oracle']
        
        for source_type in source_types:
            try:
                config = config_manager.load_source_config(source_type)
                if config:
                    click.echo(f"\n{source_type.upper()} Sources:")
                    for source_name in config.keys():
                        click.echo(f"  - {source_name}")
            except:
                continue
                
    except Exception as e:
        click.echo(f"❌ Error listing sources: {str(e)}", err=True)
        sys.exit(1)


def _find_source_config(config_manager, source_name: str):
    """Find source configuration and type.
    
    Args:
        config_manager: Configuration manager instance
        source_name: Name of the source
        
    Returns:
        Tuple of (source_config, source_type)
    """
    # Try different source types
    for possible_type in ['excel', 'oracle', 'csv', 'database']:
        try:
            type_config = config_manager.load_source_config(possible_type)
            if source_name in type_config:
                source_def = type_config[source_name]
                source_type = _determine_source_type(source_def)
                return source_def, source_type
        except:
            continue
            
    raise ValueError(f"Source '{source_name}' not found in any configuration")


def _determine_source_type(source_config: Dict[str, Any]) -> str:
    """Determine source type from source configuration.
    
    Args:
        source_config: Source configuration dictionary
        
    Returns:
        Source type string
    """
    # First check if type is explicitly defined
    if 'type' in source_config:
        return source_config['type']
    
    # Check connection type
    connection = source_config.get('connection', {})
    
    if 'type' in connection:
        return connection['type']
    
    # Infer from connection properties
    if 'jdbc_url' in connection or 'host' in connection:
        # Database connection
        if 'oracle' in connection.get('jdbc_url', '').lower():
            return 'oracle'
        elif connection.get('driver', '').lower().startswith('oracle'):
            return 'oracle'
        elif 'service_name' in connection or 'sid' in connection:
            return 'oracle'
        elif 'postgresql' in connection.get('jdbc_url', '').lower():
            return 'postgresql'
        elif 'mysql' in connection.get('jdbc_url', '').lower():
            return 'mysql'
        elif 'sqlserver' in connection.get('jdbc_url', '').lower():
            return 'sqlserver'
        else:
            return 'database'  # Generic database
            
    elif 'path' in connection or 'path_pattern' in connection:
        # File-based connection
        path = connection.get('path', connection.get('path_pattern', ''))
        
        if path.lower().endswith(('.xlsx', '.xls')):
            return 'excel'
        elif path.lower().endswith('.csv'):
            return 'csv'
        elif path.lower().endswith('.parquet'):
            return 'parquet'
        elif path.lower().endswith('.json'):
            return 'json'
        else:
            # Check file format if specified
            file_format = connection.get('file_format', '').lower()
            if file_format:
                return file_format
            
            # Default to CSV for unknown file types
            return 'csv'
    
    # Fallback based on source name
    source_name = source_config.get('name', '').lower()
    if 'excel' in source_name:
        return 'excel'
    elif 'csv' in source_name:
        return 'csv'
    elif 'oracle' in source_name:
        return 'oracle'
    
    # Default
    return 'file'


def _run_sql_transformation(spark, config_manager, transform_def):
    """Run SparkSQL transformation."""
    logger.info("Running SparkSQL transformation")
    
    from common_data_platform.transformation.sql_transformer import SQLTransformer
    
    transformer = SQLTransformer(spark, config_manager)
    
    source_table = f"{transform_def['source']['catalog']}.{transform_def['source']['schema']}.{transform_def['source']['table']}"
    target_table = f"{transform_def['target']['catalog']}.{transform_def['target']['schema']}.{transform_def['target']['table']}"
    
    # Build transformation config
    transformation_config = {
        'query': transform_def.get('query', f"SELECT * FROM {source_table}"),
        'write_mode': transform_def.get('write_mode', 'overwrite'),
        'partition_columns': transform_def.get('partition_columns', [])
    }
    
    # If SQL file is specified, load it
    if 'sql_file' in transform_def:
        sql_path = transform_def['sql_file']
        logger.info(f"Loading SQL from file: {sql_path}")
        with open(sql_path, 'r') as f:
            transformation_config['query'] = f.read()
    
    # Execute transformation
    success = transformer.transform(source_table, target_table, transformation_config)
    
    if not success:
        raise Exception(f"SQL transformation failed for {target_table}")


def _run_pyspark_transformation(spark, config_manager, transform_def):
    """Run PySpark transformation."""
    logger.info("Running PySpark transformation")
    
    from common_data_platform.transformation.pyspark_transformer import PySparkTransformer
    
    transformer = PySparkTransformer(spark, config_manager)
    
    source_table = f"{transform_def['source']['catalog']}.{transform_def['source']['schema']}.{transform_def['source']['table']}"
    target_table = f"{transform_def['target']['catalog']}.{transform_def['target']['schema']}.{transform_def['target']['table']}"
    
    # Build transformation config
    transformation_config = {
        'transform_type': transform_def.get('transform_type', 'custom'),
        'write_mode': transform_def.get('write_mode', 'overwrite'),
        'partition_columns': transform_def.get('partition_columns', [])
    }
    
    # Add type-specific configuration
    if transformation_config['transform_type'] == 'custom':
        if 'function_path' in transform_def:
            transformation_config['function_path'] = transform_def['function_path']
            transformation_config['function_name'] = transform_def.get('function_name', 'transform')
        elif 'transform_code' in transform_def:
            transformation_config['transform_code'] = transform_def['transform_code']
    elif transformation_config['transform_type'] == 'aggregation':
        transformation_config['group_by'] = transform_def.get('group_by', [])
        transformation_config['aggregations'] = transform_def.get('aggregations', {})
    elif transformation_config['transform_type'] == 'filter':
        transformation_config['filter_condition'] = transform_def.get('filter_condition')
    elif transformation_config['transform_type'] == 'join':
        transformation_config['join_table'] = transform_def.get('join_table')
        transformation_config['join_keys'] = transform_def.get('join_keys', [])
        transformation_config['join_type'] = transform_def.get('join_type', 'inner')
    
    # Execute transformation
    success = transformer.transform(source_table, target_table, transformation_config)
    
    if not success:
        raise Exception(f"PySpark transformation failed for {target_table}")


if __name__ == '__main__':
    cli()