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

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from core.config_manager import ConfigManager
from core.catalog_manager import CatalogManager
from core.secret_manager import SecretManager
from ingestion.file_ingester import FileIngester
from connectivity.connector_factory import ConnectorFactory
from utilities.logger import setup_logging, DataPipelineLogger

logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session for pipeline operations."""
    try:
        from pyspark.sql import SparkSession
        
        return SparkSession.builder \
            .appName("CommonDataPlatform-CLI") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
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
@click.option('--source', required=True, help='Source configuration name')
@click.option('--batch-id', help='Optional batch identifier')
@click.option('--write-mode', default='append', 
              type=click.Choice(['append', 'overwrite']),
              help='Write mode for target table')
@click.option('--fail-on-error/--continue-on-error', default=True,
              help='Whether to fail pipeline on individual file errors')
@click.pass_context
def run_bronze_ingestion(ctx, source, batch_id, write_mode, fail_on_error):
    """Run bronze layer ingestion for specified source."""
    pipeline_logger = DataPipelineLogger(__name__)
    
    try:
        pipeline_id = f"bronze_ingestion_{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        pipeline_logger.start_pipeline(pipeline_id)
        
        logger.info(f"Starting bronze ingestion for source: {source}")
        
        # Initialize components
        spark = create_spark_session()
        config_manager = ConfigManager()
        catalog_manager = CatalogManager(spark, config_manager)
        secret_manager = SecretManager(spark)
        
        # Load source configuration
        source_type = _determine_source_type(source)
        source_config = config_manager.load_source_config(source_type)
        
        if source not in source_config:
            raise ValueError(f"Source '{source}' not found in {source_type} configuration")
        
        source_def = source_config[source]
        
        # Prepare target configuration
        target_config = source_def.get('target', {})
        target_config['catalog'] = config_manager.get_catalog_name('bronze')
        
        # Create appropriate ingester
        if source_type in ['excel', 'csv']:
            ingester = FileIngester(spark, config_manager, catalog_manager, secret_manager)
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
        
        # Load transformation configuration
        source_type = _determine_source_type(source)
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
        source_type = _determine_source_type(source)
        source_config = config_manager.load_source_config(source_type)
        
        if source not in source_config:
            click.echo(f"❌ Source '{source}' not found in {source_type} configuration", err=True)
            sys.exit(1)
        
        source_def = source_config[source]
        
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


def _determine_source_type(source_name: str) -> str:
    """Determine source type from source name."""
    # Simple heuristic - in practice, you might want a more robust mapping
    if 'excel' in source_name.lower():
        return 'excel'
    elif 'csv' in source_name.lower():
        return 'csv'
    elif 'oracle' in source_name.lower():
        return 'oracle'
    else:
        # Default to excel for files
        return 'excel'


def _run_sql_transformation(spark, config_manager, transform_def):
    """Run SparkSQL transformation."""
    logger.info("Running SparkSQL transformation")
    
    # This is a simplified implementation
    # In the full version, you would:
    # 1. Load SQL from file
    # 2. Replace parameters
    # 3. Execute the SQL
    # 4. Write results to target
    
    source_table = f"{transform_def['source']['catalog']}.{transform_def['source']['schema']}.{transform_def['source']['table']}"
    target_table = f"{transform_def['target']['catalog']}.{transform_def['target']['schema']}.{transform_def['target']['table']}"
    
    # Simple example transformation
    sql = f"""
    CREATE OR REPLACE TABLE {target_table}
    AS
    SELECT *,
           current_timestamp() as processed_timestamp
    FROM {source_table}
    """
    
    spark.sql(sql)
    logger.info(f"Created table: {target_table}")


def _run_pyspark_transformation(spark, config_manager, transform_def):
    """Run PySpark transformation."""
    logger.info("Running PySpark transformation")
    
    # This is a simplified implementation
    # In the full version, you would dynamically import and execute
    # the specified PySpark transformation module
    
    source_table = f"{transform_def['source']['catalog']}.{transform_def['source']['schema']}.{transform_def['source']['table']}"
    target_table = f"{transform_def['target']['catalog']}.{transform_def['target']['schema']}.{transform_def['target']['table']}"
    
    df = spark.table(source_table)
    
    # Simple example transformation
    from pyspark.sql.functions import current_timestamp
    result_df = df.withColumn("processed_timestamp", current_timestamp())
    
    result_df.write.mode("overwrite").saveAsTable(target_table)
    logger.info(f"Created table: {target_table}")


if __name__ == '__main__':
    cli()