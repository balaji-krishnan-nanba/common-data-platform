"""Batch processing utilities for handling large collections of files."""

import logging
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from .error_handler import DataPipelineError, ErrorHandler

logger = logging.getLogger(__name__)

T = TypeVar('T')


class BatchProcessor:
    """Processor for handling data in batches to prevent memory issues."""
    
    def __init__(self, spark: SparkSession, batch_size: int = 100, 
                 memory_fraction: float = 0.6):
        """Initialize batch processor.
        
        Args:
            spark: SparkSession instance
            batch_size: Number of items to process in each batch
            memory_fraction: Fraction of memory to use before triggering batch processing
        """
        self.spark = spark
        self.batch_size = batch_size
        self.memory_fraction = memory_fraction
        self.error_handler = ErrorHandler(raise_on_error=False)
        
    def process_files_in_batches(self, 
                                files: List[str],
                                read_func: Callable[[str], DataFrame],
                                transform_func: Optional[Callable[[DataFrame], DataFrame]] = None,
                                fail_on_error: bool = True) -> Iterator[DataFrame]:
        """Process files in batches to avoid memory issues.
        
        Args:
            files: List of file paths to process
            read_func: Function to read a single file
            transform_func: Optional transformation function
            fail_on_error: Whether to fail on first error
            
        Yields:
            DataFrame: Batch DataFrame containing processed files
        """
        total_files = len(files)
        processed_count = 0
        
        for i in range(0, total_files, self.batch_size):
            batch_files = files[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_files + self.batch_size - 1) // self.batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} "
                       f"({len(batch_files)} files)")
            
            batch_dfs = []
            batch_errors = []
            
            for file_path in batch_files:
                try:
                    # Read file
                    df = read_func(file_path)
                    
                    # Apply transformation if provided
                    if transform_func:
                        df = transform_func(df)
                        
                    # Add file source column
                    df = df.withColumn("_source_file", lit(file_path))
                    
                    batch_dfs.append(df)
                    processed_count += 1
                    
                except Exception as e:
                    error_msg = f"Error processing file {file_path}: {str(e)}"
                    logger.error(error_msg)
                    batch_errors.append((file_path, e))
                    
                    if fail_on_error:
                        raise DataPipelineError(
                            f"Failed to process file {file_path}",
                            context={"file": file_path, "error": str(e)}
                        )
            
            # Union batch DataFrames if any were successful
            if batch_dfs:
                batch_df = batch_dfs[0]
                for df in batch_dfs[1:]:
                    batch_df = batch_df.unionByName(df, allowMissingColumns=True)
                    
                logger.info(f"Batch {batch_num} completed: {len(batch_dfs)} files processed")
                
                # Report any errors
                if batch_errors:
                    logger.warning(f"Batch {batch_num} had {len(batch_errors)} errors")
                    
                yield batch_df
            elif batch_errors and not fail_on_error:
                logger.warning(f"Batch {batch_num} failed completely, skipping")
                continue
            else:
                raise DataPipelineError(
                    f"No files processed successfully in batch {batch_num}",
                    context={"batch_num": batch_num, "errors": batch_errors}
                )
                
    def write_batches_to_target(self,
                               batch_iterator: Iterator[DataFrame],
                               target_table: str,
                               write_mode: str = "append",
                               partition_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Write batches to target table with progress tracking.
        
        Args:
            batch_iterator: Iterator of batch DataFrames
            target_table: Target table name
            write_mode: Write mode (append/overwrite)
            partition_columns: Optional partition columns
            
        Returns:
            Dict with write statistics
        """
        start_time = datetime.now()
        total_records = 0
        batch_count = 0
        
        try:
            for batch_num, batch_df in enumerate(batch_iterator, 1):
                batch_start = datetime.now()
                
                # Cache batch for multiple operations
                batch_df.cache()
                
                # Get batch size
                batch_records = batch_df.count()
                total_records += batch_records
                
                # Write batch
                writer = batch_df.write
                
                # Use overwrite for first batch if mode is overwrite
                if batch_num == 1 and write_mode == "overwrite":
                    writer = writer.mode("overwrite")
                else:
                    writer = writer.mode("append")
                    
                if partition_columns:
                    writer = writer.partitionBy(*partition_columns)
                    
                writer.saveAsTable(target_table)
                
                # Unpersist after writing
                batch_df.unpersist()
                
                batch_duration = (datetime.now() - batch_start).total_seconds()
                logger.info(f"Batch {batch_num} written: {batch_records} records "
                           f"in {batch_duration:.2f} seconds")
                
                batch_count = batch_num
                
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return {
                "status": "success",
                "batches_processed": batch_count,
                "total_records": total_records,
                "duration_seconds": duration,
                "records_per_second": total_records / duration if duration > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error writing batches to target: {str(e)}")
            raise DataPipelineError(
                f"Failed to write batches to {target_table}",
                context={
                    "target_table": target_table,
                    "batches_processed": batch_count,
                    "records_processed": total_records,
                    "error": str(e)
                }
            )
            
    def create_chunked_iterator(self, items: List[T], chunk_size: Optional[int] = None) -> Iterator[List[T]]:
        """Create an iterator that yields chunks of items.
        
        Args:
            items: List of items to chunk
            chunk_size: Size of each chunk (defaults to batch_size)
            
        Yields:
            List[T]: Chunk of items
        """
        chunk_size = chunk_size or self.batch_size
        
        for i in range(0, len(items), chunk_size):
            yield items[i:i + chunk_size]