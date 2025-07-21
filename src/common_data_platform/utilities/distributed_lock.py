"""Distributed locking mechanism for concurrent file processing in Databricks."""

import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from .error_handler import DataPipelineError

logger = logging.getLogger(__name__)


class DistributedLock:
    """Distributed lock implementation using Delta Lake for coordination."""
    
    def __init__(self, spark: SparkSession, lock_table: str):
        """Initialize distributed lock.
        
        Args:
            spark: SparkSession instance
            lock_table: Fully qualified lock table name (catalog.schema.table)
        """
        self.spark = spark
        self.lock_table = lock_table
        self.instance_id = str(uuid.uuid4())
        self._ensure_lock_table_exists()
        
    def _ensure_lock_table_exists(self):
        """Ensure the lock table exists with proper schema."""
        try:
            # Check if table exists
            if not self.spark.catalog.tableExists(self.lock_table):
                logger.info(f"Creating lock table: {self.lock_table}")
                
                # Create lock table with ACID properties
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.lock_table} (
                    resource_id STRING NOT NULL,
                    lock_id STRING NOT NULL,
                    instance_id STRING NOT NULL,
                    acquired_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    metadata MAP<STRING, STRING>
                ) USING DELTA
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
                """
                
                self.spark.sql(create_sql)
                
                # Create index on resource_id for faster lookups
                self.spark.sql(f"""
                    ALTER TABLE {self.lock_table} 
                    SET TBLPROPERTIES ('delta.dataSkippingIndexColumns' = 'resource_id')
                """)
                
        except Exception as e:
            raise DataPipelineError(
                f"Failed to create lock table {self.lock_table}",
                context={"error": str(e)}
            )
            
    def acquire_lock(self, resource_id: str, 
                    timeout_seconds: int = 300,
                    retry_interval: float = 1.0,
                    metadata: Optional[Dict[str, str]] = None) -> str:
        """Acquire a distributed lock on a resource.
        
        Args:
            resource_id: Unique identifier for the resource to lock
            timeout_seconds: Lock timeout in seconds
            retry_interval: Seconds to wait between retries
            metadata: Optional metadata to store with lock
            
        Returns:
            str: Lock ID if acquired
            
        Raises:
            DataPipelineError: If lock cannot be acquired
        """
        lock_id = str(uuid.uuid4())
        max_attempts = int(timeout_seconds / retry_interval)
        
        for attempt in range(max_attempts):
            try:
                # Clean up expired locks first
                self._cleanup_expired_locks()
                
                # Check if resource is already locked
                active_locks = self.spark.sql(f"""
                    SELECT lock_id, instance_id, expires_at 
                    FROM {self.lock_table}
                    WHERE resource_id = '{resource_id}'
                    AND expires_at > current_timestamp()
                """).collect()
                
                if not active_locks:
                    # Try to acquire lock
                    expires_at = datetime.now() + timedelta(seconds=timeout_seconds)
                    
                    # Prepare metadata
                    metadata_map = metadata or {}
                    metadata_map['hostname'] = self.spark.sparkContext.applicationId
                    
                    # Insert lock record
                    lock_df = self.spark.createDataFrame([{
                        'resource_id': resource_id,
                        'lock_id': lock_id,
                        'instance_id': self.instance_id,
                        'acquired_at': datetime.now(),
                        'expires_at': expires_at,
                        'metadata': metadata_map
                    }])
                    
                    lock_df.write.mode("append").saveAsTable(self.lock_table)
                    
                    # Verify lock was acquired (handle race conditions)
                    verification = self.spark.sql(f"""
                        SELECT lock_id 
                        FROM {self.lock_table}
                        WHERE resource_id = '{resource_id}'
                        AND lock_id = '{lock_id}'
                        AND expires_at > current_timestamp()
                    """).collect()
                    
                    if verification:
                        logger.info(f"Acquired lock {lock_id} on resource {resource_id}")
                        return lock_id
                    else:
                        logger.warning(f"Failed to verify lock acquisition, retrying...")
                        
                else:
                    # Resource is locked
                    lock_info = active_locks[0]
                    logger.debug(f"Resource {resource_id} is locked by {lock_info['instance_id']}, "
                               f"expires at {lock_info['expires_at']}")
                    
                # Wait before retry
                if attempt < max_attempts - 1:
                    time.sleep(retry_interval)
                    
            except Exception as e:
                logger.error(f"Error acquiring lock: {str(e)}")
                if attempt == max_attempts - 1:
                    raise DataPipelineError(
                        f"Failed to acquire lock on resource {resource_id}",
                        context={"resource_id": resource_id, "error": str(e)}
                    )
                time.sleep(retry_interval)
                
        raise DataPipelineError(
            f"Timeout acquiring lock on resource {resource_id}",
            context={"resource_id": resource_id, "timeout_seconds": timeout_seconds}
        )
        
    def release_lock(self, resource_id: str, lock_id: str) -> bool:
        """Release a distributed lock.
        
        Args:
            resource_id: Resource identifier
            lock_id: Lock ID to release
            
        Returns:
            bool: True if lock was released
        """
        try:
            # Delete lock record
            self.spark.sql(f"""
                DELETE FROM {self.lock_table}
                WHERE resource_id = '{resource_id}'
                AND lock_id = '{lock_id}'
                AND instance_id = '{self.instance_id}'
            """)
            
            logger.info(f"Released lock {lock_id} on resource {resource_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error releasing lock: {str(e)}")
            return False
            
    def extend_lock(self, resource_id: str, lock_id: str, 
                   additional_seconds: int = 300) -> bool:
        """Extend the expiration time of an existing lock.
        
        Args:
            resource_id: Resource identifier
            lock_id: Lock ID to extend
            additional_seconds: Seconds to add to expiration
            
        Returns:
            bool: True if lock was extended
        """
        try:
            new_expires_at = datetime.now() + timedelta(seconds=additional_seconds)
            
            # Update expiration time
            self.spark.sql(f"""
                UPDATE {self.lock_table}
                SET expires_at = timestamp'{new_expires_at.isoformat()}'
                WHERE resource_id = '{resource_id}'
                AND lock_id = '{lock_id}'
                AND instance_id = '{self.instance_id}'
                AND expires_at > current_timestamp()
            """)
            
            logger.info(f"Extended lock {lock_id} on resource {resource_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error extending lock: {str(e)}")
            return False
            
    def _cleanup_expired_locks(self):
        """Clean up expired locks from the lock table."""
        try:
            deleted_count = self.spark.sql(f"""
                DELETE FROM {self.lock_table}
                WHERE expires_at <= current_timestamp()
            """).collect()[0][0]
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} expired locks")
                
                # Optimize table after cleanup
                self.spark.sql(f"OPTIMIZE {self.lock_table}")
                
        except Exception as e:
            logger.warning(f"Error cleaning up expired locks: {str(e)}")
            
    def with_lock(self, resource_id: str, timeout_seconds: int = 300):
        """Context manager for distributed locking.
        
        Usage:
            with lock.with_lock("my_resource"):
                # Do work while holding lock
                pass
        """
        class LockContext:
            def __init__(self, lock_instance, resource_id, timeout):
                self.lock = lock_instance
                self.resource_id = resource_id
                self.timeout = timeout
                self.lock_id = None
                
            def __enter__(self):
                self.lock_id = self.lock.acquire_lock(self.resource_id, self.timeout)
                return self.lock_id
                
            def __exit__(self, exc_type, exc_val, exc_tb):
                if self.lock_id:
                    self.lock.release_lock(self.resource_id, self.lock_id)
                    
        return LockContext(self, resource_id, timeout_seconds)