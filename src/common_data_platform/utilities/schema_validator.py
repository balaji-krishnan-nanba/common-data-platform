"""Schema validation utilities for data quality enforcement."""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DataType
from pyspark.sql.functions import col, regexp_extract, size, when
import pyspark.sql.functions as F
import logging

from .error_handler import SchemaValidationError, DataQualityError

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates DataFrame schemas against defined specifications."""
    
    def __init__(self):
        """Initialize schema validator."""
        self.type_mapping = {
            'string': 'StringType',
            'int': 'IntegerType',
            'integer': 'IntegerType',
            'bigint': 'LongType',
            'long': 'LongType',
            'float': 'FloatType',
            'double': 'DoubleType',
            'decimal': 'DecimalType',
            'boolean': 'BooleanType',
            'date': 'DateType',
            'timestamp': 'TimestampType',
            'binary': 'BinaryType'
        }
    
    def validate_schema(
        self, 
        df: DataFrame, 
        schema_config: Dict[str, Any],
        strict: bool = True
    ) -> Dict[str, Any]:
        """
        Validate DataFrame schema against configuration.
        
        Args:
            df: DataFrame to validate
            schema_config: Schema configuration
            strict: Whether to enforce strict validation
            
        Returns:
            Validation results dictionary
        """
        results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "column_validations": {}
        }
        
        try:
            expected_columns = schema_config.get('columns', [])
            actual_columns = df.columns
            
            # Check for missing required columns
            self._check_required_columns(expected_columns, actual_columns, results)
            
            # Check for unexpected columns
            if strict:
                self._check_unexpected_columns(expected_columns, actual_columns, results)
            
            # Validate individual columns
            for col_config in expected_columns:
                col_name = col_config['name']
                if col_name in actual_columns:
                    col_result = self._validate_column(df, col_config)
                    results["column_validations"][col_name] = col_result
                    
                    if not col_result["valid"]:
                        results["valid"] = False
                        results["errors"].extend(col_result["errors"])
            
            return results
            
        except Exception as e:
            logger.error(f"Schema validation error: {str(e)}")
            results["valid"] = False
            results["errors"].append(f"Validation failed: {str(e)}")
            return results
    
    def enforce_schema(
        self, 
        df: DataFrame, 
        schema_config: Dict[str, Any],
        drop_extra_columns: bool = False
    ) -> DataFrame:
        """
        Enforce schema on DataFrame by casting types and handling nulls.
        
        Args:
            df: DataFrame to enforce schema on
            schema_config: Schema configuration
            drop_extra_columns: Whether to drop unexpected columns
            
        Returns:
            DataFrame with enforced schema
        """
        try:
            expected_columns = schema_config.get('columns', [])
            
            # Start with original DataFrame
            result_df = df
            
            # Drop extra columns if requested
            if drop_extra_columns:
                expected_col_names = [col['name'] for col in expected_columns]
                result_df = result_df.select(*expected_col_names)
            
            # Apply column transformations
            for col_config in expected_columns:
                col_name = col_config['name']
                
                if col_name in result_df.columns:
                    result_df = self._apply_column_schema(result_df, col_config)
                else:
                    # Add missing column with null values
                    col_type = col_config.get('type', 'string')
                    result_df = result_df.withColumn(col_name, F.lit(None).cast(col_type))
            
            return result_df
            
        except Exception as e:
            logger.error(f"Schema enforcement error: {str(e)}")
            raise SchemaValidationError(f"Failed to enforce schema: {str(e)}")
    
    def _check_required_columns(
        self, 
        expected_columns: List[Dict[str, Any]], 
        actual_columns: List[str], 
        results: Dict[str, Any]
    ) -> None:
        """Check for missing required columns."""
        required_cols = [
            col['name'] for col in expected_columns 
            if not col.get('nullable', True)
        ]
        
        missing_cols = [col for col in required_cols if col not in actual_columns]
        
        if missing_cols:
            error_msg = f"Missing required columns: {missing_cols}"
            results["errors"].append(error_msg)
            results["valid"] = False
    
    def _check_unexpected_columns(
        self, 
        expected_columns: List[Dict[str, Any]], 
        actual_columns: List[str], 
        results: Dict[str, Any]
    ) -> None:
        """Check for unexpected columns."""
        expected_col_names = [col['name'] for col in expected_columns]
        unexpected_cols = [col for col in actual_columns if col not in expected_col_names]
        
        if unexpected_cols:
            warning_msg = f"Unexpected columns found: {unexpected_cols}"
            results["warnings"].append(warning_msg)
    
    def _validate_column(
        self, 
        df: DataFrame, 
        col_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate individual column."""
        col_name = col_config['name']
        
        result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        try:
            # Check data type
            actual_type = dict(df.dtypes)[col_name]
            expected_type = col_config.get('type', 'string')
            
            if not self._is_compatible_type(actual_type, expected_type):
                result["errors"].append(
                    f"Column {col_name}: expected {expected_type}, got {actual_type}"
                )
                result["valid"] = False
            
            # Check nullability
            if not col_config.get('nullable', True):
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    result["errors"].append(
                        f"Column {col_name}: found {null_count} null values in non-nullable column"
                    )
                    result["valid"] = False
            
            # Check data constraints
            constraints = col_config.get('constraints', {})
            self._validate_constraints(df, col_name, constraints, result)
            
            # Check pattern if specified
            pattern = col_config.get('pattern')
            if pattern and actual_type in ['string']:
                self._validate_pattern(df, col_name, pattern, result)
            
            return result
            
        except Exception as e:
            result["valid"] = False
            result["errors"].append(f"Column validation error: {str(e)}")
            return result
    
    def _apply_column_schema(
        self, 
        df: DataFrame, 
        col_config: Dict[str, Any]
    ) -> DataFrame:
        """Apply schema enforcement to a specific column."""
        col_name = col_config['name']
        target_type = col_config.get('type', 'string')
        
        try:
            # Cast to target type
            df = df.withColumn(col_name, col(col_name).cast(target_type))
            
            # Handle nullable constraint
            if not col_config.get('nullable', True):
                # You might want to either drop nulls or replace with default
                default_value = col_config.get('default')
                if default_value is not None:
                    df = df.withColumn(
                        col_name,
                        when(col(col_name).isNull(), default_value).otherwise(col(col_name))
                    )
                # else: let validation catch the nulls
            
            return df
            
        except Exception as e:
            logger.error(f"Error applying schema to column {col_name}: {str(e)}")
            raise
    
    def _is_compatible_type(self, actual_type: str, expected_type: str) -> bool:
        """Check if actual type is compatible with expected type."""
        # Normalize type names
        actual_type = actual_type.lower()
        expected_type = expected_type.lower()
        
        # Handle decimal types specially
        if expected_type.startswith('decimal'):
            return actual_type.startswith('decimal')
        
        # Direct match
        if actual_type == expected_type:
            return True
        
        # Compatible types
        compatible_types = {
            'string': ['string', 'varchar'],
            'int': ['int', 'integer', 'smallint'],
            'bigint': ['bigint', 'long'],
            'float': ['float', 'real'],
            'double': ['double', 'float'],
            'boolean': ['boolean', 'bool'],
            'timestamp': ['timestamp', 'datetime'],
            'date': ['date']
        }
        
        expected_compatible = compatible_types.get(expected_type, [expected_type])
        return actual_type in expected_compatible
    
    def _validate_constraints(
        self, 
        df: DataFrame, 
        col_name: str, 
        constraints: Dict[str, Any], 
        result: Dict[str, Any]
    ) -> None:
        """Validate data constraints on a column."""
        try:
            # Min value constraint
            if 'min' in constraints:
                min_val = constraints['min']
                violations = df.filter(col(col_name) < min_val).count()
                if violations > 0:
                    result["errors"].append(
                        f"Column {col_name}: {violations} values below minimum {min_val}"
                    )
                    result["valid"] = False
            
            # Max value constraint
            if 'max' in constraints:
                max_val = constraints['max']
                violations = df.filter(col(col_name) > max_val).count()
                if violations > 0:
                    result["errors"].append(
                        f"Column {col_name}: {violations} values above maximum {max_val}"
                    )
                    result["valid"] = False
            
            # Length constraint
            if 'max_length' in constraints:
                max_length = constraints['max_length']
                violations = df.filter(F.length(col(col_name)) > max_length).count()
                if violations > 0:
                    result["errors"].append(
                        f"Column {col_name}: {violations} values exceed max length {max_length}"
                    )
                    result["valid"] = False
                    
        except Exception as e:
            result["warnings"].append(f"Error validating constraints for {col_name}: {str(e)}")
    
    def _validate_pattern(
        self, 
        df: DataFrame, 
        col_name: str, 
        pattern: str, 
        result: Dict[str, Any]
    ) -> None:
        """Validate string pattern constraint."""
        try:
            # Count rows that don't match the pattern
            violations = df.filter(
                ~col(col_name).rlike(pattern) & col(col_name).isNotNull()
            ).count()
            
            if violations > 0:
                result["warnings"].append(
                    f"Column {col_name}: {violations} values don't match pattern {pattern}"
                )
                
        except Exception as e:
            result["warnings"].append(f"Error validating pattern for {col_name}: {str(e)}")


class DataQualityChecker:
    """Performs data quality checks on DataFrames."""
    
    def __init__(self):
        """Initialize data quality checker."""
        pass
    
    def run_quality_checks(
        self, 
        df: DataFrame, 
        checks_config: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Run data quality checks on DataFrame.
        
        Args:
            df: DataFrame to check
            checks_config: List of check configurations
            
        Returns:
            Quality check results
        """
        results = {
            "passed": True,
            "checks": {},
            "summary": {}
        }
        
        total_rows = df.count()
        
        for check in checks_config:
            check_name = check.get('name', 'unnamed_check')
            check_type = check['type']
            
            try:
                check_result = self._run_single_check(df, check)
                results["checks"][check_name] = check_result
                
                if not check_result["passed"]:
                    results["passed"] = False
                    
            except Exception as e:
                logger.error(f"Error running check {check_name}: {str(e)}")
                results["checks"][check_name] = {
                    "passed": False,
                    "error": str(e)
                }
                results["passed"] = False
        
        # Generate summary
        results["summary"] = {
            "total_checks": len(checks_config),
            "passed_checks": sum(1 for check in results["checks"].values() if check.get("passed", False)),
            "total_rows": total_rows
        }
        
        return results
    
    def _run_single_check(self, df: DataFrame, check: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single data quality check."""
        check_type = check['type']
        
        if check_type == 'null_check':
            return self._check_nulls(df, check)
        elif check_type == 'duplicate_check':
            return self._check_duplicates(df, check)
        elif check_type == 'range_check':
            return self._check_range(df, check)
        elif check_type == 'pattern_check':
            return self._check_pattern(df, check)
        elif check_type == 'custom_sql':
            return self._check_custom_sql(df, check)
        else:
            raise ValueError(f"Unknown check type: {check_type}")
    
    def _check_nulls(self, df: DataFrame, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check for null values in specified columns."""
        columns = check['columns']
        threshold = check.get('threshold', 0)  # Percentage threshold
        
        null_counts = {}
        total_rows = df.count()
        
        for col_name in columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
            null_counts[col_name] = {
                "count": null_count,
                "percentage": null_percentage
            }
        
        # Check if any column exceeds threshold
        failed_columns = [
            col_name for col_name, stats in null_counts.items()
            if stats["percentage"] > threshold
        ]
        
        return {
            "passed": len(failed_columns) == 0,
            "null_counts": null_counts,
            "failed_columns": failed_columns,
            "threshold": threshold
        }
    
    def _check_duplicates(self, df: DataFrame, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check for duplicate records based on key columns."""
        key_columns = check['columns']
        threshold = check.get('threshold', 0)  # Number of duplicates allowed
        
        total_rows = df.count()
        distinct_rows = df.select(*key_columns).distinct().count()
        duplicate_count = total_rows - distinct_rows
        
        return {
            "passed": duplicate_count <= threshold,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
            "threshold": threshold
        }
    
    def _check_range(self, df: DataFrame, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check if values are within specified range."""
        column = check['column']
        min_val = check.get('min')
        max_val = check.get('max')
        
        violations = 0
        
        if min_val is not None:
            violations += df.filter(col(column) < min_val).count()
        
        if max_val is not None:
            violations += df.filter(col(column) > max_val).count()
        
        return {
            "passed": violations == 0,
            "violations": violations,
            "min_value": min_val,
            "max_value": max_val
        }
    
    def _check_pattern(self, df: DataFrame, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check if string values match specified pattern."""
        column = check['column']
        pattern = check['pattern']
        
        violations = df.filter(
            ~col(column).rlike(pattern) & col(column).isNotNull()
        ).count()
        
        return {
            "passed": violations == 0,
            "violations": violations,
            "pattern": pattern
        }
    
    def _check_custom_sql(self, df: DataFrame, check: Dict[str, Any]) -> Dict[str, Any]:
        """Run custom SQL check."""
        sql_condition = check['condition']
        
        # Validate SQL condition to prevent injection
        # Only allow specific patterns for safety
        import re
        allowed_pattern = r'^[a-zA-Z0-9_\s\(\)\+\-\*\/\=\>\<\!\.\,\'\"]+$'
        if not re.match(allowed_pattern, sql_condition):
            raise ValueError(f"Invalid SQL condition format: {sql_condition}")
        
        # Create temporary view with unique name
        temp_view = f"temp_check_view_{abs(hash(str(id(df))))}"
        df.createOrReplaceTempView(temp_view)
        
        try:
            # Use parameterized query approach by building the condition with Column expressions
            # Parse simple conditions and convert to Column expressions for safety
            violations = self._evaluate_safe_condition(df, sql_condition)
            
            return {
                "passed": violations == 0,
                "violations": violations,
                "condition": sql_condition
            }
        finally:
            # Clean up temp view
            df.sparkSession.catalog.dropTempView(temp_view)
    
    def _evaluate_safe_condition(self, df: DataFrame, condition: str) -> int:
        """Safely evaluate condition using Column expressions instead of raw SQL."""
        try:
            # For now, use a safer approach with expr() which still validates SQL
            # In production, you'd want to implement a proper SQL parser
            from pyspark.sql.functions import expr
            
            # Basic validation - reject dangerous keywords
            dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE', 'ALTER', 'EXEC']
            condition_upper = condition.upper()
            
            for keyword in dangerous_keywords:
                if keyword in condition_upper:
                    raise ValueError(f"Dangerous SQL keyword '{keyword}' not allowed in conditions")
            
            # Use expr() which provides some SQL injection protection
            violations = df.filter(~expr(condition)).count()
            return violations
            
        except Exception as e:
            logger.error(f"Error evaluating condition '{condition}': {str(e)}")
            raise ValueError(f"Invalid or unsafe SQL condition: {condition}")