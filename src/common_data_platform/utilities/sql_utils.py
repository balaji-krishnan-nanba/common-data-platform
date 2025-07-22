"""SQL utilities for safe query construction and parameter handling."""

import re
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


class SQLSanitizer:
    """Utility class for SQL query sanitization and safe parameter handling."""
    
    # Pattern for valid table/column names (alphanumeric, underscore, dot for schema)
    VALID_IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$')
    
    # Pattern for detecting potential SQL injection attempts
    SQL_INJECTION_PATTERNS = [
        re.compile(r'(union|select|insert|update|delete|drop|create|alter|exec|execute|script|javascript)', re.IGNORECASE),
        re.compile(r'(--|/\*|\*/|;|\'|"|\||\\)', re.IGNORECASE),
        re.compile(r'(xp_|sp_|0x|hex|char|nchar|varchar|nvarchar|cast|convert)', re.IGNORECASE)
    ]
    
    @staticmethod
    def validate_identifier(identifier: str, identifier_type: str = "identifier") -> str:
        """Validate and sanitize SQL identifiers (table names, column names).
        
        Args:
            identifier: The identifier to validate
            identifier_type: Type of identifier for error messages
            
        Returns:
            Sanitized identifier
            
        Raises:
            ValueError: If identifier is invalid
        """
        if not identifier:
            raise ValueError(f"Empty {identifier_type} provided")
            
        # Remove surrounding whitespace
        identifier = identifier.strip()
        
        # Check against valid pattern
        if not SQLSanitizer.VALID_IDENTIFIER_PATTERN.match(identifier):
            raise ValueError(
                f"Invalid {identifier_type}: '{identifier}'. "
                f"Must contain only alphanumeric characters, underscores, and dots for schema qualification."
            )
            
        # Check for SQL injection patterns
        for pattern in SQLSanitizer.SQL_INJECTION_PATTERNS:
            if pattern.search(identifier):
                raise ValueError(
                    f"Potential SQL injection detected in {identifier_type}: '{identifier}'"
                )
                
        return identifier
    
    @staticmethod
    def validate_table_name(table_name: str) -> str:
        """Validate a table name (can include catalog.schema.table).
        
        Args:
            table_name: The table name to validate
            
        Returns:
            Validated table name
            
        Raises:
            ValueError: If table name is invalid
        """
        parts = table_name.split('.')
        if len(parts) > 3:
            raise ValueError(
                f"Invalid table name: '{table_name}'. "
                "Maximum 3 parts allowed (catalog.schema.table)"
            )
            
        # Validate each part
        validated_parts = []
        for part in parts:
            validated_parts.append(
                SQLSanitizer.validate_identifier(part, "table name component")
            )
            
        return '.'.join(validated_parts)
    
    @staticmethod
    def format_value(value: Any) -> str:
        """Format a value for safe SQL inclusion.
        
        Args:
            value: The value to format
            
        Returns:
            Formatted SQL value string
        """
        if value is None:
            return "NULL"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (datetime, date)):
            # Format as ISO string
            return f"'{value.isoformat()}'"
        elif isinstance(value, str):
            # Escape single quotes and wrap in quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        else:
            # Convert to string and escape
            str_value = str(value).replace("'", "''")
            return f"'{str_value}'"
    
    @staticmethod
    def build_where_clause(conditions: Dict[str, Any]) -> str:
        """Build a safe WHERE clause from conditions.
        
        Args:
            conditions: Dictionary of column:value pairs
            
        Returns:
            WHERE clause string (without 'WHERE' keyword)
        """
        if not conditions:
            return "1=1"
            
        clauses = []
        for column, value in conditions.items():
            # Validate column name
            safe_column = SQLSanitizer.validate_identifier(column, "column name")
            
            if value is None:
                clauses.append(f"{safe_column} IS NULL")
            elif isinstance(value, list):
                # Handle IN clause
                if not value:
                    clauses.append("1=0")  # Empty list means no match
                else:
                    formatted_values = [SQLSanitizer.format_value(v) for v in value]
                    clauses.append(f"{safe_column} IN ({', '.join(formatted_values)})")
            else:
                formatted_value = SQLSanitizer.format_value(value)
                clauses.append(f"{safe_column} = {formatted_value}")
                
        return " AND ".join(clauses)
    
    @staticmethod
    def build_safe_query(base_query: str, parameters: Dict[str, Any]) -> str:
        """Build a safe query by replacing placeholders with sanitized values.
        
        Args:
            base_query: Query with placeholders like {table_name}, {watermark}
            parameters: Dictionary of parameter values
            
        Returns:
            Safe query string
        """
        safe_query = base_query
        
        for param_name, param_value in parameters.items():
            placeholder = f"{{{param_name}}}"
            
            if placeholder not in safe_query:
                continue
                
            # Handle different parameter types
            if param_name.endswith(('_table', '_name')) or param_name in ['table', 'schema', 'catalog']:
                # Treat as identifier
                safe_value = SQLSanitizer.validate_table_name(str(param_value))
            elif param_name.endswith('_column') or param_name == 'column':
                # Treat as column name
                safe_value = SQLSanitizer.validate_identifier(str(param_value), "column")
            else:
                # Treat as value
                safe_value = SQLSanitizer.format_value(param_value)
                
            safe_query = safe_query.replace(placeholder, safe_value)
            
        # Check for any remaining placeholders
        remaining_placeholders = re.findall(r'\{[^}]+\}', safe_query)
        if remaining_placeholders:
            logger.warning(f"Unresolved placeholders in query: {remaining_placeholders}")
            
        return safe_query


class SparkSQLBuilder:
    """Builder for Spark SQL queries with safety features."""
    
    def __init__(self):
        """Initialize the SQL builder."""
        self.sanitizer = SQLSanitizer()
        
    def build_jdbc_query(self, table_name: str, 
                        conditions: Optional[Dict[str, Any]] = None,
                        columns: Optional[List[str]] = None) -> str:
        """Build a JDBC subquery for Spark.
        
        Args:
            table_name: The table name
            conditions: Optional WHERE conditions
            columns: Optional column list (default: *)
            
        Returns:
            JDBC subquery string
        """
        # Validate table name
        safe_table = self.sanitizer.validate_table_name(table_name)
        
        # Build column list
        if columns:
            safe_columns = [
                self.sanitizer.validate_identifier(col, "column") 
                for col in columns
            ]
            column_list = ", ".join(safe_columns)
        else:
            column_list = "*"
            
        # Build base query
        query = f"SELECT {column_list} FROM {safe_table}"
        
        # Add WHERE clause if conditions exist
        if conditions:
            where_clause = self.sanitizer.build_where_clause(conditions)
            query = f"{query} WHERE {where_clause}"
            
        # Wrap for JDBC
        return f"({query}) AS t"
    
    def build_incremental_query(self, table_name: str,
                              watermark_column: str,
                              last_watermark: Any,
                              additional_conditions: Optional[Dict[str, Any]] = None) -> str:
        """Build an incremental load query.
        
        Args:
            table_name: The table name
            watermark_column: The watermark column name
            last_watermark: The last watermark value
            additional_conditions: Optional additional WHERE conditions
            
        Returns:
            Incremental query string
        """
        # Start with watermark condition
        conditions = {watermark_column: None}  # Placeholder
        
        # Add additional conditions
        if additional_conditions:
            conditions.update(additional_conditions)
            
        # Build base query
        safe_table = self.sanitizer.validate_table_name(table_name)
        safe_watermark_col = self.sanitizer.validate_identifier(watermark_column, "watermark column")
        
        # Build WHERE clause manually for > comparison
        where_parts = []
        
        # Watermark condition
        if last_watermark is not None:
            formatted_watermark = self.sanitizer.format_value(last_watermark)
            where_parts.append(f"{safe_watermark_col} > {formatted_watermark}")
        
        # Additional conditions
        if additional_conditions:
            for col, val in additional_conditions.items():
                if col != watermark_column:  # Skip watermark column
                    safe_col = self.sanitizer.validate_identifier(col, "column")
                    formatted_val = self.sanitizer.format_value(val)
                    where_parts.append(f"{safe_col} = {formatted_val}")
        
        # Build final query
        query = f"SELECT * FROM {safe_table}"
        if where_parts:
            query = f"{query} WHERE {' AND '.join(where_parts)}"
            
        return f"({query}) AS t"