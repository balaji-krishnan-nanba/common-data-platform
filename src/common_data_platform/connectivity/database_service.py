"""Database connectivity service for Common Data Platform."""

from typing import Dict, Any, Optional
import logging
from urllib.parse import quote_plus


class DatabaseService:
    """Service for database connectivity."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize database service.
        
        Args:
            config: Database connection configuration
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.db_type = config.get('type', 'oracle').lower()
        
    def get_jdbc_url(self) -> str:
        """Build JDBC URL for database connection.
        
        Returns:
            JDBC connection URL
        """
        if self.db_type == 'oracle':
            return self._build_oracle_jdbc_url()
        elif self.db_type == 'postgresql':
            return self._build_postgresql_jdbc_url()
        elif self.db_type == 'mysql':
            return self._build_mysql_jdbc_url()
        elif self.db_type == 'sqlserver':
            return self._build_sqlserver_jdbc_url()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
            
    def _build_oracle_jdbc_url(self) -> str:
        """Build Oracle JDBC URL.
        
        Returns:
            Oracle JDBC URL
        """
        host = self.config['host']
        port = self.config.get('port', 1521)
        
        if 'service_name' in self.config:
            # Service name format
            return f"jdbc:oracle:thin:@//{host}:{port}/{self.config['service_name']}"
        elif 'sid' in self.config:
            # SID format
            return f"jdbc:oracle:thin:@{host}:{port}:{self.config['sid']}"
        else:
            raise ValueError("Either service_name or sid must be provided for Oracle")
            
    def _build_postgresql_jdbc_url(self) -> str:
        """Build PostgreSQL JDBC URL.
        
        Returns:
            PostgreSQL JDBC URL
        """
        host = self.config['host']
        port = self.config.get('port', 5432)
        database = self.config['database']
        
        url = f"jdbc:postgresql://{host}:{port}/{database}"
        
        # Add SSL if configured
        if self.config.get('ssl_enabled', False):
            url += "?sslmode=require"
            
        return url
        
    def _build_mysql_jdbc_url(self) -> str:
        """Build MySQL JDBC URL.
        
        Returns:
            MySQL JDBC URL
        """
        host = self.config['host']
        port = self.config.get('port', 3306)
        database = self.config['database']
        
        url = f"jdbc:mysql://{host}:{port}/{database}"
        
        # Add common MySQL parameters
        params = []
        if self.config.get('use_ssl', False):
            params.append("useSSL=true")
        if self.config.get('server_timezone'):
            params.append(f"serverTimezone={self.config['server_timezone']}")
            
        if params:
            url += "?" + "&".join(params)
            
        return url
        
    def _build_sqlserver_jdbc_url(self) -> str:
        """Build SQL Server JDBC URL.
        
        Returns:
            SQL Server JDBC URL
        """
        host = self.config['host']
        port = self.config.get('port', 1433)
        database = self.config['database']
        
        url = f"jdbc:sqlserver://{host}:{port};databaseName={database}"
        
        # Add authentication mode
        if self.config.get('integrated_security', False):
            url += ";integratedSecurity=true"
        
        if self.config.get('trust_server_certificate', False):
            url += ";trustServerCertificate=true"
            
        return url
        
    def get_connection_properties(self) -> Dict[str, str]:
        """Get additional connection properties.
        
        Returns:
            Dictionary of connection properties
        """
        props = {
            "user": self.config['user'],
            "password": self.config['password']
        }
        
        # Add database-specific properties
        if self.db_type == 'oracle':
            props["oracle.jdbc.timezoneAsRegion"] = "false"
            
        return props
        
    def test_connection(self) -> bool:
        """Test database connection.
        
        Returns:
            True if connection successful
        """
        try:
            # This would use actual JDBC driver to test
            # For now, just validate configuration
            required_fields = ['host', 'user', 'password']
            
            for field in required_fields:
                if field not in self.config:
                    self.logger.error(f"Missing required field: {field}")
                    return False
                    
            self.logger.info(f"Database configuration validated for {self.db_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False