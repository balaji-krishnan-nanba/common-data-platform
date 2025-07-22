# Security Architecture

This document outlines the comprehensive security architecture of the Common Data Platform framework, covering authentication, authorization, data protection, and compliance measures.

## Security Overview

The framework implements a defense-in-depth security model with multiple layers:

1. **Authentication & Identity** - Azure AD integration with MFA
2. **Authorization & Access Control** - Role-based access with Unity Catalog
3. **Data Protection** - Encryption at rest and in transit
4. **Network Security** - VNet integration and private endpoints
5. **Audit & Compliance** - Comprehensive logging and monitoring
6. **Secret Management** - Azure Key Vault integration

## Authentication Architecture

### Azure Active Directory Integration

#### Service Principal Authentication
```python
# Automated pipeline authentication
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

# Service principal for pipeline automation
credential = ClientSecretCredential(
    tenant_id=os.environ["AZURE_TENANT_ID"],
    client_id=os.environ["AZURE_CLIENT_ID"], 
    client_secret=os.environ["AZURE_CLIENT_SECRET"]
)

# Key Vault access
vault_url = "https://kv-data-platform-prod.vault.azure.net/"
secret_client = SecretClient(vault_url=vault_url, credential=credential)
```

#### Interactive User Authentication
```python
# Developer/analyst interactive authentication
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential

# Automatic credential chain resolution
credential = DefaultAzureCredential()

# Explicit browser authentication for development
dev_credential = InteractiveBrowserCredential()
```

### Databricks Authentication

#### Personal Access Tokens (Development)
```bash
# Configure Databricks CLI for development
databricks configure --token

# Store token securely in environment
export DATABRICKS_TOKEN="dapi1234567890abcdef"
export DATABRICKS_HOST="https://adb-1234567890123456.14.azuredatabricks.net"
```

#### Service Principal Authentication (Production)
```yaml
# databricks.yml configuration
environments:
  prod:
    mode: production
    auth_type: azure-cli
    azure_client_id: ${AZURE_CLIENT_ID}
    azure_client_secret: ${AZURE_CLIENT_SECRET}
    azure_tenant_id: ${AZURE_TENANT_ID}
```

## Authorization and Access Control

### Role-Based Access Control (RBAC)

#### Predefined Security Roles

| Role | Access Level | Bronze Layer | Silver Layer | Gold Layer | System Tables |
|------|-------------|-------------|-------------|------------|---------------|
| **Data Engineers** | Full Dev/Test<br>Read-only Prod | ✅ Full | ✅ Full | ✅ Full | ✅ Full |
| **Data Analysts** | Read-only All | ❌ No Access | ✅ Read | ✅ Read | ✅ Read |
| **Business Users** | Read-only Gold | ❌ No Access | ❌ No Access | ✅ Read | ❌ No Access |
| **Data Scientists** | Read Silver/Gold | ❌ No Access | ✅ Read | ✅ Read | ✅ Read |
| **Auditors** | Read-only All | ✅ Read | ✅ Read | ✅ Read | ✅ Read |

#### Unity Catalog Permissions
```sql
-- Data Engineering Team
CREATE GROUP IF NOT EXISTS `data-engineers`;
GRANT CREATE CATALOG ON METASTORE TO `data-engineers`;
GRANT USE CATALOG ON CATALOG `cddp-prod-bronze` TO `data-engineers`;
GRANT ALL PRIVILEGES ON CATALOG `cddp-dev-bronze` TO `data-engineers`;

-- Data Analytics Team  
CREATE GROUP IF NOT EXISTS `data-analysts`;
GRANT USE CATALOG ON CATALOG `cddp-prod-silver` TO `data-analysts`;
GRANT USE CATALOG ON CATALOG `cddp-prod-gold` TO `data-analysts`;
GRANT SELECT ON CATALOG `cddp-prod-silver` TO `data-analysts`;
GRANT SELECT ON CATALOG `cddp-prod-gold` TO `data-analysts`;

-- Business Users
CREATE GROUP IF NOT EXISTS `business-users`;
GRANT USE CATALOG ON CATALOG `cddp-prod-gold` TO `business-users`;
GRANT SELECT ON SCHEMA `cddp-prod-gold`.`reporting` TO `business-users`;
GRANT SELECT ON SCHEMA `cddp-prod-gold`.`dashboards` TO `business-users`;

-- Service Principal for Automation
GRANT USE CATALOG ON CATALOG `cddp-prod-bronze` TO `data-platform-sp`;
GRANT ALL PRIVILEGES ON CATALOG `cddp-prod-bronze` TO `data-platform-sp`;
GRANT ALL PRIVILEGES ON CATALOG `cddp-prod-silver` TO `data-platform-sp`;
GRANT ALL PRIVILEGES ON CATALOG `cddp-prod-gold` TO `data-platform-sp`;
```

### Fine-Grained Access Control

#### Column-Level Security
```sql
-- Mask sensitive PII data
CREATE FUNCTION `cddp-prod-silver`.`customer_data`.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_member('pii-full-access') THEN ssn
  WHEN is_member('pii-partial-access') THEN CONCAT('XXX-XX-', RIGHT(ssn, 4))
  ELSE 'XXX-XX-XXXX'
END;

-- Apply masking to customer table
ALTER TABLE `cddp-prod-silver`.`customer_data`.`dim_customers_scd2`
SET COLUMN ssn MASK `cddp-prod-silver`.`customer_data`.mask_ssn;

-- Email masking for privacy
CREATE FUNCTION `cddp-prod-silver`.`customer_data`.mask_email(email STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_member('pii-full-access') THEN email
  ELSE REGEXP_REPLACE(email, '^(.{2}).*@(.*)$', '$1***@$2')
END;
```

#### Row-Level Security
```sql
-- Multi-tenant data access control
CREATE FUNCTION `cddp-prod-gold`.`analytics`.tenant_filter()
RETURNS STRING
RETURN CASE 
  WHEN is_member('tenant-a-users') THEN "tenant_id = 'TENANT_A'"
  WHEN is_member('tenant-b-users') THEN "tenant_id = 'TENANT_B'"
  WHEN is_member('global-admins') THEN "1=1"
  ELSE "1=0"
END;

-- Apply row-level security
ALTER TABLE `cddp-prod-gold`.`analytics`.`sales_summary`
SET ROW FILTER `cddp-prod-gold`.`analytics`.tenant_filter() ON;
```

#### Dynamic Access Control
```python
# Python-based dynamic access control
class AccessControlManager:
    def __init__(self, user_identity: str, user_groups: List[str]):
        self.user_identity = user_identity
        self.user_groups = user_groups
    
    def get_accessible_catalogs(self) -> List[str]:
        """Return catalogs accessible to user."""
        catalogs = []
        
        if 'data-engineers' in self.user_groups:
            catalogs.extend(['cddp-dev-bronze', 'cddp-dev-silver', 'cddp-dev-gold'])
            
        if 'data-analysts' in self.user_groups:
            catalogs.extend(['cddp-prod-silver', 'cddp-prod-gold'])
            
        if 'business-users' in self.user_groups:
            catalogs.append('cddp-prod-gold')
            
        return list(set(catalogs))
    
    def can_access_pii(self) -> bool:
        """Check if user can access PII data."""
        return any(group in ['pii-full-access', 'data-engineers'] 
                  for group in self.user_groups)
```

## Data Protection

### Encryption Strategy

#### Encryption at Rest
```yaml
# Azure Storage encryption configuration
storage_encryption:
  customer_managed_keys:
    enabled: true
    key_vault_url: "https://kv-data-platform-prod.vault.azure.net/"
    key_name: "storage-encryption-key"
    
  service_encryption:
    blob_encryption: true
    file_encryption: true
    queue_encryption: true
    table_encryption: true
```

#### Encryption in Transit
```python
# Force HTTPS/TLS for all connections
import ssl
from azure.storage.blob import BlobServiceClient

# Ensure TLS 1.2 minimum
ssl_context = ssl.create_default_context()
ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

# Azure Storage with encryption in transit
blob_client = BlobServiceClient(
    account_url="https://stadatplatform.blob.core.windows.net",
    credential=credential,
    connection_timeout=300,
    read_timeout=300
)
```

#### Delta Lake Encryption
```sql
-- Enable encryption for Delta tables
CREATE TABLE `cddp-prod-silver`.`customer_data`.`dim_customers_encrypted` (
  customer_id STRING,
  encrypted_ssn STRING,
  encrypted_email STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.encryption.enabled' = 'true',
  'delta.encryption.algorithm' = 'AES-256-GCM'
);
```

### Data Classification and Tagging

#### Automated Data Classification
```python
class DataClassifier:
    def __init__(self):
        self.pii_patterns = {
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'\b\d{3}-\d{3}-\d{4}\b',
            'credit_card': r'\b\d{4}-\d{4}-\d{4}-\d{4}\b'
        }
    
    def classify_column(self, column_name: str, sample_data: List[str]) -> str:
        """Classify column as PII, PHI, or public."""
        column_lower = column_name.lower()
        
        # Check for obvious PII column names
        if any(pii_term in column_lower for pii_term in 
               ['ssn', 'social', 'email', 'phone', 'address']):
            return 'PII'
            
        # Check data patterns
        for pii_type, pattern in self.pii_patterns.items():
            if any(re.match(pattern, str(value)) for value in sample_data):
                return 'PII'
                
        return 'PUBLIC'
    
    def apply_tags(self, catalog: str, schema: str, table: str, 
                   column_classifications: Dict[str, str]):
        """Apply security tags to table and columns."""
        for column, classification in column_classifications.items():
            if classification == 'PII':
                # Apply PII protection
                self.apply_column_mask(catalog, schema, table, column)
```

#### Security Tags Implementation
```sql
-- Tag tables with security classifications
ALTER TABLE `cddp-prod-silver`.`customer_data`.`dim_customers_scd2`
SET TAGS (
  'data_classification' = 'PII',
  'compliance_framework' = 'GDPR,CCPA',
  'retention_period' = '7_years',
  'encryption_required' = 'true'
);

-- Tag columns with specific classifications
ALTER TABLE `cddp-prod-silver`.`customer_data`.`dim_customers_scd2`
ALTER COLUMN ssn SET TAGS ('pii_type' = 'government_id', 'mask_required' = 'true');
ALTER COLUMN email SET TAGS ('pii_type' = 'contact_info', 'mask_required' = 'true');
```

## Network Security

### Virtual Network Integration

#### Databricks VNet Injection
```json
{
  "vnetConfiguration": {
    "vnetId": "/subscriptions/{subscription-id}/resourceGroups/rg-data-platform/providers/Microsoft.Network/virtualNetworks/vnet-databricks",
    "privateSubnetName": "private-subnet",
    "publicSubnetName": "public-subnet",
    "enableNoPublicIp": true
  }
}
```

#### Private Endpoints
```yaml
# Private endpoint configuration
private_endpoints:
  storage_account:
    service_name: "stadatplatform.privatelink.blob.core.windows.net"
    subnet_id: "/subscriptions/{sub}/resourceGroups/rg-data-platform/providers/Microsoft.Network/virtualNetworks/vnet-data/subnets/private-endpoints"
    
  key_vault:
    service_name: "kv-data-platform.privatelink.vaultcore.azure.net" 
    subnet_id: "/subscriptions/{sub}/resourceGroups/rg-data-platform/providers/Microsoft.Network/virtualNetworks/vnet-data/subnets/private-endpoints"
```

### Firewall and Network Security Groups

#### Storage Account Firewall
```bash
# Configure storage account network rules
az storage account network-rule add \
  --account-name stadatplatform \
  --vnet-name vnet-databricks \
  --subnet private-subnet

# Deny public access
az storage account update \
  --name stadatplatform \
  --default-action Deny \
  --bypass AzureServices
```

#### Network Security Group Rules
```json
{
  "securityRules": [
    {
      "name": "AllowDatabricksControlPlane",
      "properties": {
        "protocol": "Tcp",
        "sourcePortRange": "*",
        "destinationPortRange": "443",
        "sourceAddressPrefix": "AzureDatabricks",
        "destinationAddressPrefix": "*",
        "access": "Allow",
        "priority": 100,
        "direction": "Outbound"
      }
    },
    {
      "name": "AllowStorageAccess", 
      "properties": {
        "protocol": "Tcp",
        "sourcePortRange": "*",
        "destinationPortRange": "443",
        "sourceAddressPrefix": "VirtualNetwork",
        "destinationAddressPrefix": "Storage",
        "access": "Allow",
        "priority": 110,
        "direction": "Outbound"
      }
    }
  ]
}
```

## Secret Management

### Azure Key Vault Integration

#### Secret Organization
```
Key Vault: kv-data-platform-{environment}
├── Database Credentials
│   ├── oracle-{env}-username
│   ├── oracle-{env}-password
│   └── oracle-{env}-connection-string
├── Storage Credentials  
│   ├── storage-{env}-account-key
│   └── storage-{env}-sas-token
├── Service Principal Credentials
│   ├── sp-{env}-client-id
│   └── sp-{env}-client-secret
└── API Keys
    ├── external-api-{env}-key
    └── monitoring-{env}-key
```

#### Secret Access Pattern
```python
class SecretManager:
    def __init__(self, spark_session, environment: str):
        self.spark = spark_session
        self.environment = environment
        self.scope = f"databricks-secrets-{environment}"
    
    def get_secret(self, secret_name: str) -> str:
        """Retrieve secret from Databricks secret scope."""
        try:
            return dbutils.secrets.get(scope=self.scope, key=secret_name)
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_name}: {str(e)}")
            raise SecurityError(f"Secret access denied: {secret_name}")
    
    def get_oracle_connection(self) -> Dict[str, str]:
        """Get Oracle connection parameters."""
        return {
            'username': self.get_secret(f'oracle-{self.environment}-username'),
            'password': self.get_secret(f'oracle-{self.environment}-password'),
            'host': self.get_secret(f'oracle-{self.environment}-host'),
            'port': self.get_secret(f'oracle-{self.environment}-port'),
            'service_name': self.get_secret(f'oracle-{self.environment}-service')
        }
```

#### Secret Rotation Strategy
```python
class SecretRotationManager:
    def __init__(self, key_vault_client):
        self.kv_client = key_vault_client
    
    def rotate_secret(self, secret_name: str, new_value: str):
        """Rotate secret with zero-downtime."""
        # Create new version
        self.kv_client.set_secret(secret_name, new_value)
        
        # Update dependent services
        self.update_databricks_scope(secret_name)
        
        # Verify rotation success
        self.verify_secret_rotation(secret_name)
    
    def schedule_rotation(self, secret_name: str, rotation_days: int = 90):
        """Schedule automatic secret rotation."""
        # Implementation would integrate with Azure Logic Apps or Functions
        pass
```

## Compliance and Auditing

### Audit Logging

#### Comprehensive Audit Trail
```python
class AuditLogger:
    def __init__(self, user_identity: str, session_id: str):
        self.user_identity = user_identity
        self.session_id = session_id
        self.audit_table = "cddp-prod-bronze.system.audit_logs"
    
    def log_data_access(self, catalog: str, schema: str, table: str, 
                       operation: str, row_count: Optional[int] = None):
        """Log data access events."""
        audit_record = {
            'timestamp': datetime.utcnow(),
            'user_identity': self.user_identity,
            'session_id': self.session_id,
            'resource_type': 'table',
            'resource_name': f"{catalog}.{schema}.{table}",
            'operation': operation,
            'row_count': row_count,
            'source_ip': self.get_client_ip(),
            'user_agent': self.get_user_agent()
        }
        
        self.write_audit_record(audit_record)
    
    def log_schema_change(self, catalog: str, schema: str, table: str, 
                         change_type: str, change_details: Dict):
        """Log schema modification events."""
        audit_record = {
            'timestamp': datetime.utcnow(),
            'user_identity': self.user_identity,
            'resource_type': 'schema',
            'resource_name': f"{catalog}.{schema}.{table}",
            'operation': change_type,
            'change_details': json.dumps(change_details),
            'change_hash': hashlib.md5(json.dumps(change_details).encode()).hexdigest()
        }
        
        self.write_audit_record(audit_record)
```

#### System Audit Queries
```sql
-- Monitor privileged access
SELECT 
    user_identity,
    resource_name,
    operation,
    COUNT(*) as access_count,
    MIN(timestamp) as first_access,
    MAX(timestamp) as last_access
FROM `cddp-prod-bronze`.`system`.`audit_logs`
WHERE operation IN ('CREATE', 'DROP', 'ALTER')
    AND timestamp >= current_date() - INTERVAL 7 DAYS
GROUP BY user_identity, resource_name, operation
ORDER BY access_count DESC;

-- Detect unusual access patterns
WITH user_baseline AS (
    SELECT 
        user_identity,
        COUNT(DISTINCT resource_name) as avg_tables_accessed,
        COUNT(*) as avg_operations
    FROM `cddp-prod-bronze`.`system`.`audit_logs`
    WHERE timestamp >= current_date() - INTERVAL 30 DAYS
    GROUP BY user_identity
),
recent_activity AS (
    SELECT 
        user_identity,
        COUNT(DISTINCT resource_name) as current_tables_accessed,
        COUNT(*) as current_operations
    FROM `cddp-prod-bronze`.`system`.`audit_logs`
    WHERE timestamp >= current_date() - INTERVAL 1 DAYS
    GROUP BY user_identity
)
SELECT 
    r.user_identity,
    r.current_tables_accessed,
    b.avg_tables_accessed,
    r.current_operations,
    b.avg_operations,
    CASE 
        WHEN r.current_tables_accessed > b.avg_tables_accessed * 3 THEN 'ANOMALY'
        WHEN r.current_operations > b.avg_operations * 5 THEN 'ANOMALY'
        ELSE 'NORMAL'
    END as risk_level
FROM recent_activity r
LEFT JOIN user_baseline b ON r.user_identity = b.user_identity
WHERE r.current_tables_accessed > b.avg_tables_accessed * 2;
```

### Compliance Frameworks

#### GDPR Compliance
```python
class GDPRComplianceManager:
    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
    
    def handle_data_subject_request(self, subject_id: str, request_type: str):
        """Handle GDPR data subject requests."""
        if request_type == 'access':
            return self.extract_personal_data(subject_id)
        elif request_type == 'deletion':
            return self.delete_personal_data(subject_id)
        elif request_type == 'portability':
            return self.export_personal_data(subject_id)
    
    def extract_personal_data(self, subject_id: str) -> Dict:
        """Extract all personal data for a subject."""
        personal_data = {}
        
        # Find all tables with PII tags
        pii_tables = self.catalog_manager.find_tables_with_tag('data_classification', 'PII')
        
        for table in pii_tables:
            # Extract data for the subject
            data = self.catalog_manager.query_table(
                table, f"customer_id = '{subject_id}'"
            )
            personal_data[table] = data
        
        return personal_data
    
    def anonymize_expired_data(self, retention_days: int):
        """Anonymize data beyond retention period."""
        expiry_date = datetime.now() - timedelta(days=retention_days)
        
        # Find tables with retention policies
        tables_with_retention = self.catalog_manager.find_tables_with_tag('retention_period')
        
        for table_info in tables_with_retention:
            self.anonymize_table_data(table_info['table'], expiry_date)
```

#### SOX Compliance
```sql
-- SOX compliance monitoring
CREATE OR REPLACE VIEW sox_sensitive_access AS
SELECT 
    user_identity,
    resource_name,
    operation,
    timestamp,
    CASE 
        WHEN resource_name LIKE '%financial%' OR resource_name LIKE '%revenue%' THEN 'FINANCIAL'
        WHEN resource_name LIKE '%customer%' OR resource_name LIKE '%sales%' THEN 'CUSTOMER'
        ELSE 'OTHER'
    END as data_category
FROM `cddp-prod-bronze`.`system`.`audit_logs`
WHERE operation IN ('SELECT', 'UPDATE', 'DELETE')
    AND (resource_name LIKE '%financial%' 
         OR resource_name LIKE '%revenue%'
         OR resource_name LIKE '%accounting%');

-- Segregation of duties check
CREATE OR REPLACE VIEW sox_segregation_violations AS
WITH user_roles AS (
    SELECT DISTINCT
        user_identity,
        CASE 
            WHEN user_identity IN (SELECT user FROM information_schema.grantees WHERE group = 'financial-users') THEN 'FINANCIAL'
            WHEN user_identity IN (SELECT user FROM information_schema.grantees WHERE group = 'it-admins') THEN 'IT_ADMIN'
            ELSE 'OTHER'
        END as role_type
    FROM `cddp-prod-bronze`.`system`.`audit_logs`
)
SELECT 
    a.user_identity,
    u.role_type,
    a.resource_name,
    a.operation,
    a.timestamp
FROM `cddp-prod-bronze`.`system`.`audit_logs` a
JOIN user_roles u ON a.user_identity = u.user_identity
WHERE u.role_type = 'IT_ADMIN'
    AND a.resource_name LIKE '%financial%'
    AND a.operation IN ('SELECT', 'UPDATE', 'DELETE');
```

## Security Monitoring and Alerting

### Real-time Security Monitoring
```python
class SecurityMonitor:
    def __init__(self, alert_manager):
        self.alert_manager = alert_manager
        
    def monitor_failed_authentications(self):
        """Monitor authentication failures."""
        query = """
        SELECT COUNT(*) as failed_attempts
        FROM system.access.failed_authentications
        WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
        """
        
        failed_count = self.execute_query(query)
        if failed_count > 10:  # Threshold
            self.alert_manager.send_alert(
                'HIGH', 
                f'High number of authentication failures: {failed_count}'
            )
    
    def monitor_privilege_escalation(self):
        """Detect privilege escalation attempts."""
        query = """
        SELECT user_identity, operation, resource_name
        FROM `cddp-prod-bronze`.`system`.`audit_logs`
        WHERE operation = 'GRANT'
            AND timestamp >= current_timestamp() - INTERVAL 1 HOUR
            AND user_identity NOT IN ('admin-user1', 'admin-user2')
        """
        
        escalations = self.execute_query(query)
        if escalations:
            self.alert_manager.send_alert(
                'CRITICAL',
                f'Privilege escalation detected: {escalations}'
            )
```

### Security Metrics Dashboard
```sql
-- Security KPIs for monitoring dashboard
CREATE OR REPLACE VIEW security_metrics AS
SELECT 
    'Authentication Failures' as metric_name,
    COUNT(*) as metric_value,
    'Last 24 Hours' as time_period
FROM system.access.failed_authentications
WHERE timestamp >= current_date() - INTERVAL 1 DAY

UNION ALL

SELECT 
    'PII Access Events',
    COUNT(*),
    'Last 24 Hours'
FROM `cddp-prod-bronze`.`system`.`audit_logs`
WHERE resource_name IN (
    SELECT DISTINCT table_name 
    FROM information_schema.table_tags 
    WHERE tag_name = 'data_classification' AND tag_value = 'PII'
)
AND timestamp >= current_date() - INTERVAL 1 DAY

UNION ALL

SELECT 
    'Unusual Access Patterns',
    COUNT(DISTINCT user_identity),
    'Last 24 Hours'
FROM `cddp-prod-bronze`.`system`.`audit_logs`
WHERE timestamp >= current_date() - INTERVAL 1 DAY
GROUP BY user_identity
HAVING COUNT(DISTINCT resource_name) > 50;
```

This comprehensive security architecture ensures that the Common Data Platform maintains the highest security standards while enabling efficient data operations and analytics capabilities.