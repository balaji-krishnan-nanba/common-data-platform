# Troubleshooting Guide

This comprehensive troubleshooting guide helps diagnose and resolve common issues in the Common Data Platform framework across ingestion, transformation, and infrastructure components.

## Quick Diagnostic Commands

### Framework Health Check
```bash
# Complete system health check
python -m src.cli health-check --environment dev --verbose

# Check specific components
python -m src.cli health-check --components [storage,secrets,catalog,clusters]

# Test connectivity to all services
python -m src.cli test-connectivity --all
```

### Pipeline Status Check
```bash
# Check recent pipeline executions
python -m src.cli pipeline-status --last 24h

# Check specific source status
python -m src.cli pipeline-status --source oracle_erp_system

# View failed pipeline details
python -m src.cli pipeline-status --status failed --details
```

## Common Issues and Solutions

### 1. Authentication and Authorization Issues

#### Issue: "Authentication failed" when accessing Azure resources
```
Error: DefaultAzureCredential failed to retrieve a token
```

**Diagnosis:**
```bash
# Check Azure CLI authentication
az account show

# Check current user permissions
az role assignment list --assignee $(az account show --query user.name -o tsv)

# Test Key Vault access
az keyvault secret show --vault-name kv-data-platform-dev --name test-secret
```

**Solutions:**
```bash
# Re-authenticate Azure CLI
az login

# For service principal authentication
az login --service-principal --username $AZURE_CLIENT_ID --password $AZURE_CLIENT_SECRET --tenant $AZURE_TENANT_ID

# Check and fix permissions
az role assignment create \
  --assignee $(az account show --query user.name -o tsv) \
  --role "Key Vault Secrets User" \
  --scope "/subscriptions/{subscription-id}/resourceGroups/rg-data-platform/providers/Microsoft.KeyVault/vaults/kv-data-platform-dev"
```

#### Issue: "Secret scope not found" in Databricks
```
Error: Secret scope 'databricks-secrets-dev' does not exist
```

**Diagnosis:**
```bash
# List available secret scopes
databricks secrets list-scopes

# Check Databricks workspace authentication
databricks workspace list
```

**Solutions:**
```bash
# Recreate secret scope
databricks secrets create-scope \
  --scope databricks-secrets-dev \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "/subscriptions/{subscription-id}/resourceGroups/rg-data-platform/providers/Microsoft.KeyVault/vaults/kv-data-platform-dev" \
  --dns-name "https://kv-data-platform-dev.vault.azure.net/"

# Verify secret scope creation
databricks secrets list --scope databricks-secrets-dev
```

### 2. Unity Catalog Issues

#### Issue: "Catalog not found" error
```
Error: [CATALOG_NOT_FOUND] Catalog 'cddp-dev-bronze' not found
```

**Diagnosis:**
```sql
-- Check available catalogs
SHOW CATALOGS;

-- Check catalog permissions
DESCRIBE CATALOG `cddp-dev-bronze`;

-- Check metastore assignment
SELECT * FROM system.information_schema.metastores;
```

**Solutions:**
```bash
# Recreate missing catalogs
python scripts/provision_infrastructure.py --environments dev --project-code cddp

# Grant catalog permissions
databricks unity-catalog catalogs grant \
  --catalog cddp-dev-bronze \
  --principal $(databricks current-user me --output json | jq -r .userName) \
  --privilege USE_CATALOG
```

#### Issue: "Table not found" in Unity Catalog
```
Error: [TABLE_OR_VIEW_NOT_FOUND] Table 'cddp-dev-bronze.oracle_data.customers' not found
```

**Diagnosis:**
```sql
-- Check if schema exists
USE CATALOG `cddp-dev-bronze`;
SHOW SCHEMAS;

-- Check tables in schema
USE SCHEMA `oracle_data`;
SHOW TABLES;

-- Check table location
DESCRIBE TABLE EXTENDED `cddp-dev-bronze`.`oracle_data`.`customers`;
```

**Solutions:**
```sql
-- Recreate missing schema
CREATE SCHEMA IF NOT EXISTS `cddp-dev-bronze`.`oracle_data`;

-- Recreate table if data exists but table is missing
CREATE TABLE `cddp-dev-bronze`.`oracle_data`.`customers`
USING DELTA
LOCATION 'abfss://processed-data@stadatplatform.dfs.core.windows.net/bronze/oracle_data/customers/';

-- Repair table if location is incorrect
MSCK REPAIR TABLE `cddp-dev-bronze`.`oracle_data`.`customers`;
```

### 3. Data Ingestion Issues

#### Issue: File ingestion fails with schema validation errors
```
Error: Schema validation failed. Column 'customer_id' expected type STRING, found INTEGER
```

**Diagnosis:**
```python
# Check actual file schema
python -m src.cli inspect-file --file-path "sales/2024/01/15/sales_data.xlsx" --show-schema

# Validate source configuration
python -m src.cli validate-source-config --source my_excel_source --verbose
```

**Solutions:**
```yaml
# Update source configuration with correct schema
# config/sources/excel_sources.yaml
schema:
  columns:
    - name: customer_id
      type: string  # Changed from integer to string
      nullable: false
      transformations:
        - type: cast
          target_type: string
```

#### Issue: Oracle connection timeout
```
Error: java.sql.SQLTimeoutException: ORA-01013: user requested cancel of current operation
```

**Diagnosis:**
```python
# Test Oracle connectivity
python -c "
from src.connectivity.oracle_connector import OracleConnector
from src.core.config_manager import ConfigManager

config = ConfigManager()
oracle_config = config.get_source_config('oracle_erp_system')
connector = OracleConnector(oracle_config)
result = connector.test_connection()
print(f'Connection test: {result}')
"

# Check network connectivity
telnet oracle-server.company.com 1521
```

**Solutions:**
```yaml
# Increase timeout settings in oracle_sources.yaml
connection:
  host: oracle-server.company.com
  port: 1521
  service_name: PROD
  connection_timeout: 300  # Increased from default
  socket_timeout: 600     # Added socket timeout

jdbc_options:
  fetchsize: 5000         # Reduced fetch size
  queryTimeout: 1800      # 30 minutes query timeout
```

#### Issue: Large file ingestion fails with memory errors
```
Error: java.lang.OutOfMemoryError: Java heap space
```

**Diagnosis:**
```bash
# Check file size
az storage blob show --account-name stadatplatform --container-name raw-data --name "large_file.xlsx" --query "properties.contentLength"

# Check cluster configuration
databricks clusters get --cluster-id $CLUSTER_ID
```

**Solutions:**
```yaml
# Update cluster configuration for large files
cluster_config:
  driver_node_type: Standard_DS5_v2  # Increased memory
  node_type: Standard_DS4_v2
  num_workers: 8
  spark_conf:
    "spark.driver.memory": "16g"
    "spark.driver.maxResultSize": "8g"
    "spark.sql.adaptive.enabled": "true"
    "spark.sql.adaptive.coalescePartitions.enabled": "true"

# Process large files in chunks
processing:
  chunk_size: 100000  # Process 100k rows at a time
  enable_streaming: true
```

### 4. Transformation Issues

#### Issue: SCD Type 2 merge operation fails
```
Error: [DELTA_CONCURRENT_WRITE] A concurrent write to the Delta table occurred
```

**Diagnosis:**
```sql
-- Check for concurrent operations
DESCRIBE HISTORY `cddp-dev-silver`.`customer_data`.`dim_customers_scd2`;

-- Check table properties
DESCRIBE TABLE EXTENDED `cddp-dev-silver`.`customer_data`.`dim_customers_scd2`;
```

**Solutions:**
```python
# Implement retry logic with exponential backoff
def apply_scd_with_retry(df, config, max_retries=3):
    for attempt in range(max_retries):
        try:
            return apply_scd_type_2(df, config)
        except DeltaConcurrentWriteException:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
            logger.warning(f"Concurrent write detected, retrying attempt {attempt + 1}")

# Enable optimistic concurrency control
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")
spark.conf.set("spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled", "true")
```

#### Issue: Transformation performance is slow
```
Warning: Transformation taking longer than expected (>30 minutes)
```

**Diagnosis:**
```sql
-- Check query execution plan
EXPLAIN EXTENDED 
SELECT * FROM transformation_query;

-- Monitor running queries
SELECT 
  query_id,
  statement_type,
  state,
  total_task_duration_ms,
  rows_read,
  rows_written
FROM system.query.history
WHERE warehouse_id = 'your_warehouse_id'
  AND start_time >= current_timestamp() - INTERVAL 1 HOUR
ORDER BY start_time DESC;
```

**Solutions:**
```python
# Optimize transformation query
# 1. Add partition pruning
df_filtered = df.filter(col("transaction_date") >= "2024-01-01")

# 2. Broadcast small lookup tables
customer_lookup = spark.table("dim_customers").hint("broadcast")

# 3. Repartition before expensive operations
df_repartitioned = df.repartition(200, "customer_id")

# 4. Cache intermediate results
df_intermediate.cache()
df_result = df_intermediate.join(other_df, "key")
df_intermediate.unpersist()  # Clean up cache
```

### 5. Storage and Connectivity Issues

#### Issue: Azure Storage access denied
```
Error: 403 Forbidden. Access denied to storage account
```

**Diagnosis:**
```bash
# Check storage account permissions
az storage account show --name stadatplatform --query "primaryEndpoints"

# Test storage access with SAS token
az storage blob list --account-name stadatplatform --container-name raw-data --auth-mode key

# Check network access rules
az storage account network-rule list --account-name stadatplatform
```

**Solutions:**
```bash
# Add IP/VNet to storage firewall
az storage account network-rule add \
  --account-name stadatplatform \
  --ip-address "your.public.ip.address"

# Update storage account key in Key Vault
NEW_KEY=$(az storage account keys list --account-name stadatplatform --query '[0].value' -o tsv)
az keyvault secret set --vault-name kv-data-platform-dev --name storage-account-key --value "$NEW_KEY"

# Grant Databricks access to storage
az role assignment create \
  --assignee "your-databricks-service-principal" \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/{subscription-id}/resourceGroups/rg-data-platform/providers/Microsoft.Storage/storageAccounts/stadatplatform"
```

#### Issue: Delta table corruption
```
Error: [DELTA_MISSING_TRANSACTION_LOG] Transaction log is missing or corrupted
```

**Diagnosis:**
```python
# Check Delta table status
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "abfss://processed-data@stadatplatform.dfs.core.windows.net/bronze/customers/")
delta_table.history().show()

# Check file system integrity
dbutils.fs.ls("abfss://processed-data@stadatplatform.dfs.core.windows.net/bronze/customers/_delta_log/")
```

**Solutions:**
```python
# Repair Delta table
from delta.tables import DeltaTable

# Option 1: Restore from backup
delta_table.restoreToVersion(previous_version_number)

# Option 2: Repair transaction log
spark.sql("FSCK REPAIR TABLE delta.`abfss://processed-data@stadatplatform.dfs.core.windows.net/bronze/customers/`")

# Option 3: Recreate table from Parquet files (last resort)
df = spark.read.parquet("abfss://processed-data@stadatplatform.dfs.core.windows.net/bronze/customers/")
df.write.format("delta").mode("overwrite").save("abfss://processed-data@stadatplatform.dfs.core.windows.net/bronze/customers_recovered/")
```

### 6. Performance Issues

#### Issue: Cluster startup is slow
```
Warning: Cluster taking >10 minutes to start
```

**Diagnosis:**
```bash
# Check cluster events
databricks clusters events --cluster-id $CLUSTER_ID

# Check cluster configuration
databricks clusters get --cluster-id $CLUSTER_ID --output json | jq '.spark_conf'
```

**Solutions:**
```yaml
# Optimize cluster configuration
cluster_config:
  # Use pool for faster startup
  instance_pool_id: "pool-123456"
  
  # Reduce init script complexity
  # Use bundle artifacts instead of DBFS for libraries
  libraries:
    - whl: "path/to/bundle/artifacts/*.whl"
  
  # Enable fast cluster startup
  enable_elastic_disk: false
  custom_tags:
    "cluster-purpose": "data-platform"
  
  spark_conf:
    # Reduce Spark overhead
    "spark.speculation": "false"
    "spark.dynamicAllocation.enabled": "false"
```

#### Issue: Query performance degradation
```
Warning: Query execution time increased by 200% compared to baseline
```

**Diagnosis:**
```sql
-- Check table statistics
ANALYZE TABLE `cddp-dev-silver`.`sales_data`.`fact_sales` COMPUTE STATISTICS;

-- Check file fragmentation
DESCRIBE DETAIL `cddp-dev-silver`.`sales_data`.`fact_sales`;

-- Monitor query performance
SELECT 
  query_text,
  total_task_duration_ms,
  rows_read,
  bytes_read,
  cached_local_bytes_read
FROM system.query.history
WHERE query_text LIKE '%fact_sales%'
ORDER BY start_time DESC
LIMIT 10;
```

**Solutions:**
```sql
-- Optimize table with Z-ordering
OPTIMIZE `cddp-dev-silver`.`sales_data`.`fact_sales`
ZORDER BY (customer_id, transaction_date);

-- Clean up old versions
VACUUM `cddp-dev-silver`.`sales_data`.`fact_sales` RETAIN 168 HOURS;

-- Update table statistics
ANALYZE TABLE `cddp-dev-silver`.`sales_data`.`fact_sales` COMPUTE STATISTICS FOR ALL COLUMNS;

-- Consider repartitioning large tables
CREATE OR REPLACE TABLE `cddp-dev-silver`.`sales_data`.`fact_sales_optimized`
USING DELTA
PARTITIONED BY (transaction_year, transaction_month)
AS SELECT * FROM `cddp-dev-silver`.`sales_data`.`fact_sales`;
```

## Monitoring and Alerting

### System Health Monitoring
```python
class SystemHealthMonitor:
    def __init__(self):
        self.health_checks = [
            self.check_unity_catalog_access,
            self.check_storage_connectivity,
            self.check_secret_access,
            self.check_cluster_availability,
            self.check_pipeline_health
        ]
    
    def run_health_check(self) -> HealthReport:
        """Run comprehensive system health check."""
        results = []
        
        for check in self.health_checks:
            try:
                start_time = time.time()
                result = check()
                duration = time.time() - start_time
                
                results.append(HealthCheckResult(
                    check_name=check.__name__,
                    status=result.status,
                    message=result.message,
                    duration=duration,
                    details=result.details
                ))
            except Exception as e:
                results.append(HealthCheckResult(
                    check_name=check.__name__,
                    status='FAILED',
                    message=str(e),
                    duration=0,
                    details={'error': str(e)}
                ))
        
        overall_status = 'HEALTHY' if all(r.status == 'HEALTHY' for r in results) else 'UNHEALTHY'
        
        return HealthReport(
            timestamp=datetime.utcnow(),
            overall_status=overall_status,
            check_results=results
        )
    
    def check_unity_catalog_access(self) -> HealthCheckResult:
        """Check Unity Catalog accessibility."""
        try:
            catalogs = spark.sql("SHOW CATALOGS").collect()
            expected_catalogs = ['cddp-dev-bronze', 'cddp-dev-silver', 'cddp-dev-gold']
            
            available_catalogs = [row.catalog for row in catalogs]
            missing_catalogs = [cat for cat in expected_catalogs if cat not in available_catalogs]
            
            if missing_catalogs:
                return HealthCheckResult(
                    status='WARNING',
                    message=f"Missing catalogs: {missing_catalogs}",
                    details={'missing_catalogs': missing_catalogs}
                )
            
            return HealthCheckResult(
                status='HEALTHY',
                message="All Unity Catalog resources accessible",
                details={'available_catalogs': available_catalogs}
            )
        except Exception as e:
            return HealthCheckResult(
                status='FAILED',
                message=f"Unity Catalog access failed: {str(e)}",
                details={'error': str(e)}
            )
```

### Automated Issue Detection
```sql
-- Create automated monitoring views
CREATE OR REPLACE VIEW system_health_summary AS
WITH recent_executions AS (
    SELECT 
        source_name,
        COUNT(*) as total_runs,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
        AVG(DATEDIFF(SECOND, execution_start_time, execution_end_time)) as avg_duration_seconds
    FROM `cddp-dev-bronze`.`system`.`pipeline_executions`
    WHERE execution_start_time >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY source_name
),
quality_summary AS (
    SELECT 
        table_name,
        COUNT(*) as total_checks,
        SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) as passed_checks
    FROM `cddp-dev-bronze`.`system`.`data_quality_results`
    WHERE check_timestamp >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY table_name
)
SELECT 
    'pipeline_execution' as metric_type,
    re.source_name as resource_name,
    CASE 
        WHEN re.successful_runs / re.total_runs < 0.95 THEN 'CRITICAL'
        WHEN re.successful_runs / re.total_runs < 0.98 THEN 'WARNING'
        ELSE 'HEALTHY'
    END as health_status,
    re.successful_runs / re.total_runs * 100 as success_rate,
    re.avg_duration_seconds
FROM recent_executions re

UNION ALL

SELECT 
    'data_quality' as metric_type,
    qs.table_name as resource_name,
    CASE 
        WHEN qs.passed_checks / qs.total_checks < 0.90 THEN 'CRITICAL'
        WHEN qs.passed_checks / qs.total_checks < 0.95 THEN 'WARNING'
        ELSE 'HEALTHY'
    END as health_status,
    qs.passed_checks / qs.total_checks * 100 as success_rate,
    NULL as avg_duration_seconds
FROM quality_summary qs;
```

## Diagnostic Tools and Scripts

### Framework Diagnostic Script
```bash
#!/bin/bash
# diagnose.sh - Comprehensive framework diagnostics

echo "=== Data Platform Framework Diagnostics ==="
echo "Timestamp: $(date)"
echo ""

# Environment check
echo "1. Environment Configuration:"
echo "PROJECT_CODE: ${PROJECT_CODE:-'Not set'}"
echo "ENVIRONMENT: ${ENVIRONMENT:-'Not set'}"
echo "AZURE_STORAGE_ACCOUNT: ${AZURE_STORAGE_ACCOUNT_DEV:-'Not set'}"
echo ""

# Azure authentication
echo "2. Azure Authentication:"
az account show --output table 2>/dev/null || echo "Azure CLI not authenticated"
echo ""

# Databricks connectivity
echo "3. Databricks Connectivity:"
databricks workspace list --output json 2>/dev/null | jq -r '.[0].path' || echo "Databricks CLI not configured"
echo ""

# Storage connectivity
echo "4. Storage Connectivity:"
az storage container list --account-name ${AZURE_STORAGE_ACCOUNT_DEV} --output table 2>/dev/null || echo "Storage access failed"
echo ""

# Key Vault access
echo "5. Key Vault Access:"
az keyvault secret list --vault-name kv-data-platform-dev --output table 2>/dev/null || echo "Key Vault access failed"
echo ""

# Python environment
echo "6. Python Environment:"
python --version
pip list | grep -E "(pyspark|azure|databricks)" || echo "Required packages not found"
echo ""

# Framework validation
echo "7. Framework Validation:"
python -c "
try:
    from src.core.config_manager import ConfigManager
    config = ConfigManager()
    print('✓ Framework imports successful')
    print(f'✓ Project: {config.project_code}')
    print(f'✓ Environment: {config.environment}')
except Exception as e:
    print(f'✗ Framework validation failed: {e}')
"

echo ""
echo "=== Diagnostic Complete ==="
```

### Log Analysis Tools
```python
class LogAnalyzer:
    def __init__(self):
        self.log_patterns = {
            'authentication_error': r'Authentication.*failed|401|403',
            'timeout_error': r'timeout|TimeoutException|SQLTimeoutException',
            'memory_error': r'OutOfMemoryError|GC overhead|heap space',
            'network_error': r'Connection.*refused|UnknownHostException|SocketException',
            'permission_error': r'Permission.*denied|Access.*denied|Forbidden'
        }
    
    def analyze_logs(self, log_file_path: str) -> LogAnalysisResult:
        """Analyze log files for common error patterns."""
        error_counts = defaultdict(int)
        error_samples = defaultdict(list)
        
        with open(log_file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                for error_type, pattern in self.log_patterns.items():
                    if re.search(pattern, line, re.IGNORECASE):
                        error_counts[error_type] += 1
                        if len(error_samples[error_type]) < 5:  # Keep sample errors
                            error_samples[error_type].append({
                                'line_number': line_num,
                                'message': line.strip()
                            })
        
        return LogAnalysisResult(
            file_path=log_file_path,
            error_counts=dict(error_counts),
            error_samples=dict(error_samples),
            recommendations=self._generate_recommendations(error_counts)
        )
    
    def _generate_recommendations(self, error_counts: Dict[str, int]) -> List[str]:
        """Generate recommendations based on error patterns."""
        recommendations = []
        
        if error_counts.get('authentication_error', 0) > 0:
            recommendations.append("Check Azure authentication and service principal configuration")
        
        if error_counts.get('timeout_error', 0) > 0:
            recommendations.append("Increase timeout settings or optimize query performance")
        
        if error_counts.get('memory_error', 0) > 0:
            recommendations.append("Increase cluster memory or optimize data processing")
        
        if error_counts.get('network_error', 0) > 0:
            recommendations.append("Check network connectivity and firewall rules")
        
        if error_counts.get('permission_error', 0) > 0:
            recommendations.append("Verify Unity Catalog and storage permissions")
        
        return recommendations
```

This comprehensive troubleshooting guide provides systematic approaches to diagnosing and resolving the most common issues encountered in the Common Data Platform framework.