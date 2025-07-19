# Unity Catalog Architecture

This document explains how the Common Data Platform framework leverages Unity Catalog for data governance, security, and catalog management across the medallion architecture.

## Overview

Unity Catalog provides centralized governance for data and AI assets across Databricks workspaces, enabling:
- **Multi-environment catalog isolation** (dev, test, prod)
- **Fine-grained access control** at catalog, schema, and table levels
- **Data lineage tracking** across bronze, silver, and gold layers
- **Automated metadata management** with business glossary integration

## Catalog Naming Convention

### Standard Format
```
{project-code}-{environment}-{medallion-layer}
```

### Examples
| Environment | Bronze Catalog | Silver Catalog | Gold Catalog |
|-------------|----------------|----------------|--------------|
| **Development** | `cddp-dev-bronze` | `cddp-dev-silver` | `cddp-dev-gold` |
| **Test** | `cddp-test-bronze` | `cddp-test-silver` | `cddp-test-gold` |
| **Production** | `cddp-prod-bronze` | `cddp-prod-silver` | `cddp-prod-gold` |

### Multi-Project Support
For different projects within the same organization:
```bash
# Customer Data Platform
cddp-prod-bronze, cddp-prod-silver, cddp-prod-gold

# Financial Reporting Platform  
frrp-prod-bronze, frrp-prod-silver, frrp-prod-gold

# Marketing Analytics Platform
mapp-prod-bronze, mapp-prod-silver, mapp-prod-gold
```

## Catalog Structure and Organization

### Bronze Layer Catalogs
Purpose: Raw data ingestion with minimal processing

```sql
-- Bronze catalog structure
CREATE CATALOG IF NOT EXISTS `cddp-dev-bronze`;

-- Source system schemas
CREATE SCHEMA IF NOT EXISTS `cddp-dev-bronze`.`oracle_data`;
CREATE SCHEMA IF NOT EXISTS `cddp-dev-bronze`.`excel_data`;
CREATE SCHEMA IF NOT EXISTS `cddp-dev-bronze`.`csv_data`;
CREATE SCHEMA IF NOT EXISTS `cddp-dev-bronze`.`api_data`;

-- System management schema
CREATE SCHEMA IF NOT EXISTS `cddp-dev-bronze`.`system`;
```

#### Bronze Layer Tables
| Schema | Table | Purpose |
|--------|-------|---------|
| `oracle_data` | `customers`, `orders`, `products` | Oracle source tables |
| `excel_data` | `daily_sales`, `customer_master` | Excel file ingestion |
| `csv_data` | `product_catalog`, `inventory` | CSV file ingestion |
| `system` | `processed_files`, `watermarks`, `pipeline_executions` | Framework metadata |

### Silver Layer Catalogs
Purpose: Cleansed, validated, and standardized data

```sql
-- Silver catalog structure
CREATE CATALOG IF NOT EXISTS `cddp-dev-silver`;

-- Business domain schemas
CREATE SCHEMA IF NOT EXISTS `cddp-dev-silver`.`customer_data`;
CREATE SCHEMA IF NOT EXISTS `cddp-dev-silver`.`product_data`;
CREATE SCHEMA IF NOT EXISTS `cddp-dev-silver`.`sales_data`;
CREATE SCHEMA IF NOT EXISTS `cddp-dev-silver`.`inventory_data`;

-- System schema for data quality
CREATE SCHEMA IF NOT EXISTS `cddp-dev-silver`.`system`;
```

#### Silver Layer Tables
| Schema | Table | Purpose |
|--------|-------|---------|
| `customer_data` | `dim_customers_scd2`, `customer_profiles` | Customer dimensions |
| `product_data` | `dim_products`, `product_hierarchy` | Product dimensions |
| `sales_data` | `fact_sales`, `sales_transactions` | Sales transactions |
| `inventory_data` | `fact_inventory`, `stock_movements` | Inventory facts |

### Gold Layer Catalogs
Purpose: Business-ready analytics and aggregated data

```sql
-- Gold catalog structure
CREATE CATALOG IF NOT EXISTS `cddp-dev-gold`;

-- Analytics schemas
CREATE SCHEMA IF NOT EXISTS `cddp-dev-gold`.`analytics`;
CREATE SCHEMA IF NOT EXISTS `cddp-dev-gold`.`reporting`;
CREATE SCHEMA IF NOT EXISTS `cddp-dev-gold`.`kpis`;
CREATE SCHEMA IF NOT EXISTS `cddp-dev-gold`.`dashboards`;
```

#### Gold Layer Tables
| Schema | Table | Purpose |
|--------|-------|---------|
| `analytics` | `customer_360`, `product_performance` | Advanced analytics |
| `reporting` | `daily_sales_summary`, `monthly_revenue` | Standard reports |
| `kpis` | `sales_kpis`, `inventory_kpis` | Key performance indicators |
| `dashboards` | `executive_dashboard`, `ops_dashboard` | Dashboard datasets |

## Permissions and Security Model

### Role-Based Access Control

#### Predefined Roles
```sql
-- Data Engineers - Full access to dev/test, limited prod access
GRANT CREATE CATALOG ON METASTORE TO `data-engineers`;
GRANT USE CATALOG ON CATALOG `cddp-dev-bronze` TO `data-engineers`;
GRANT ALL PRIVILEGES ON CATALOG `cddp-dev-bronze` TO `data-engineers`;

-- Data Analysts - Read access to silver/gold, no bronze access
GRANT USE CATALOG ON CATALOG `cddp-prod-silver` TO `data-analysts`;
GRANT USE CATALOG ON CATALOG `cddp-prod-gold` TO `data-analysts`;
GRANT SELECT ON CATALOG `cddp-prod-silver` TO `data-analysts`;
GRANT SELECT ON CATALOG `cddp-prod-gold` TO `data-analysts`;

-- Business Users - Read access to gold layer only
GRANT USE CATALOG ON CATALOG `cddp-prod-gold` TO `business-users`;
GRANT SELECT ON SCHEMA `cddp-prod-gold`.`reporting` TO `business-users`;
GRANT SELECT ON SCHEMA `cddp-prod-gold`.`dashboards` TO `business-users`;

-- Service Principal - Automated pipeline execution
GRANT USE CATALOG ON CATALOG `cddp-prod-bronze` TO `data-platform-sp`;
GRANT USE CATALOG ON CATALOG `cddp-prod-silver` TO `data-platform-sp`;
GRANT USE CATALOG ON CATALOG `cddp-prod-gold` TO `data-platform-sp`;
```

#### Environment-Specific Permissions
| Environment | Data Engineers | Data Analysts | Business Users | Service Principal |
|-------------|----------------|---------------|----------------|-------------------|
| **Development** | Full Access | Read Silver/Gold | No Access | Full Access |
| **Test** | Full Access | Read Silver/Gold | No Access | Full Access |
| **Production** | Read Only | Read Silver/Gold | Read Gold | Full Access |

### Column-Level Security

#### Sensitive Data Protection
```sql
-- Create column mask for PII data
CREATE FUNCTION `cddp-prod-silver`.`customer_data`.mask_email(email STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_member('pii-viewers') THEN email
  ELSE REGEXP_REPLACE(email, '^(.{2}).*@(.*)$', '$1***@$2')
END;

-- Apply mask to customer table
ALTER TABLE `cddp-prod-silver`.`customer_data`.`dim_customers_scd2`
SET COLUMN email MASK `cddp-prod-silver`.`customer_data`.mask_email;
```

#### Row-Level Security
```sql
-- Row-level security for multi-tenant data
CREATE FUNCTION `cddp-prod-gold`.`analytics`.customer_filter()
RETURNS STRING
RETURN CASE 
  WHEN is_member('customer-a-analysts') THEN "customer_segment = 'Customer-A'"
  WHEN is_member('customer-b-analysts') THEN "customer_segment = 'Customer-B'"
  WHEN is_member('admin-analysts') THEN "1=1"
  ELSE "1=0"
END;

-- Apply row filter
ALTER TABLE `cddp-prod-gold`.`analytics`.`customer_360`
SET ROW FILTER `cddp-prod-gold`.`analytics`.customer_filter() ON;
```

## Data Lineage and Discovery

### Automated Lineage Tracking

Unity Catalog automatically tracks lineage for:
- **Source to Bronze**: File ingestion to bronze tables
- **Bronze to Silver**: Transformation queries and dependencies
- **Silver to Gold**: Aggregation and business logic

#### Lineage Query Examples
```sql
-- View table lineage
DESCRIBE TABLE LINEAGE `cddp-prod-gold`.`analytics`.`customer_360`;

-- View column lineage  
DESCRIBE COLUMN LINEAGE `cddp-prod-gold`.`analytics`.`customer_360`.`customer_lifetime_value`;

-- Search for tables using specific source
SELECT * FROM system.lineage.table_lineage 
WHERE upstream_table_name LIKE '%customers%';
```

### Metadata Management

#### Table Properties and Comments
```sql
-- Add business metadata to tables
ALTER TABLE `cddp-prod-silver`.`customer_data`.`dim_customers_scd2`
SET TBLPROPERTIES (
  'business_owner' = 'Customer Analytics Team',
  'data_classification' = 'PII',
  'refresh_frequency' = 'Daily',
  'sla_hours' = '4',
  'retention_years' = '7'
)
COMMENT 'Customer dimension table with SCD Type 2 history tracking';

-- Add column comments
ALTER TABLE `cddp-prod-silver`.`customer_data`.`dim_customers_scd2`
ALTER COLUMN customer_id COMMENT 'Unique customer identifier from source system';
ALTER COLUMN record_hash COMMENT 'MD5 hash of tracked columns for change detection';
```

#### Data Quality Tags
```sql
-- Tag tables with data quality metrics
ALTER TABLE `cddp-prod-silver`.`sales_data`.`fact_sales`
SET TAGS ('data_quality' = 'high', 'completeness' = '99.8%', 'timeliness' = 'T+1');
```

## Multi-Environment Strategy

### Environment Isolation

#### Development Environment
- **Purpose**: Feature development and testing
- **Data**: Subset of production data or synthetic data
- **Access**: Open access for data engineers
- **Retention**: 30 days for transient data

#### Test Environment  
- **Purpose**: Integration testing and UAT
- **Data**: Production-like data with masking
- **Access**: Controlled access for testing teams
- **Retention**: 90 days for validation data

#### Production Environment
- **Purpose**: Live business operations
- **Data**: Full production datasets
- **Access**: Strict role-based access
- **Retention**: According to business requirements

### Cross-Environment Promotion

#### Automated Promotion Pipeline
```yaml
# Databricks Asset Bundle configuration
environments:
  dev:
    mode: development
    compute_id: ${var.dev_cluster_id}
    catalog: cddp-dev-bronze
    
  test:
    mode: production
    compute_id: ${var.test_cluster_id}
    catalog: cddp-test-bronze
    
  prod:
    mode: production
    compute_id: ${var.prod_cluster_id}
    catalog: cddp-prod-bronze
```

#### Configuration Management
```python
# Environment-specific configuration
class EnvironmentManager:
    def __init__(self, environment: str):
        self.environment = environment
        
    def get_catalog_name(self, layer: str) -> str:
        return f"cddp-{self.environment}-{layer}"
        
    def get_permissions(self) -> Dict[str, List[str]]:
        if self.environment == 'prod':
            return {'read_only': ['data-analysts', 'business-users']}
        else:
            return {'full_access': ['data-engineers']}
```

## Performance Optimization

### Catalog-Level Optimizations

#### Delta Table Optimizations
```sql
-- Enable auto-optimize for all tables in catalog
ALTER CATALOG `cddp-prod-silver` 
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

#### Partitioning Strategy
```sql
-- Partition strategy for large fact tables
CREATE TABLE `cddp-prod-silver`.`sales_data`.`fact_sales` (
  transaction_id STRING,
  customer_id STRING,
  product_id STRING,
  sale_date DATE,
  amount DECIMAL(10,2)
)
USING DELTA
PARTITIONED BY (year(sale_date), month(sale_date));
```

#### Z-Ordering for Query Performance
```sql
-- Optimize for common query patterns
OPTIMIZE `cddp-prod-silver`.`sales_data`.`fact_sales`
ZORDER BY (customer_id, product_id);
```

### Monitoring and Maintenance

#### Catalog Statistics
```sql
-- Monitor catalog storage usage
SELECT 
  catalog_name,
  schema_name,
  table_name,
  size_in_bytes,
  num_files
FROM system.information_schema.table_storage_info
WHERE catalog_name LIKE 'cddp-%'
ORDER BY size_in_bytes DESC;
```

#### Access Monitoring
```sql
-- Monitor table access patterns
SELECT 
  table_catalog,
  table_schema,
  table_name,
  user_identity,
  access_time,
  query_type
FROM system.access.table_access_history
WHERE table_catalog LIKE 'cddp-%'
  AND access_time >= current_date() - INTERVAL 7 DAYS;
```

## Best Practices

### 1. Naming Conventions
- **Catalogs**: `{project}-{env}-{layer}` format
- **Schemas**: Business domain-based grouping
- **Tables**: Descriptive names with layer prefix (`dim_`, `fact_`, `raw_`)

### 2. Security Implementation
- **Principle of Least Privilege**: Grant minimum required access
- **Environment Separation**: Strict isolation between environments
- **Sensitive Data Protection**: Column masks and row filters for PII

### 3. Metadata Management
- **Business Documentation**: Comprehensive table and column comments
- **Data Classification**: Proper tagging for sensitive data
- **Lineage Tracking**: Maintain clear data flow documentation

### 4. Performance Considerations
- **Partitioning**: Use date-based partitioning for large tables
- **Z-Ordering**: Optimize for common query patterns
- **Auto-Optimize**: Enable for frequently updated tables

### 5. Governance Controls
- **Change Management**: Use CI/CD for schema changes
- **Access Reviews**: Regular permission audits
- **Data Quality**: Implement automated quality checks

This Unity Catalog architecture ensures scalable, secure, and well-governed data management across the entire medallion architecture while supporting multi-environment deployments and team collaboration.