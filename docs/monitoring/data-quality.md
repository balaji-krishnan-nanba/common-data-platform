# Data Quality Monitoring Guide

This guide covers comprehensive data quality monitoring capabilities built into the Common Data Platform framework, including validation rules, quality metrics, and automated remediation strategies.

## Data Quality Overview

The framework implements a multi-layered data quality approach:

1. **Ingestion Quality** - Schema validation and basic checks at bronze layer
2. **Transformation Quality** - Business rule validation during silver processing
3. **Analytics Quality** - Completeness and consistency checks in gold layer
4. **Continuous Monitoring** - Ongoing quality assessment and alerting

## Quality Check Types

### 1. Schema Validation Checks

#### Column Existence and Type Validation
```yaml
# config/data_quality/schema_validation.yaml
schema_checks:
  customer_data:
    required_columns:
      - name: customer_id
        type: string
        nullable: false
      - name: email
        type: string
        nullable: true
        pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
      - name: phone
        type: string
        nullable: true
        pattern: '^\+?1?-?\(?[0-9]{3}\)?-?[0-9]{3}-?[0-9]{4}$'
      - name: signup_date
        type: date
        nullable: false
        min_date: '2020-01-01'
        max_date: 'current_date'
    
    optional_columns:
      - name: marketing_consent
        type: boolean
        default: false
```

#### Implementation
```python
class SchemaValidator:
    def __init__(self, schema_config: Dict):
        self.schema_config = schema_config
    
    def validate_dataframe(self, df: DataFrame, table_name: str) -> ValidationResult:
        """Validate DataFrame against expected schema."""
        errors = []
        warnings = []
        
        expected_schema = self.schema_config.get(table_name, {})
        required_columns = expected_schema.get('required_columns', [])
        
        # Check required columns exist
        df_columns = set(df.columns)
        for col_config in required_columns:
            col_name = col_config['name']
            
            if col_name not in df_columns:
                errors.append(f"Missing required column: {col_name}")
                continue
            
            # Check data type
            actual_type = dict(df.dtypes)[col_name]
            expected_type = col_config['type']
            
            if not self._types_compatible(actual_type, expected_type):
                errors.append(f"Column {col_name} type mismatch: expected {expected_type}, got {actual_type}")
            
            # Check nullability
            if not col_config.get('nullable', True):
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    errors.append(f"Column {col_name} has {null_count} null values but nulls not allowed")
            
            # Check patterns (for string columns)
            if 'pattern' in col_config:
                pattern = col_config['pattern']
                invalid_count = df.filter(
                    col(col_name).isNotNull() & 
                    ~col(col_name).rlike(pattern)
                ).count()
                if invalid_count > 0:
                    errors.append(f"Column {col_name} has {invalid_count} values not matching pattern {pattern}")
        
        return ValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            total_records=df.count()
        )
```

### 2. Completeness Checks

#### Null Value Detection
```yaml
# config/data_quality/completeness_checks.yaml
completeness_checks:
  customer_master:
    critical_fields:
      - customer_id: 
          null_threshold: 0%  # No nulls allowed
      - customer_name:
          null_threshold: 0%
      - email:
          null_threshold: 5%  # Up to 5% nulls acceptable
    
    completeness_thresholds:
      overall: 95%  # 95% of all fields should be populated
      critical: 100%  # All critical fields must be complete
```

#### Implementation
```python
class CompletenessChecker:
    def __init__(self, completeness_config: Dict):
        self.config = completeness_config
    
    def check_completeness(self, df: DataFrame, table_name: str) -> CompletenessResult:
        """Check data completeness against thresholds."""
        table_config = self.config.get(table_name, {})
        results = []
        
        total_records = df.count()
        
        # Check critical fields
        for field, config in table_config.get('critical_fields', {}).items():
            null_count = df.filter(col(field).isNull()).count()
            null_percentage = (null_count / total_records) * 100
            threshold = float(config['null_threshold'].rstrip('%'))
            
            result = CompletenessCheckResult(
                field_name=field,
                null_count=null_count,
                null_percentage=null_percentage,
                threshold=threshold,
                passed=null_percentage <= threshold
            )
            results.append(result)
        
        # Check overall completeness
        overall_completeness = self._calculate_overall_completeness(df)
        overall_threshold = table_config.get('completeness_thresholds', {}).get('overall', 95)
        
        return CompletenessResult(
            table_name=table_name,
            field_results=results,
            overall_completeness=overall_completeness,
            overall_threshold=overall_threshold,
            passed=all(r.passed for r in results) and overall_completeness >= overall_threshold
        )
    
    def _calculate_overall_completeness(self, df: DataFrame) -> float:
        """Calculate overall data completeness percentage."""
        total_cells = df.count() * len(df.columns)
        
        # Count non-null values across all columns
        non_null_exprs = [
            when(col(c).isNotNull(), 1).otherwise(0).alias(f"{c}_non_null")
            for c in df.columns
        ]
        
        non_null_df = df.select(*non_null_exprs)
        total_non_null = sum([
            non_null_df.agg(sum(f"{c}_non_null")).collect()[0][0] 
            for c in df.columns
        ])
        
        return (total_non_null / total_cells) * 100
```

### 3. Uniqueness Checks

#### Duplicate Detection
```yaml
# config/data_quality/uniqueness_checks.yaml
uniqueness_checks:
  customer_data:
    primary_keys:
      - columns: [customer_id]
        duplicate_threshold: 0  # No duplicates allowed
    
    business_keys:
      - columns: [email]
        duplicate_threshold: 1%  # Allow 1% duplicates (data entry errors)
      - columns: [phone]
        duplicate_threshold: 2%
    
    composite_keys:
      - columns: [customer_id, transaction_date]
        duplicate_threshold: 0
        scope: transaction_data
```

#### Implementation
```python
class UniquenessChecker:
    def __init__(self, uniqueness_config: Dict):
        self.config = uniqueness_config
    
    def check_uniqueness(self, df: DataFrame, table_name: str) -> UniquenessResult:
        """Check data uniqueness constraints."""
        table_config = self.config.get(table_name, {})
        results = []
        
        total_records = df.count()
        
        # Check primary keys
        for key_config in table_config.get('primary_keys', []):
            columns = key_config['columns']
            threshold = key_config['duplicate_threshold']
            
            # Count duplicates
            duplicate_count = df.groupBy(*columns).count() \
                .filter(col('count') > 1) \
                .agg(sum('count') - count('*')).collect()[0][0] or 0
            
            duplicate_percentage = (duplicate_count / total_records) * 100
            
            result = UniquenessCheckResult(
                key_type='primary',
                columns=columns,
                duplicate_count=duplicate_count,
                duplicate_percentage=duplicate_percentage,
                threshold=threshold,
                passed=duplicate_percentage <= threshold
            )
            results.append(result)
        
        # Check business keys
        for key_config in table_config.get('business_keys', []):
            columns = key_config['columns']
            threshold = key_config['duplicate_threshold']
            
            duplicate_count = df.filter(col(columns[0]).isNotNull()) \
                .groupBy(*columns).count() \
                .filter(col('count') > 1) \
                .agg(sum('count') - count('*')).collect()[0][0] or 0
            
            duplicate_percentage = (duplicate_count / total_records) * 100
            
            result = UniquenessCheckResult(
                key_type='business',
                columns=columns,
                duplicate_count=duplicate_count,
                duplicate_percentage=duplicate_percentage,
                threshold=threshold,
                passed=duplicate_percentage <= threshold
            )
            results.append(result)
        
        return UniquenessResult(
            table_name=table_name,
            check_results=results,
            passed=all(r.passed for r in results)
        )
```

### 4. Range and Domain Checks

#### Value Range Validation
```yaml
# config/data_quality/range_checks.yaml
range_checks:
  sales_data:
    numeric_ranges:
      - column: quantity
        min_value: 1
        max_value: 1000
        outlier_threshold: 3_std_dev
      
      - column: unit_price
        min_value: 0.01
        max_value: 10000.00
        business_rule: "unit_price > 0"
      
      - column: discount_percentage
        min_value: 0
        max_value: 100
        warning_threshold: 50  # Warn if discount > 50%
    
    date_ranges:
      - column: order_date
        min_date: '2020-01-01'
        max_date: 'current_date + 1'  # Allow tomorrow for pre-orders
      
      - column: delivery_date
        min_date: 'order_date'  # Can't deliver before order
        max_date: 'order_date + 30'  # Max 30 days delivery
    
    categorical_domains:
      - column: order_status
        allowed_values: ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
        case_sensitive: false
      
      - column: payment_method
        allowed_values: ['credit_card', 'debit_card', 'cash', 'bank_transfer']
        fuzzy_match: true  # Allow slight variations
```

#### Implementation
```python
class RangeChecker:
    def __init__(self, range_config: Dict):
        self.config = range_config
    
    def check_ranges(self, df: DataFrame, table_name: str) -> RangeCheckResult:
        """Check value ranges and domains."""
        table_config = self.config.get(table_name, {})
        results = []
        
        # Numeric range checks
        for range_config in table_config.get('numeric_ranges', []):
            column = range_config['column']
            min_val = range_config.get('min_value')
            max_val = range_config.get('max_value')
            
            violations = 0
            if min_val is not None:
                violations += df.filter(col(column) < min_val).count()
            if max_val is not None:
                violations += df.filter(col(column) > max_val).count()
            
            # Check for outliers using statistical methods
            if 'outlier_threshold' in range_config:
                outlier_count = self._detect_outliers(df, column, range_config['outlier_threshold'])
                violations += outlier_count
            
            results.append(RangeCheckResult(
                column=column,
                check_type='numeric_range',
                violations=violations,
                total_records=df.count(),
                passed=violations == 0
            ))
        
        # Date range checks
        for date_config in table_config.get('date_ranges', []):
            column = date_config['column']
            violations = self._check_date_ranges(df, date_config)
            
            results.append(RangeCheckResult(
                column=column,
                check_type='date_range',
                violations=violations,
                total_records=df.count(),
                passed=violations == 0
            ))
        
        # Categorical domain checks
        for cat_config in table_config.get('categorical_domains', []):
            column = cat_config['column']
            allowed_values = cat_config['allowed_values']
            case_sensitive = cat_config.get('case_sensitive', True)
            
            if not case_sensitive:
                allowed_values = [v.lower() for v in allowed_values]
                check_col = lower(col(column))
            else:
                check_col = col(column)
            
            violations = df.filter(
                col(column).isNotNull() & 
                ~check_col.isin(allowed_values)
            ).count()
            
            results.append(RangeCheckResult(
                column=column,
                check_type='categorical_domain',
                violations=violations,
                total_records=df.count(),
                passed=violations == 0
            ))
        
        return RangeCheckResult(
            table_name=table_name,
            check_results=results,
            passed=all(r.passed for r in results)
        )
    
    def _detect_outliers(self, df: DataFrame, column: str, method: str) -> int:
        """Detect outliers using statistical methods."""
        if method == '3_std_dev':
            stats = df.select(
                mean(column).alias('mean'),
                stddev(column).alias('stddev')
            ).collect()[0]
            
            mean_val = stats['mean']
            std_val = stats['stddev']
            
            return df.filter(
                (col(column) < mean_val - 3 * std_val) |
                (col(column) > mean_val + 3 * std_val)
            ).count()
        
        elif method == 'iqr':
            # Interquartile range method
            quantiles = df.select(
                expr(f"percentile_approx({column}, 0.25)").alias('q1'),
                expr(f"percentile_approx({column}, 0.75)").alias('q3')
            ).collect()[0]
            
            q1, q3 = quantiles['q1'], quantiles['q3']
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            return df.filter(
                (col(column) < lower_bound) |
                (col(column) > upper_bound)
            ).count()
        
        return 0
```

### 5. Referential Integrity Checks

#### Foreign Key Validation
```yaml
# config/data_quality/referential_integrity.yaml
referential_integrity:
  order_items:
    foreign_keys:
      - column: customer_id
        reference_table: "cddp-dev-silver.customer_data.dim_customers_scd2"
        reference_column: customer_id
        reference_filter: "is_current_flag = true"
        violation_threshold: 0%
      
      - column: product_id
        reference_table: "cddp-dev-silver.product_data.dim_products"
        reference_column: product_id
        reference_filter: "is_active = true"
        violation_threshold: 1%  # Allow 1% for data timing issues
    
    cross_table_consistency:
      - check: "order_total_matches_items"
        sql: |
          SELECT o.order_id 
          FROM orders o
          LEFT JOIN (
            SELECT order_id, SUM(quantity * unit_price) as calculated_total
            FROM order_items 
            GROUP BY order_id
          ) i ON o.order_id = i.order_id
          WHERE ABS(o.total_amount - COALESCE(i.calculated_total, 0)) > 0.01
```

#### Implementation
```python
class ReferentialIntegrityChecker:
    def __init__(self, integrity_config: Dict):
        self.config = integrity_config
    
    def check_referential_integrity(self, df: DataFrame, table_name: str) -> IntegrityResult:
        """Check referential integrity constraints."""
        table_config = self.config.get(table_name, {})
        results = []
        
        # Check foreign keys
        for fk_config in table_config.get('foreign_keys', []):
            column = fk_config['column']
            ref_table = fk_config['reference_table']
            ref_column = fk_config['reference_column']
            ref_filter = fk_config.get('reference_filter')
            threshold = float(fk_config.get('violation_threshold', '0%').rstrip('%'))
            
            # Load reference table
            ref_df = spark.table(ref_table)
            if ref_filter:
                ref_df = ref_df.filter(expr(ref_filter))
            
            # Get valid reference values
            valid_refs = ref_df.select(ref_column).distinct()
            
            # Find orphaned records
            orphaned_count = df.filter(col(column).isNotNull()) \
                .join(valid_refs, col(column) == col(ref_column), 'left_anti') \
                .count()
            
            total_records = df.filter(col(column).isNotNull()).count()
            violation_percentage = (orphaned_count / total_records) * 100 if total_records > 0 else 0
            
            results.append(IntegrityCheckResult(
                check_type='foreign_key',
                column=column,
                reference_table=ref_table,
                orphaned_count=orphaned_count,
                violation_percentage=violation_percentage,
                threshold=threshold,
                passed=violation_percentage <= threshold
            ))
        
        # Check cross-table consistency
        for consistency_check in table_config.get('cross_table_consistency', []):
            check_name = consistency_check['check']
            check_sql = consistency_check['sql']
            
            violations_df = spark.sql(check_sql)
            violation_count = violations_df.count()
            
            results.append(IntegrityCheckResult(
                check_type='cross_table_consistency',
                check_name=check_name,
                violation_count=violation_count,
                passed=violation_count == 0
            ))
        
        return IntegrityResult(
            table_name=table_name,
            check_results=results,
            passed=all(r.passed for r in results)
        )
```

## Quality Monitoring Dashboard

### Real-time Quality Metrics
```sql
-- Data Quality Dashboard Views
CREATE OR REPLACE VIEW data_quality_dashboard AS
WITH quality_summary AS (
    SELECT 
        table_name,
        check_type,
        check_name,
        DATE(check_timestamp) as check_date,
        COUNT(*) as total_checks,
        SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) as passed_checks,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_checks,
        SUM(CASE WHEN status = 'warning' THEN 1 ELSE 0 END) as warning_checks,
        AVG(records_checked) as avg_records_checked,
        AVG(records_failed) as avg_records_failed
    FROM `cddp-dev-bronze`.`system`.`data_quality_results`
    WHERE check_timestamp >= current_date() - INTERVAL 7 DAYS
    GROUP BY table_name, check_type, check_name, DATE(check_timestamp)
),
quality_trends AS (
    SELECT 
        table_name,
        check_date,
        SUM(passed_checks) / SUM(total_checks) * 100 as daily_pass_rate,
        SUM(failed_checks) as daily_failures,
        SUM(avg_records_failed) as daily_record_failures
    FROM quality_summary
    GROUP BY table_name, check_date
)
SELECT 
    qs.table_name,
    qs.check_type,
    qs.check_name,
    qs.check_date,
    qs.total_checks,
    qs.passed_checks,
    qs.failed_checks,
    qs.warning_checks,
    ROUND(qs.passed_checks / qs.total_checks * 100, 2) as pass_percentage,
    qs.avg_records_checked,
    qs.avg_records_failed,
    qt.daily_pass_rate,
    -- Trend analysis
    LAG(qt.daily_pass_rate) OVER (
        PARTITION BY qs.table_name 
        ORDER BY qs.check_date
    ) as previous_day_pass_rate,
    CASE 
        WHEN qt.daily_pass_rate > LAG(qt.daily_pass_rate) OVER (
            PARTITION BY qs.table_name ORDER BY qs.check_date
        ) THEN 'IMPROVING'
        WHEN qt.daily_pass_rate < LAG(qt.daily_pass_rate) OVER (
            PARTITION BY qs.table_name ORDER BY qs.check_date
        ) THEN 'DEGRADING'
        ELSE 'STABLE'
    END as quality_trend
FROM quality_summary qs
JOIN quality_trends qt ON qs.table_name = qt.table_name AND qs.check_date = qt.check_date
ORDER BY qs.table_name, qs.check_date DESC, qs.check_type;
```

### Quality Score Calculation
```python
class DataQualityScorer:
    def __init__(self):
        self.weights = {
            'completeness': 0.25,
            'uniqueness': 0.20,
            'validity': 0.20,
            'consistency': 0.15,
            'accuracy': 0.10,
            'timeliness': 0.10
        }
    
    def calculate_quality_score(self, table_name: str, date: str) -> QualityScore:
        """Calculate comprehensive data quality score."""
        
        # Get all quality check results for the table/date
        quality_results = self._get_quality_results(table_name, date)
        
        dimension_scores = {}
        
        # Calculate scores for each dimension
        dimension_scores['completeness'] = self._calculate_completeness_score(quality_results)
        dimension_scores['uniqueness'] = self._calculate_uniqueness_score(quality_results)
        dimension_scores['validity'] = self._calculate_validity_score(quality_results)
        dimension_scores['consistency'] = self._calculate_consistency_score(quality_results)
        dimension_scores['accuracy'] = self._calculate_accuracy_score(quality_results)
        dimension_scores['timeliness'] = self._calculate_timeliness_score(quality_results)
        
        # Calculate weighted overall score
        overall_score = sum(
            score * self.weights[dimension]
            for dimension, score in dimension_scores.items()
        )
        
        return QualityScore(
            table_name=table_name,
            date=date,
            overall_score=overall_score,
            dimension_scores=dimension_scores,
            grade=self._assign_grade(overall_score)
        )
    
    def _assign_grade(self, score: float) -> str:
        """Assign letter grade based on score."""
        if score >= 95:
            return 'A+'
        elif score >= 90:
            return 'A'
        elif score >= 85:
            return 'B+'
        elif score >= 80:
            return 'B'
        elif score >= 75:
            return 'C+'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'
```

## Automated Quality Remediation

### Data Cleansing Rules
```python
class DataCleanser:
    def __init__(self):
        self.cleansing_rules = self._load_cleansing_rules()
    
    def apply_automatic_cleansing(self, df: DataFrame, table_name: str) -> DataFrame:
        """Apply automatic data cleansing rules."""
        
        cleansed_df = df
        applied_rules = []
        
        rules = self.cleansing_rules.get(table_name, {})
        
        # Standard cleansing operations
        for column, column_rules in rules.get('column_rules', {}).items():
            
            # Trim whitespace
            if column_rules.get('trim_whitespace', False):
                cleansed_df = cleansed_df.withColumn(column, trim(col(column)))
                applied_rules.append(f"Trimmed whitespace from {column}")
            
            # Standardize case
            case_rule = column_rules.get('case_standardization')
            if case_rule == 'upper':
                cleansed_df = cleansed_df.withColumn(column, upper(col(column)))
                applied_rules.append(f"Converted {column} to uppercase")
            elif case_rule == 'lower':
                cleansed_df = cleansed_df.withColumn(column, lower(col(column)))
                applied_rules.append(f"Converted {column} to lowercase")
            elif case_rule == 'title':
                cleansed_df = cleansed_df.withColumn(column, initcap(col(column)))
                applied_rules.append(f"Converted {column} to title case")
            
            # Replace invalid values
            if 'invalid_value_replacements' in column_rules:
                for invalid, replacement in column_rules['invalid_value_replacements'].items():
                    cleansed_df = cleansed_df.withColumn(
                        column,
                        when(col(column) == invalid, replacement).otherwise(col(column))
                    )
                    applied_rules.append(f"Replaced '{invalid}' with '{replacement}' in {column}")
            
            # Fill null values
            null_fill = column_rules.get('null_fill_value')
            if null_fill is not None:
                cleansed_df = cleansed_df.fillna({column: null_fill})
                applied_rules.append(f"Filled null values in {column} with '{null_fill}'")
        
        # Add cleansing metadata
        cleansed_df = cleansed_df.withColumn(
            "cleansing_applied",
            lit("|".join(applied_rules))
        ).withColumn(
            "cleansing_timestamp",
            current_timestamp()
        )
        
        return cleansed_df
```

### Quality Incident Management
```python
class QualityIncidentManager:
    def __init__(self, alert_manager):
        self.alert_manager = alert_manager
        self.incident_threshold = {
            'critical': 95,  # Quality score below 95% is critical
            'warning': 85,   # Quality score below 85% is warning
            'info': 75       # Quality score below 75% is info
        }
    
    def process_quality_results(self, quality_results: List[QualityResult]):
        """Process quality results and create incidents."""
        
        for result in quality_results:
            quality_score = self._calculate_score(result)
            
            if quality_score < self.incident_threshold['critical']:
                self._create_incident(result, 'CRITICAL', quality_score)
            elif quality_score < self.incident_threshold['warning']:
                self._create_incident(result, 'WARNING', quality_score)
            elif quality_score < self.incident_threshold['info']:
                self._create_incident(result, 'INFO', quality_score)
    
    def _create_incident(self, result: QualityResult, severity: str, score: float):
        """Create quality incident."""
        incident = QualityIncident(
            incident_id=str(uuid.uuid4()),
            table_name=result.table_name,
            check_type=result.check_type,
            severity=severity,
            quality_score=score,
            failure_details=result.failure_details,
            detected_timestamp=datetime.utcnow(),
            status='OPEN'
        )
        
        # Store incident
        self._store_incident(incident)
        
        # Send alert
        self.alert_manager.send_alert(
            severity=severity,
            title=f"Data Quality Issue - {result.table_name}",
            message=f"Quality score {score:.1f}% for {result.table_name}.{result.check_type}",
            details=incident
        )
        
        # Auto-remediation for known issues
        if severity != 'CRITICAL':
            self._attempt_auto_remediation(incident)
```

## Best Practices

### 1. Quality Check Configuration
- **Layer-appropriate checks**: Different quality standards for bronze vs. gold
- **Business-driven thresholds**: Set thresholds based on business impact
- **Graduated responses**: Warning before failing, with different severity levels

### 2. Performance Optimization
- **Sampling for large datasets**: Use statistical sampling for massive tables
- **Incremental checking**: Only check new/changed data when possible
- **Parallel execution**: Run multiple quality checks concurrently

### 3. Quality Monitoring Strategy
- **Continuous monitoring**: Integrate quality checks into all pipelines
- **Historical trending**: Track quality metrics over time
- **Business impact assessment**: Link quality issues to business outcomes

### 4. Incident Response
- **Automated alerting**: Immediate notification for critical issues
- **Clear escalation paths**: Define who gets notified based on severity
- **Root cause analysis**: Track issues back to source systems

This comprehensive data quality monitoring system ensures high-quality data throughout the medallion architecture while providing visibility into data health and automated remediation capabilities.