# Operational Guide

This guide covers day-to-day operations of the Common Data Platform.

## Daily Operations

### Check Pipeline Status

1. **View Job Runs in Databricks**
   - Navigate to Workflows → Select your job
   - Check recent run status and logs

2. **Monitor Data Quality**
   ```sql
   SELECT * FROM cddp_dev_bronze.system.pipeline_executions
   WHERE date(start_time) = current_date()
   ORDER BY start_time DESC
   ```

### Common Issues and Solutions

#### Excel File Not Found

**Symptoms:** Job fails with "No files found to process"

**Solution:**
1. Check file path pattern in configuration
2. Verify storage account access
3. Confirm file exists in expected location

#### Schema Validation Errors

**Symptoms:** Job fails with "Schema validation failed"

**Solution:**
1. Check source file column names and types
2. Update schema configuration if needed
3. Set `strict_validation: false` for flexible validation

## Monitoring

### Key Metrics to Monitor

- **Pipeline Success Rate**: % of successful job runs
- **Data Freshness**: Time since last successful ingestion
- **Data Volume**: Number of records processed daily
- **Processing Time**: Average job execution time

### Alerts Setup

Configure alerts for:
- Job failures
- Data quality issues
- Processing time increases
- Data volume anomalies

## Maintenance

### Weekly Tasks

1. Review job performance metrics
2. Check data quality reports
3. Monitor storage usage
4. Review failed job logs

### Monthly Tasks

1. Archive old log data
2. Review and optimize job schedules
3. Update documentation
4. Performance tuning review

## Troubleshooting

### Debug Mode

Enable debug logging:
```bash
python -m src.cli run_bronze_ingestion --source daily_sales_excel --log-level DEBUG
```

### Common Commands

**Check configuration:**
```bash
python -m src.cli validate_source_config --source daily_sales_excel
```

**List all sources:**
```bash
python -m src.cli list_sources
```

**View logs:**
```bash
databricks fs ls /mnt/logs/cddp-dev/
```