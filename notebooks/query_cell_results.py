# Databricks notebook source
# MAGIC %md
# MAGIC # Query Cell Execution Results
# MAGIC 
# MAGIC This notebook queries the cell execution monitoring table to see detailed results

# COMMAND ----------

from datetime import datetime, timedelta

# Configuration
BRONZE_CATALOG = "cddp-dev-bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent Cell Executions

# COMMAND ----------

# Query recent executions
print("📊 RECENT CELL EXECUTIONS")
print("=" * 60)

# Get executions from last hour
spark.sql(f"""
    SELECT 
        notebook_run_id,
        cell_id,
        cell_name,
        status,
        duration_seconds,
        critical,
        start_time
    FROM `{BRONZE_CATALOG}`.`system`.`cell_executions`
    WHERE start_time > current_timestamp() - INTERVAL 1 HOUR
    ORDER BY start_time DESC, cell_id
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by Run

# COMMAND ----------

# Summary by run
spark.sql(f"""
    SELECT 
        notebook_run_id,
        COUNT(*) as total_cells,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_cells,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_cells,
        ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate,
        SUM(duration_seconds) as total_duration,
        MAX(start_time) as run_time
    FROM `{BRONZE_CATALOG}`.`system`.`cell_executions`
    WHERE start_time > current_timestamp() - INTERVAL 1 HOUR
    GROUP BY notebook_run_id
    ORDER BY run_time DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failed Cells Detail

# COMMAND ----------

# Show failed cells
failed_df = spark.sql(f"""
    SELECT 
        notebook_run_id,
        cell_id,
        cell_name,
        error,
        critical,
        start_time
    FROM `{BRONZE_CATALOG}`.`system`.`cell_executions`
    WHERE status = 'FAILED'
    AND start_time > current_timestamp() - INTERVAL 1 HOUR
    ORDER BY start_time DESC
""")

if failed_df.count() > 0:
    print("❌ FAILED CELLS:")
    failed_df.show(truncate=False)
else:
    print("✅ No failed cells in the last hour!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Most Recent Run Details

# COMMAND ----------

# Get most recent run
recent_run = spark.sql(f"""
    SELECT notebook_run_id
    FROM `{BRONZE_CATALOG}`.`system`.`cell_executions`
    WHERE start_time > current_timestamp() - INTERVAL 1 HOUR
    ORDER BY start_time DESC
    LIMIT 1
""").collect()

if recent_run:
    run_id = recent_run[0].notebook_run_id
    print(f"📊 MOST RECENT RUN: {run_id}")
    print("=" * 60)
    
    # Show all cells from this run
    spark.sql(f"""
        SELECT 
            cell_id,
            cell_name,
            status,
            CASE 
                WHEN LENGTH(output) > 100 THEN CONCAT(SUBSTRING(output, 1, 100), '...')
                ELSE output
            END as output_preview,
            duration_seconds
        FROM `{BRONZE_CATALOG}`.`system`.`cell_executions`
        WHERE notebook_run_id = '{run_id}'
        ORDER BY cell_id
    """).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Table Creation Results

# COMMAND ----------

# Check what tables were created
print("📊 TABLES CREATED IN EACH LAYER:")
print("=" * 60)

# Bronze tables
print("\n🥉 BRONZE LAYER:")
for schema in ['csv_data', 'excel_data']:
    tables = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`{schema}`").collect()
    if tables:
        print(f"\n{schema}:")
        for table in tables:
            count = spark.sql(f"SELECT COUNT(*) FROM `{BRONZE_CATALOG}`.`{schema}`.`{table.tableName}`").collect()[0][0]
            print(f"  - {table.tableName}: {count:,} records")

# Silver tables
print("\n🥈 SILVER LAYER:")
SILVER_CATALOG = "cddp-dev-silver"
for schema in ['products', 'sales']:
    try:
        tables = spark.sql(f"SHOW TABLES IN `{SILVER_CATALOG}`.`{schema}`").collect()
        if tables:
            print(f"\n{schema}:")
            for table in tables:
                count = spark.sql(f"SELECT COUNT(*) FROM `{SILVER_CATALOG}`.`{schema}`.`{table.tableName}`").collect()[0][0]
                print(f"  - {table.tableName}: {count:,} records")
    except:
        pass

# Gold tables
print("\n🥇 GOLD LAYER:")
GOLD_CATALOG = "cddp-dev-gold"
for schema in ['analytics', 'reporting']:
    try:
        tables = spark.sql(f"SHOW TABLES IN `{GOLD_CATALOG}`.`{schema}`").collect()
        if tables:
            print(f"\n{schema}:")
            for table in tables:
                count = spark.sql(f"SELECT COUNT(*) FROM `{GOLD_CATALOG}`.`{schema}`.`{table.tableName}`").collect()[0][0]
                print(f"  - {table.tableName}: {count:,} records")
    except:
        pass

# COMMAND ----------

# Return success
dbutils.notebook.exit("QUERY_COMPLETE")