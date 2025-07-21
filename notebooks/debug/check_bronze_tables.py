# Databricks notebook source
# MAGIC %md
# MAGIC # Check Bronze Tables
# MAGIC This notebook checks the current state of bronze tables

# COMMAND ----------

print("🔍 Checking Bronze Tables")
print("=" * 50)

# COMMAND ----------

# Check catalogs
print("1️⃣ Available Catalogs:")
spark.sql("SHOW CATALOGS LIKE 'cddp-%'").show()

# COMMAND ----------

# Check schemas in bronze catalog
print("\n2️⃣ Schemas in Bronze Catalog:")
try:
    spark.sql("SHOW SCHEMAS IN `cddp-dev-bronze`").show()
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# Check tables in csv_data schema
print("\n3️⃣ Tables in csv_data schema:")
try:
    spark.sql("SHOW TABLES IN `cddp-dev-bronze`.`csv_data`").show()
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# Check if product_catalog table exists and has data
print("\n4️⃣ Checking product_catalog table:")
try:
    count_df = spark.sql("SELECT COUNT(*) as total_records FROM `cddp-dev-bronze`.`csv_data`.`product_catalog`")
    count = count_df.collect()[0]['total_records']
    print(f"✅ product_catalog table has {count} records")
    
    if count > 0:
        print("\nSample data:")
        spark.sql("SELECT * FROM `cddp-dev-bronze`.`csv_data`.`product_catalog` LIMIT 5").show()
        
        print("\nData summary:")
        spark.sql("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT product_id) as unique_products,
                COUNT(DISTINCT category) as categories,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM `cddp-dev-bronze`.`csv_data`.`product_catalog`
        """).show()
except Exception as e:
    print(f"❌ Table doesn't exist or error: {e}")

# COMMAND ----------

# Check system tables
print("\n5️⃣ Checking system tracking tables:")
try:
    spark.sql("SHOW TABLES IN `cddp-dev-bronze`.`system`").show()
    
    # Check processed files
    processed_files_df = spark.sql("""
        SELECT * FROM `cddp-dev-bronze`.`system`.`processed_files`
        WHERE source_name = 'product_catalog_csv'
        ORDER BY processed_timestamp DESC
        LIMIT 5
    """)
    
    if processed_files_df.count() > 0:
        print("\nProcessed files:")
        processed_files_df.show(truncate=False)
except Exception as e:
    print(f"System tables not found or error: {e}")

# COMMAND ----------

print("\n✅ Check complete!")