# Databricks notebook source
# MAGIC %md
# MAGIC Vectorized Delete in Delta Lake: A Performance Game-Changer
# MAGIC
# MAGIC  "🚀 Delta Lake's Vectorized Delete: The Secret to 10x Faster Data Operations!"
# MAGIC
# MAGIC
# MAGIC
# MAGIC Big news for data engineers! Delta Lake 2.0+ introduces vectorized delete - a revolutionary optimization that dramatically improves DELETE operation performance.
# MAGIC
# MAGIC 🔍 What is it?
# MAGIC Vectorized delete processes multiple records in a single operation rather than row-by-row, reducing I/O operations and improving throughput.
# MAGIC
# MAGIC 💡 Why it matters:
# MAGIC
# MAGIC 10x faster DELETE operations on large datasets
# MAGIC
# MAGIC Reduced compute costs
# MAGIC
# MAGIC Lower latency for critical data pipelines
# MAGIC
# MAGIC When to use it:
# MAGIC ✅ Bulk deletion of stale records
# MAGIC ✅ GDPR/compliance data purging
# MAGIC ✅ Regular data maintenance operations
# MAGIC
# MAGIC Pro tip: Combine with OPTIMIZE and ZORDER for maximum performance!

# COMMAND ----------

# MAGIC %md
# MAGIC **Generate Sample Data (100M Records)**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switching to the analytics catalog
# MAGIC USE CATALOG analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switching to the customer_360 schema in the analytics catalog
# MAGIC USE analytics.customer_360

# COMMAND ----------

# Drop the table if it exists
spark.sql("DROP TABLE IF EXISTS analytics.customer_360.customers")

# Generate synthetic customer data (100M records)
df = spark.range(0, 100_000_000).selectExpr(
    "id as customer_id",
    "concat('user_', id) as name",
    "date_add(current_date(), -cast(rand() * 1000 as int)) as last_purchase_date",
    "cast(rand() * 1000 as int) as purchase_count"
)

# Write the DataFrame as a Delta table, automatically registering it in the metastore
df.write.format("delta").mode("overwrite").saveAsTable("analytics.customer_360.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC **Traditional DELETE (Row-by-Row)**

# COMMAND ----------

import time  # Ensure you import the time module

# Benchmark DELETE operation
start_time = time.time()

# Replace 'customers' with your fully qualified table name if required
spark.sql("DELETE FROM customers WHERE last_purchase_date < date_sub(current_date(), 365)")

execution_time = time.time() - start_time

print(f"\033[1mTraditional DELETE took: {execution_time:.2f} seconds\033[0m")

# COMMAND ----------

# MAGIC %md
# MAGIC **deletion vectors on the table **

# COMMAND ----------

import time 
# Enable deletion vectors on the table 
spark.sql("""
  ALTER TABLE analytics.customer_360.customers 
  SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)
""")

# Benchmark DELETE operation using deletion vectors
start_time = time.time()

# Update the table name if necessary; this DELETE will now use deletion vectors
spark.sql("""
  DELETE FROM analytics.customer_360.customers 
  WHERE last_purchase_date < date_sub(current_date(), 365)
""")

execution_time = time.time() - start_time
print(f"\033[1m\033[4mDELETE with Deletion Vectors took: {execution_time:.2f} seconds\033[0m")

# COMMAND ----------

# MAGIC %md
# MAGIC When you run the DELETE operation with deletion vectors enabled, Delta Lake avoids the typical heavy copy-on-write rewrite of entire data files. Instead, it performs these steps behind the scenes:
# MAGIC - Identifying Rows to Delete:
# MAGIC The DELETE command evaluates your condition (in this case, last_purchase_date < date_sub(current_date(), 365)), determining which rows should be removed.
# MAGIC - Recording Deletions Via Vectors:
# MAGIC Instead of rewriting all affected files, Delta Lake creates or updates “deletion vectors.” These are lightweight metadata constructs—often in the form of bitmaps or lists—that mark the specific rows in a file as deleted. Essentially, the original data file remains intact, but Delta records offsets or row positions that should be skipped in future reads.
# MAGIC
# MAGIC - Query-Time Row Filtering:
# MAGIC When queries read the table, Delta automatically applies these deletion vectors, filtering out the logically deleted rows. This means that from a query perspective, the deleted rows are invisible, even though the underlying file hasn’t been physically rewritten.
# MAGIC - Deferred Maintenance:
# MAGIC Over time (for example, during an OPTIMIZE operation), Delta may compact data and physically remove the rows marked as deleted. This consolidates the deletion vectors and reclaims storage, but that’s a background maintenance step separate from your immediate DELETE command.
# MAGIC
# MAGIC So, in your benchmark code, when you issue the DELETE command on the table with deletion vectors enabled, Delta Lake performs a lightweight, vectorized removal of rows, which is typically much faster than the standard full file rewrite—especially when only a small subset of rows needs to be deleted.

# COMMAND ----------

