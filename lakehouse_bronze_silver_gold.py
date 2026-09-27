# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze / Silver / Gold Lakehouse
# MAGIC Raw CSV -> Bronze (as-is) -> Silver (cleaned) -> Gold (business-ready), all as Delta tables.
# MAGIC Run each cell top to bottom with Shift+Enter.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Where the tables will live
# MAGIC A *catalog* is a folder of *schemas*; a *schema* is a folder of *tables*. We create one schema per layer.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
print("Schemas ready: bronze, silver, gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: land the raw file exactly as it arrived
# MAGIC Nothing is fixed here. Bronze is the audit trail; if Silver ever looks wrong you come back to this.
# MAGIC We add two columns every platform adds: when it landed and where it came from.

# COMMAND ----------

from pyspark.sql import functions as F

# CHANGE THIS PATH to wherever you uploaded sales_raw.csv (see instructions).
RAW_PATH = "/Volumes/workspace/default/raw/sales_raw.csv"

bronze_df = (spark.read
    .option("header", True)
    .csv(RAW_PATH)                       # every column read as text on purpose: raw means raw
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit("sales_raw.csv")))

bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze.sales")

display(spark.table("bronze.sales"))
print("Bronze rows:", spark.table("bronze.sales").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver: clean, standardise, deduplicate, validate
# MAGIC The raw file has the problems real feeds have: two date formats, a lowercase region,
# MAGIC a missing region, a missing quantity, a missing date, and two exact duplicate orders.
# MAGIC Silver fixes what can be fixed, quarantines what cannot, and records why.

# COMMAND ----------

b = spark.table("bronze.sales")

# Parse two date formats into one real date column.
parsed_date = F.coalesce(
    F.to_date("order_date", "yyyy-MM-dd"),
    F.to_date("order_date", "dd/MM/yyyy"))

silver_df = (b
    .withColumn("order_date", parsed_date)
    .withColumn("region", F.upper(F.trim("region")))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .dropDuplicates(["order_id"]))          # same order_id twice = duplicate feed row

# Data quality rule: a row must have a date, a region, and a quantity to be trusted.
silver_df = silver_df.withColumn("dq_issue",
    F.when(F.col("order_date").isNull(), "missing_date")
     .when(F.col("region").isNull() | (F.col("region") == ""), "missing_region")
     .when(F.col("quantity").isNull(), "missing_quantity")
     .otherwise(None))

good = silver_df.filter(F.col("dq_issue").isNull()).drop("dq_issue")
quarantine = silver_df.filter(F.col("dq_issue").isNotNull())

good.write.format("delta").mode("overwrite").saveAsTable("silver.sales")
quarantine.write.format("delta").mode("overwrite").saveAsTable("silver.sales_quarantine")

print("Bronze rows:", b.count())
print("Silver rows (clean):", good.count())
print("Quarantined rows:", quarantine.count())
display(spark.table("silver.sales_quarantine"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold: business-ready aggregates
# MAGIC Gold answers questions people actually ask. Here: revenue by month and region.
# MAGIC Power BI would point at this table, never at Bronze or Silver.

# COMMAND ----------

s = spark.table("silver.sales")

gold_df = (s
    .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
    .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("order_month", "region")
    .agg(F.sum("revenue").alias("total_revenue"),
         F.sum("quantity").alias("units_sold"),
         F.countDistinct("order_id").alias("orders"))
    .orderBy("order_month", "region"))

gold_df.write.format("delta").mode("overwrite").saveAsTable("gold.monthly_revenue_by_region")

display(spark.table("gold.monthly_revenue_by_region"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta features you can talk about in an interview
# MAGIC Every save above created a version. Delta keeps the history, so you can audit or roll back.

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY silver.sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Governance: who can see what
# MAGIC In Unity Catalog you grant access per schema. This is the "governance and operational controls" line in the job.
# MAGIC The command below is what you would run on a real workspace; on Free Edition run it if it works, otherwise leave it and explain it.

# COMMAND ----------

try:
    spark.sql("GRANT SELECT ON SCHEMA gold TO `account users`")
    print("Granted SELECT on gold to all account users")
except Exception as e:
    print("Grant not supported on this edition; on the client platform this is how read-only access to Gold would be given.")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Monitoring: the check a scheduled job would run
# MAGIC A platform job runs this after every load and alerts if anything is off.

# COMMAND ----------

bronze_n = spark.table("bronze.sales").count()
silver_n = spark.table("silver.sales").count()
quar_n   = spark.table("silver.sales_quarantine").count()
gold_n   = spark.table("gold.monthly_revenue_by_region").count()

assert bronze_n > 0, "ALERT: Bronze is empty, ingestion failed"
assert silver_n + quar_n <= bronze_n, "ALERT: Silver has more rows than Bronze, dedupe broke"
assert gold_n > 0, "ALERT: Gold is empty, transformation failed"

print(f"Pipeline healthy. bronze={bronze_n} silver={silver_n} quarantine={quar_n} gold={gold_n}")
