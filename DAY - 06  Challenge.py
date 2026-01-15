# Databricks notebook source
# MAGIC %md
# MAGIC **# Task 01:- Design 3-layer architecture**
# MAGIC - Bronze Layer: Raw sales and customer data ingested without transformations.
# MAGIC - Silver Layer: Cleaned and validated data with duplicates removed and derived columns added.
# MAGIC - Gold Layer: Business-level aggregates such as revenue and order counts per product.
# MAGIC

# COMMAND ----------

# Import data from csv
sale_data = spark.read.csv('/Volumes/workspace/default/data/product_sales_solution - orders.csv', header=True, inferSchema=True)

# COMMAND ----------

# Task 02:- Build Bronze: raw ingestion

from pyspark.sql.functions import current_timestamp

# Rename column
sale_data_clean = sale_data.withColumnRenamed("price (in INR)", "price_in_INR")

bronze_sales = sale_data_clean.withColumn(
    "ingestion_ts",
    current_timestamp()
)

# Write to Bronze table
bronze_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_sale_data")

bronze_sales.show(5)

# COMMAND ----------

# Task 03:- Build Silver: cleaning & validation

from pyspark.sql.functions import col, when, to_date

bronze_sales = spark.table("workspace.default.bronze_sale_data")

# Clean & Validate
silver_sales = (
    bronze_sales
    .filter(col("total_price").isNotNull())
    .filter(col("total_price") > 0)
    .dropDuplicates(["order_id"])
    .withColumn(
        "price_category",
        when(col("total_price") < 500, "Low")
        .when(col("total_price") < 1000, "Medium")
        .otherwise("High")
    
    )
)

# Write to Silver table
silver_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_sale_data")

silver_sales.show(5)

# COMMAND ----------

# Task 04:- Build Gold: business aggregates

from pyspark.sql.functions import sum, countDistinct

silver_sales = spark.table("workspace.default.silver_sale_data")

gold_revenue = (
    silver_sales
    .groupBy("customer_id", "customer_name")
    .agg(
        sum("total_price").alias("total_revenue"),
        countDistinct("order_id").alias("total_orders")
    )
)

# Write to Gold revenue table
gold_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_revenue")

display(gold_revenue)