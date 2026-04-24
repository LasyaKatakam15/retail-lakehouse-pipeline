# Databricks notebook source
from pyspark.sql.functions import (
    col, sum as _sum, count, avg,
    month, year, dense_rank
)
from pyspark.sql.window import Window

print("Starting GOLD Layer Processing")

# ---------------------------------------------------
# LOAD SILVER
# ---------------------------------------------------
silver_df = spark.table("silver_table")

# ---------------------------------------------------
# SAFETY CHECK
# ---------------------------------------------------
required_cols = ["calculated_total", "Quantity", "Price"]

for c in required_cols:
    if c not in silver_df.columns:
        raise Exception(f"Missing required column in Silver: {c}")

# ===================================================
# 1. DIMENSION TABLE - SALESPERSON
# ===================================================
dim_salesperson = spark.createDataFrame([
    ("SP1", "Alex", "Store_1"),
    ("SP2", "Maria", "Store_2"),
    ("SP3", "John", "Store_3")
], ["salesperson_id", "salesperson_name", "store_id"])

dim_salesperson.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("dim_salesperson")

print("dim_salesperson created")


# ===================================================
# 2. SALESPERSON INCENTIVE MART
# ===================================================
fact = silver_df
dim = spark.table("dim_salesperson")

# NOTE: simulated join logic (since real mapping not available)
joined = fact.join(
    dim,
    fact["source_file_name"].contains(dim["store_id"])
)

salesperson_mart = joined.groupBy("salesperson_name").agg(
    _sum("calculated_total").alias("total_sales")
)

window = Window.orderBy(col("total_sales").desc())

incentive_mart = salesperson_mart.withColumn(
    "rank",
    dense_rank().over(window)
)

incentive_mart.show()

incentive_mart.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_salesperson_incentive")

print("Salesperson Incentive Mart created")


# ===================================================
# 3. KPI SUMMARY TABLE
# ===================================================
kpi_df = silver_df.agg(
    _sum("calculated_total").alias("total_revenue"),
    count("*").alias("total_orders"),
    avg("calculated_total").alias("avg_order_value"),
    _sum("Quantity").alias("total_items_sold")
)

kpi_df.write.mode("overwrite").format("delta").saveAsTable("gold_kpi_summary")

print("KPI Summary created")


# ===================================================
# 4. CUSTOMER SEGMENTATION
# ===================================================
customer_base = silver_df.groupBy("Customer").agg(
    _sum("calculated_total").alias("total_spend")
)

customer_segment = customer_base.withColumn(
    "segment",
    (col("total_spend") >= 5000).cast("string")
)

customer_segment.write.mode("overwrite").format("delta").saveAsTable("gold_customer_segment")

print("Customer segmentation created")


# ===================================================
# 5. MONTHLY REVENUE MART
# ===================================================
monthly_mart = silver_df.withColumn("year", year(col("Date"))) \
    .withColumn("month", month(col("Date"))) \
    .groupBy("year", "month") \
    .agg(
        _sum("calculated_total").alias("monthly_revenue"),
        count("*").alias("total_orders")
    )

monthly_mart.write.mode("overwrite").format("delta").saveAsTable("gold_monthly_mart")

print(" Monthly mart created")


# ===================================================
# 6. TOP CUSTOMERS
# ===================================================
customer_mart = silver_df.groupBy("Customer").agg(
    _sum("calculated_total").alias("total_spend")
)

top_customers = customer_mart.orderBy(col("total_spend").desc()).limit(10)

top_customers.write.mode("overwrite").format("delta").saveAsTable("gold_top_customers")

print("Top customers created")


# ===================================================
# 7. TOP PRODUCTS
# ===================================================
product_mart = silver_df.groupBy("SKU").agg(
    _sum("Quantity").alias("total_quantity"),
    _sum("calculated_total").alias("total_revenue")
)

top_products = product_mart.orderBy(col("total_revenue").desc()).limit(10)

top_products.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("gold_top_products")
print(" Top products created")


# ===================================================
# FINAL STATUS
# ===================================================
print("GOLD LAYER COMPLETED SUCCESSFULLY")