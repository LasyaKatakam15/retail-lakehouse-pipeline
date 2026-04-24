# Databricks notebook source
from pyspark.sql.functions import col, trim, to_date
from pyspark.sql import functions as F

# ---------------------------------------------------
# LOAD BRONZE
# ---------------------------------------------------
bronze_df = spark.table("bronze_table")
print("Starting Silver Transformation")
silver_df = bronze_df
print("Initial Row Count:", silver_df.count())

# ---------------------------------------------------
# SCHEMA VALIDATION
# ---------------------------------------------------
required_cols = ["Customer", "SKU", "Price", "Quantity"]

for c in required_cols:
    if c not in silver_df.columns:
        raise Exception(f"Missing required column: {c}")

print(" Schema validation passed")

# ---------------------------------------------------
# REMOVE DUPLICATES
# ---------------------------------------------------
silver_df = silver_df.dropDuplicates()

# ---------------------------------------------------
# CLEAN STRING COLUMNS
# ---------------------------------------------------
for c, t in silver_df.dtypes:
    if t == "string":
        silver_df = silver_df.withColumn(c, trim(col(c)))

# ---------------------------------------------------
# NULL VALUE CHECK (KEY FIELDS)
# ---------------------------------------------------
silver_df = silver_df.filter(
    col("Customer").isNotNull() & col("SKU").isNotNull()
)

# ---------------------------------------------------
# SAFE NULL HANDLING (IMPUTATION)
# ---------------------------------------------------
fill_map = {}

for c in silver_df.columns:
    if c.lower() == "quantity":
        fill_map[c] = 0
    elif c.lower() == "price":
        fill_map[c] = 0.0
    elif c.lower() == "discount":
        fill_map[c] = 0.0
    elif c.lower() == "totalsales":
        fill_map[c] = 0.0

silver_df = silver_df.fillna(fill_map)

# ---------------------------------------------------
# TYPE CASTING
# ---------------------------------------------------
if "Quantity" in silver_df.columns:
    silver_df = silver_df.withColumn("Quantity", col("Quantity").cast("int"))

if "Price" in silver_df.columns:
    silver_df = silver_df.withColumn("Price", col("Price").cast("double"))

if "Discount" in silver_df.columns:
    silver_df = silver_df.withColumn("Discount", col("Discount").cast("double"))

if "TotalSales" in silver_df.columns:
    silver_df = silver_df.withColumn("TotalSales", col("TotalSales").cast("double"))

# ---------------------------------------------------
# DATE STANDARDIZATION
# ---------------------------------------------------
if "Date" in silver_df.columns:
    silver_df = silver_df.withColumn("Date", to_date(col("Date")))

# ---------------------------------------------------
# BUSINESS RULE VALIDATIONS
# ---------------------------------------------------

# Quantity >= 0
silver_df = silver_df.filter(col("Quantity") >= 0)

# Price >= 0
silver_df = silver_df.filter(col("Price") >= 0)

# Discount rule (0–100)
if "Discount" in silver_df.columns:
    silver_df = silver_df.filter(
        (col("Discount") >= 0) & (col("Discount") <= 100)
    )

# ---------------------------------------------------
# DERIVED COLUMN
# ---------------------------------------------------
silver_df = silver_df.withColumn(
    "calculated_total",
    col("Price") * col("Quantity")
)

# ---------------------------------------------------
# CONSISTENCY CHECK (IMPORTANT FOR INTERVIEWS)
# ---------------------------------------------------
if "TotalSales" in silver_df.columns:
    silver_df = silver_df.withColumn(
        "revenue_diff",
        col("TotalSales") - col("calculated_total")
    )

# ---------------------------------------------------
# FINAL VALIDATION
# ---------------------------------------------------
print("Final Row Count:", silver_df.count())

print("Sample Data:")
silver_df.show(10)

print("Final Schema:")
silver_df.printSchema()

# ---------------------------------------------------
# SAVE SILVER TABLE
# ---------------------------------------------------
silver_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("silver_table")

print("Silver Layer Completed Successfully ")