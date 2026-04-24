# Databricks notebook source
from pyspark.sql import SparkSession


import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timezone

# -------------------------
# S3 CLIENT
# -------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="ap-southeast-2"
)

bucket = "retail-data-pipeline-lasya"

# -------------------------
# LOAD FUNCTION
# -------------------------
def load_csv_from_s3(key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(data))



# -------------------------
# FILE LIST (BRONZE SOURCES)
# -------------------------
files = [
    "bronze/file1.csv",
    "bronze/file2.csv"
]

dfs = []

for file in files:
    print(f"Ingesting: {file}")
    
    df = load_csv_from_s3(file)
    
    # BRONZE METADATA (VERY IMPORTANT)
    df["ingestion_timestamp"] = datetime.now(timezone.utc)
    df["source_file_name"] = file
    
    dfs.append(df)

# -------------------------
# FINAL BRONZE DATASET 
# -------------------------
bronze_pdf = pd.concat(dfs, ignore_index=True)

bronze_df = spark.createDataFrame(bronze_pdf)

# -------------------------
# VALIDATION
# -------------------------
print("Row Count:", bronze_df.count())
bronze_df.show()
#bronze_df.createOrReplaceTempView("bronze_table")
from pyspark.sql.functions import col

# -------------------------
# 1. DROP BAD COLUMN
# -------------------------
if "Unnamed: 0" in bronze_df.columns:
    bronze_df = bronze_df.drop("Unnamed: 0")

# -------------------------
# 2. CLEAN ALL COLUMN NAMES
# -------------------------
for c in bronze_df.columns:
    clean_name = c.strip() \
                  .replace(" ", "_") \
                  .replace(":", "_") \
                  .replace(";", "_") \
                  .replace("(", "") \
                  .replace(")", "") \
                  .replace("{", "") \
                  .replace("}", "") \
                  .replace("\n", "_") \
                  .replace("\t", "_")

    if c != clean_name:
        bronze_df = bronze_df.withColumnRenamed(c, clean_name)
bronze_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("bronze_table")
print("Bronze layer created successfully")