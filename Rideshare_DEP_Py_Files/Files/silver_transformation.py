# ------------------------------------------------------------
# SILVER TRANSFORMATION LAYER - PYSPARK + DELTA LAKE
# ------------------------------------------------------------
# This notebook transforms Bronze raw tables into clean,
# deduplicated, enriched Silver tables using PySpark.
#
# Features:
# - Data cleaning & standardization
# - CDC-based deduplication
# - Process timestamp tracking
# - Incremental UPSERT using Delta MERGE
# ------------------------------------------------------------


# ------------------------------------------------------------
# Enable auto reload for utils (Databricks)
# ------------------------------------------------------------
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2


# ------------------------------------------------------------
# Import required libraries
# ------------------------------------------------------------
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os, sys


# ------------------------------------------------------------
# Add current directory to import custom utils
# ------------------------------------------------------------
current_dir = os.getcwd()
sys.path.append(current_dir)

from utils.custom_utils import transformations

# Initialize transformation object
trns = transformations()


# ============================================================
# CUSTOMERS
# ============================================================

df_cust = spark.read.table("pysparkdbt.bronze.customers")

# Clean & transform
df_cust = df_cust.withColumn("domain", split(col('email'), '@')[1])
df_cust = df_cust.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))
df_cust = df_cust.withColumn("full_name", concat_ws(" ", col('first_name'), col('last_name')))
df_cust = df_cust.drop('first_name', 'last_name')

# Deduplicate + timestamp
df_cust = trns.dedup(df_cust, ['customer_id'], 'last_updated_timestamp')
df_cust = trns.process_timestamp(df_cust)

# Write or upsert
if not spark.catalog.tableExists("pysparkdbt.silver.customers"):
    df_cust.write.format("delta").mode("append").saveAsTable("pysparkdbt.silver.customers")
else:
    trns.upsert(spark, df_cust, ['customer_id'], 'customers', 'last_updated_timestamp')


# ============================================================
# DRIVERS
# ============================================================

df_driver = spark.read.table("pysparkdbt.bronze.drivers")

df_driver = df_driver.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))
df_driver = df_driver.withColumn("full_name", concat_ws(" ", col('first_name'), col('last_name')))
df_driver = df_driver.drop('first_name', 'last_name')

df_driver = trns.dedup(df_driver, ['driver_id'], 'last_updated_timestamp')
df_driver = trns.process_timestamp(df_driver)

if not spark.catalog.tableExists("pysparkdbt.silver.drivers"):
    df_driver.write.format("delta").mode("append").saveAsTable("pysparkdbt.silver.drivers")
else:
    trns.upsert(spark, df_driver, ['driver_id'], 'drivers', 'last_updated_timestamp')


# ============================================================
# LOCATIONS
# ============================================================

df_loc = spark.read.table("pysparkdbt.bronze.locations")

df_loc = trns.dedup(df_loc, ['location_id'], 'last_updated_timestamp')
df_loc = trns.process_timestamp(df_loc)

if not spark.catalog.tableExists("pysparkdbt.silver.locations"):
    df_loc.write.format("delta").mode("append").saveAsTable("pysparkdbt.silver.locations")
else:
    trns.upsert(spark, df_loc, ['location_id'], 'locations', 'last_updated_timestamp')


# ============================================================
# PAYMENTS
# ============================================================

df_pay = spark.read.table("pysparkdbt.bronze.payments")

df_pay = df_pay.withColumn(
    "online_payment_status",
    when((col('payment_method') == 'Card') & (col('payment_status') == 'Success'), "online-success")
    .when((col('payment_method') == 'Card') & (col('payment_status') == 'Failed'), "online-failed")
    .when((col('payment_method') == 'Card') & (col('payment_status') == 'Pending'), "online-pending")
    .otherwise("offline")
)

df_pay = trns.dedup(df_pay, ['payment_id'], 'last_updated_timestamp')
df_pay = trns.process_timestamp(df_pay)

if not spark.catalog.tableExists("pysparkdbt.silver.payments"):
    df_pay.write.format("delta").mode("append").saveAsTable("pysparkdbt.silver.payments")
else:
    trns.upsert(spark, df_pay, ['payment_id'], 'payments', 'last_updated_timestamp')


# ============================================================
# VEHICLES
# ============================================================

df_veh = spark.read.table("pysparkdbt.bronze.vehicles")

df_veh = df_veh.withColumn("make", upper(col("make")))

df_veh = trns.dedup(df_veh, ['vehicle_id'], 'last_updated_timestamp')
df_veh = trns.process_timestamp(df_veh)

if not spark.catalog.tableExists("pysparkdbt.silver.vehicles"):
    df_veh.write.format("delta").mode("append").saveAsTable("pysparkdbt.silver.vehicles")
else:
    trns.upsert(spark, df_veh, ['vehicle_id'], 'vehicles', 'last_updated_timestamp')
