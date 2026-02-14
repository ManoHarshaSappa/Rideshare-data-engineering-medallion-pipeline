# ------------------------------------------------------------
# BRONZE INGESTION LAYER (Databricks - Spark Streaming)
# ------------------------------------------------------------
# Purpose:
# This script ingests raw rideshare CSV data from the landing zone
# and loads it into Delta Bronze tables using Spark Structured Streaming.
#
# Key Features:
# - Reads raw CSV data for multiple entities
# - Infers schema from batch read (one-time)
# - Uses streaming read to ingest files incrementally
# - Writes data to Delta Bronze tables
# - Uses checkpointing for fault tolerance
# - Uses trigger(once=True) for micro-batch ingestion
# ------------------------------------------------------------


# Databricks notebook markdown
# MAGIC %md
# MAGIC ### **SPARK STREAMING - BRONZE INGESTION**


# List of all entities (tables) to ingest
entities = ['customers', 'trips', 'payments', 'locations', 'vehicles', 'drivers']


# Loop through each entity and ingest data
for entity in entities:

    # --------------------------------------------------------
    # STEP 1: Read batch data once to infer schema
    # --------------------------------------------------------
    # Spark streaming requires a predefined schema.
    # So we first read the data in batch mode to capture schema.
    # --------------------------------------------------------
    df_batch = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load(f"/Volumes/pysparkdbt/source/source_data/{entity}")

    schema_entity = df_batch.schema


    # --------------------------------------------------------
    # STEP 2: Read data as streaming source
    # --------------------------------------------------------
    # Using Spark Structured Streaming to monitor the directory
    # and ingest new files incrementally.
    # --------------------------------------------------------
    df = spark.readStream.format("csv") \
        .option("header", True) \
        .schema(schema_entity) \
        .load(f"/Volumes/pysparkdbt/source/source_data/{entity}/")


    # --------------------------------------------------------
    # STEP 3: Write stream to Delta Bronze table
    # --------------------------------------------------------
    # - format("delta") → writes data into Delta Lake format
    # - outputMode("append") → new records are appended
    # - checkpointLocation → used for fault tolerance & tracking
    # - trigger(once=True) → processes available data as one batch
    # - toTable() → writes directly into managed Delta table
    # --------------------------------------------------------
    df.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"/Volumes/pysparkdbt/bronze/checkpoint/{entity}") \
        .trigger(once=True) \
        .toTable(f"pysparkdbt.bronze.{entity}")
