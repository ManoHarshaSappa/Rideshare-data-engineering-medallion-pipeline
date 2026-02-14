from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat, row_number, desc, current_timestamp, upper
from delta.tables import DeltaTable


# ------------------------------------------------------------
# Transformations Class
# ------------------------------------------------------------
# This class contains reusable data transformation utilities
# used across Bronze → Silver data processing layers.
#
# It supports:
# - Deduplication using CDC logic
# - Adding processing timestamp
# - Performing UPSERT (merge) into Delta tables
# ------------------------------------------------------------

class transformations:

    # --------------------------------------------------------
    # Deduplication Method
    # --------------------------------------------------------
    # Purpose:
    # Removes duplicate records based on given key columns
    # and keeps the latest record using CDC column (timestamp/version).
    #
    # Parameters:
    # df          -> Input DataFrame
    # dedup_cols  -> Columns used to identify duplicates (business key)
    # cdc         -> Change Data Capture column (e.g., updated_at, timestamp)
    #
    # Logic:
    # 1. Create a composite key (dedupKey)
    # 2. Assign row_number partitioned by key and ordered by latest CDC value
    # 3. Keep only row_number = 1 (latest record)
    # 4. Drop helper columns
    # --------------------------------------------------------
    def dedup(self, df: DataFrame, dedup_cols: List[str], cdc: str):
        df = df.withColumn("dedupKey", concat(*[col(c) for c in dedup_cols]))
        
        df = df.withColumn(
            "dedupCounts",
            row_number().over(
                Window.partitionBy("dedupKey").orderBy(desc(cdc))
            )
        )

        # Keep only the latest record per key
        df = df.filter(col("dedupCounts") == 1)

        # Drop helper columns
        df = df.drop("dedupKey", "dedupCounts")

        return df


    # --------------------------------------------------------
    # Process Timestamp Method
    # --------------------------------------------------------
    # Purpose:
    # Adds a system-generated timestamp column to track
    # when the record was processed by the pipeline.
    #
    # This is useful for:
    # - Data lineage
    # - Auditing
    # - Debugging
    # --------------------------------------------------------
    def process_timestamp(self, df: DataFrame):
        df = df.withColumn("process_timestamp", current_timestamp())
        return df


    # --------------------------------------------------------
    # UPSERT (MERGE) Method
    # --------------------------------------------------------
    # Purpose:
    # Performs an UPSERT (merge) into a Delta table.
    #
    # If record exists (based on key columns):
    #   → Update it only if the incoming record is newer (CDC comparison)
    #
    # If record does not exist:
    #   → Insert it
    #
    # Parameters:
    # spark     -> SparkSession
    # df        -> Source DataFrame (incoming data)
    # key_cols  -> Primary key columns for matching
    # table     -> Target Delta table name
    # cdc       -> Change Data Capture column (timestamp/version)
    #
    # Target table path:
    # pysparkdbt.silver.<table>
    #
    # This method uses Delta Lake MERGE INTO for efficient incremental loading.
    # --------------------------------------------------------
    def upsert(self, spark: SparkSession, df: DataFrame, key_cols: List[str], table: str, cdc: str):

        # Build merge condition dynamically using key columns
        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])

        # Load existing Delta table from metastore
        dlt_obj = DeltaTable.forName(spark, f"pysparkdbt.silver.{table}")

        # Perform MERGE operation
        dlt_obj.alias("trg").merge(
            df.alias("src"),
            merge_condition
        ).whenMatchedUpdateAll(
            # Update only if incoming record is newer
            condition=f"src.{cdc} >= trg.{cdc}"
        ).whenNotMatchedInsertAll() \
         .execute()

        return 1
