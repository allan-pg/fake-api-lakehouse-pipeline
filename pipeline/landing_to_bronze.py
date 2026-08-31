from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql import DataFrame

import logging

logger = logging.getLogger("bronze_ingestion")

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.setLevel(logging.INFO)

# CONTROL TABLES

CONFIG_TABLE = "control.default.ingestion_config"
STATE_TABLE = "control.default.ingestion_state"


# S3 LANDING LOCATION WHERE WE ARE SOURCING DATA FROM
S3_BASE_PATH = "s3://fake-api-lakehouse-landing-2026/"


# BRONZE LOCATION

BRONZE_CATALOG = "enterprise_bronze"
BRONZE_SCHEMA = "ecommerce"

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {BRONZE_CATALOG}
""")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS
    {BRONZE_CATALOG}.{BRONZE_SCHEMA}
""")


def get_active_config():

    return (
        spark.table("control.default.ingestion_config")
        .filter(F.col("is_active") == True)
    )


def get_ingestion_state(source_id):

    state_df = (
        spark.table("control.default.ingestion_state")
        .filter(F.col("source_id") == source_id)
        .select("last_successful_watermark")
    )

    row = state_df.first()

    if row is None:
        logger.info(
            f"{source_id}: No ingestion state found. FULL LOAD."
        )
        return None

    return row["last_successful_watermark"]


def add_ingestion_date(df):

    return df.withColumn(
        "ingestion_date",
        F.to_date(
            F.regexp_extract(
                F.col("_metadata.file_path"),
                r"ingestion_date=(\d{4}-\d{2}-\d{2})",
                1
            )
        )
    )


def get_source_stream(source_path, last_watermark):

    logger.info(
        f"Reading source path: {source_path}"
    )

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(source_path)
    )

    df = add_ingestion_date(df)

    if last_watermark is not None:

        logger.info(
            f"Applying ingestion watermark > {last_watermark}"
        )

        df = df.filter(
            F.col("ingestion_date") > F.lit(last_watermark)
        )

    return df



def read_source_stream(
    source_path,
    file_format
):
    """
    Create an Auto Loader streaming DataFrame.

    Source files are organized by ingestion_date:

        customers/
            ingestion_date=2026-08-24/
            ingestion_date=2026-08-26/
    """

    logger.info(
        f"Reading source path: {S3_BASE_PATH}"
    )

    schema_location = (
        f"{S3_BASE_PATH}/_schema/{source_path.rstrip('/')}"
    )

    return (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            file_format
        )
        .option(
            "cloudFiles.schemaLocation",
            schema_location
        )
        .option(
            "cloudFiles.inferColumnTypes",
            "true"
        )
        .option(
            "cloudFiles.includeExistingFiles",
            "true"
        )
        .load(
            f"{S3_BASE_PATH}/{source_path}"
        )
    )


def flatten_dataframe(df):
    while True:
        # 1. Handle Structs (Expand columns)
        struct_cols = [
            field.name for field in df.schema.fields 
            if isinstance(field.dataType, StructType)
        ]
        if struct_cols:
            col_name = struct_cols[0]
            nested_fields = df.schema[col_name].dataType.fields
            flattened = [
                F.col(f"`{col_name}`.`{f.name}`").alias(f"{col_name}_{f.name}")
                for f in nested_fields
            ]
            df = df.select("*", *flattened).drop(col_name)
            continue

        # 2. Handle Arrays (Explode rows)
        array_cols = [
            field.name for field in df.schema.fields 
            if isinstance(field.dataType, ArrayType)
        ]
        if array_cols:
            col_name = array_cols[0]
            df = df.withColumn(col_name, F.explode_outer(F.col(col_name)))
            continue

        break

    return df

def add_bronze_metadata(df):

    return (
        df
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "source_file",
            F.col("_metadata.file_path")
        )
    )


def get_batch_watermark(df):

    return (
        df
        .agg(
            F.max("ingestion_date").alias("max_watermark")
        )
        .first()["max_watermark"]
    )


def merge_to_bronze(
    batch_df,
    target_table_name: str,
    primary_key: str
):
    # 1. Check if target table exists in Unity Catalog / Hive Metastore
    if not spark.catalog.tableExists(target_table_name):
        logger.info(
            f"Target table {target_table_name} does not exist. Initializing table with initial batch."
        )
        
        # Initial Write: Create the Delta table and catalog entry dynamically
        (
            batch_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table_name)
        )
    else:
        # Subsequent Batches: Perform Delta MERGE INTO
        delta_table = DeltaTable.forName(spark, target_table_name)
        
        # Build merge condition dynamically (handles composite or single primary keys)
        if isinstance(primary_key, list):
            merge_cond = " AND ".join([f"target.{col} = source.{col}" for col in primary_key])
        else:
            merge_cond = f"target.{primary_key} = source.{primary_key}"

        (
            delta_table.alias("target")
            .merge(
                batch_df.alias("source"),
                merge_cond
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


def update_ingestion_state(
    source_id,
    batch_watermark
):

    state_table = "control.default.ingestion_state"

    state_df = spark.createDataFrame(
        [
            (
                source_id,
                batch_watermark
            )
        ],
        [
            "source_id",
            "last_successful_watermark"
        ]
    ).withColumn(
        "updated_at",
        F.current_timestamp()
    )

    state_delta = DeltaTable.forName(
        spark,
        state_table
    )

    (
        state_delta.alias("target")
        .merge(
            state_df.alias("source"),
            "target.source_id = source.source_id"
        )
        .whenMatchedUpdate(
            set={
                "last_successful_watermark":
                    "source.last_successful_watermark",
                "updated_at":
                    "source.updated_at"
            }
        )
        .whenNotMatchedInsert(
            values={
                "source_id":
                    "source.source_id",
                "last_successful_watermark":
                    "source.last_successful_watermark",
                "updated_at":
                    "source.updated_at"
            }
        )
        .execute()
    )

def process_batch(
    batch_df,
    batch_id,
    source_id,
    target_table_name,
    primary_key
):

    logger.info(
        f"{source_id}: Processing batch {batch_id}"
    )

    # 1. Check for empty batch

    if batch_df.isEmpty():

        logger.info(
            f"{source_id}: Batch {batch_id} is empty."
        )

        return

    # 2. Get batch watermark BEFORE transformations

    batch_watermark = get_batch_watermark(batch_df)

    logger.info(
        f"{source_id}: Batch watermark = {batch_watermark}"
    )

    # 3. Flatten nested JSON

    logger.info(
        f"{source_id}: Flattening nested JSON."
    )

    flattened_df = flatten_structs(batch_df)

    # 4. Add Bronze metadata

    flattened_df = add_bronze_metadata(
        flattened_df
    )

    # 5. Merge into Bronze

    logger.info(
        f"{source_id}: Merging into {target_table_name}"
    )

    merge_to_bronze(
        flattened_df,
        target_table_name,
        primary_key
    )

    # 6. Update state ONLY after successful merge

    if batch_watermark is not None:

        update_ingestion_state(
            source_id,
            batch_watermark
        )

        logger.info(
            f"{source_id}: "
            f"Ingestion state updated to {batch_watermark}"
        )

    logger.info(
        f"{source_id}: "
        f"Batch {batch_id} completed successfully."
    )


def run_bronze_pipeline():

    logger.info(
        "Starting Bronze ingestion pipeline."
    )

    config_df = get_active_config()

    configs = config_df.toLocalIterator()

    for config in configs:

        source_id = config["source_id"]
        source_path = config["source_path"]
        target_catalog = config["target_catalog"]
        target_schema = config["target_schema"]
        target_table = config["target_table"]
        primary_key = config["primary_key"]

        target_table_name = (
            f"{target_catalog}."
            f"{target_schema}."
            f"{target_table}"
        )

        logger.info(
            f"Starting ingestion for {source_id}"
        )

        # ----------------------------------------------
        # Get current watermark
        # ----------------------------------------------

        last_watermark = get_ingestion_state(
            source_id
        )

        # ----------------------------------------------
        # Build source path
        # ----------------------------------------------

        source_path_full = (
            f"{source_path}"
        )

        logger.info(
            f"{source_id}: "
            f"Last watermark = {last_watermark}"
        )

        # ----------------------------------------------
        # Read source
        # ----------------------------------------------

        source_df = get_source_stream(
            source_path_full,
            last_watermark
        )

        # ----------------------------------------------
        # Process micro-batches
        # ----------------------------------------------

        query = (
            source_df
            .writeStream
            .foreachBatch(
                lambda batch_df, batch_id:
                    process_batch(
                        batch_df,
                        batch_id,
                        source_id,
                        target_table_name,
                        primary_key
                    )
            )
            .option(
                "checkpointLocation",
                f"/checkpoints/bronze/{source_id}"
            )
            .trigger(
                availableNow=True
            )
            .start()
        )

        query.awaitTermination()

        logger.info(
            f"{source_id}: Ingestion completed successfully."
        )

    logger.info(
        "Bronze ingestion pipeline completed."
    )



def run_bronze_pipeline():

    logger.info(
        "Starting Bronze ingestion pipeline."
    )

    try:

        # Read active ingestion configuration
        config_df = (
            spark.table("control.default.ingestion_config")
            .filter(
                F.col("is_active") == True
            )
        )

        # Process each configured source
        for config in config_df.toLocalIterator():

            source_id = config["source_id"]
            source_path = config["source_path"]
            target_catalog = config["target_catalog"]
            target_schema = config["target_schema"]
            target_table = config["target_table"]
            primary_key = config["primary_key"]
            watermark_column = config["watermark_column"]
            file_format = config["file_format"]

            target_table_name = (
                f"{target_catalog}."
                f"{target_schema}."
                f"{target_table}"
            )

            logger.info(
                f"Starting ingestion for {source_id}"
            )

            # Get last successful watermark
            last_watermark = get_ingestion_state(
                source_id
            )

            logger.info(
                f"{source_id}: "
                f"Last watermark = {last_watermark}"
            )

            # Create Auto Loader stream
            source_df = read_source_stream(
                source_path,
                file_format
            )

            # Process each micro-batch
            query = (
                source_df.writeStream
                .foreachBatch(
                    lambda batch_df, batch_id:
                        process_batch(
                            batch_df,
                            batch_id,
                            source_id,
                            target_table_name,
                            primary_key
                        )
                )
                .option(
                    "checkpointLocation",
                    f"{S3_BASE_PATH}/_checkpoints/"
                    f"{source_id}"
                )
                .trigger(
                    availableNow=True
                )
                .start()
            )

            query.awaitTermination()

            logger.info(
                f"Completed ingestion for {source_id}"
            )

        logger.info(
            "Bronze ingestion pipeline completed successfully."
        )

    except Exception:

        logger.exception(
            "Bronze ingestion pipeline failed."
        )

        raise

# MAIN
if __name__ == "__main__":

    logger.info("Starting Bronze ingestion pipeline.")

    try:

        run_bronze_pipeline()

        logger.info(
            "Bronze ingestion pipeline completed successfully."
        )

    except Exception as e:

        logger.exception(
            "Bronze ingestion pipeline failed."
        )

        raise