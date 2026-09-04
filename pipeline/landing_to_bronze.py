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


# s3 path for the landing zone
dbutils.widgets.text("s3_base_path", "s3://fake-api-lakehouse-landing-2026/")

# CONTROL TABLES
dbutils.widgets.text("config_table", "control.default.ingestion_config")
dbutils.widgets.text("state_table", "control.default.ingestion_state")

# bronze schema and catalog
dbutils.widgets.text("bronze_catalog", "enterprise_bronze")
dbutils.widgets.text("bronze_schema", "ecommerce")

# Bronze Location
CONFIG_TABLE = config_table
STATE_TABLE = state_table

# S3 LANDING LOCATION WHERE WE ARE SOURCING DATA FROM
S3_BASE_PATH = s3_base_path


# BRONZE LOCATION
BRONZE_CATALOG = bronze_catalog
BRONZE_SCHEMA = bronze_schema


def get_config():
    config_df = (
        spark.table(CONFIG_TABLE)\
        .filter(F.col("is_active") == True)
    )
    return config_df


def get_watermark(source_id):

    state_df = spark.table(STATE_TABLE)

    row = (
        state_df
        .filter(F.col("source_id") == source_id)
        .select("last_successful_watermark")
        .first()
    )

    if row is None:
        return None

    return row["watermark"]


import re
from datetime import datetime


def get_available_dates(source_path):

    path = f"{S3_BASE_PATH}{source_path}"

    folders = dbutils.fs.ls(path)

    dates = []

    for folder in folders:

        match = re.search(
            r"ingest_date=(\d{4}-\d{2}-\d{2})",
            folder.path
        )

        if match:

            dates.append(
                datetime.strptime(
                    match.group(1),
                    "%Y-%m-%d"
                ).date()
            )

    return sorted(dates)


def get_dates_to_process(source_path, watermark):

    available_dates = get_available_dates(source_path)

    if not available_dates:
        return []

    # FULL LOAD
    if watermark is None:

        return available_dates

    # INCREMENTAL LOAD
    return [
        d for d in available_dates
        if d > watermark
    ]


def read_source_data(config, dates):

    source_path = config["source_path"]
    file_format = config["file_format"]

    dataframes = []

    for ingest_date in dates:

        path = (
            f"{S3_BASE_PATH.rstrip('/')}/"
            f"{source_path.strip('/')}/"
            f"ingest_date={ingest_date}/"
        )

        logger.info(
            f"{config['source_id']}: Reading {path}"
        )

        df = (
            spark.read
            .format(file_format)
            .json(path)
        )

        dataframes.append(df)

    if not dataframes:
        return None

    result = dataframes[0]

    for df in dataframes[1:]:

        result = result.unionByName(
            df,
            allowMissingColumns=True
        )

    return result


def flatten_customer(df):

    return df.select(
        "customer_id",
        "name",
        "email",
        "phone",

        F.col("address.street").alias("street"),
        F.col("address.city").alias("city"),
        F.col("address.country").alias("country"),

        "created_at",
        "updated_at",
        "ingestion_date"
    )


def flatten_customer(df):

    return df.select(
        
    )


from delta.tables import DeltaTable


def update_state(source_id, watermark):

    logger.info(
        f"{source_id}: Updating state watermark to {watermark}"
    )

    state_delta = DeltaTable.forName(
        spark,
        STATE_TABLE
    )

    update_df = spark.createDataFrame(
        [
            (source_id, watermark)
        ],
        ["source_id", "watermark"]
    )

    (
        state_delta.alias("target")
        .merge(
            update_df.alias("source"),
            "target.source_id = source.source_id"
        )
        .whenMatchedUpdate(
            set={
                "watermark": "source.watermark"
            }
        )
        .whenNotMatchedInsert(
            values={
                "source_id": "source.source_id",
                "watermark": "source.watermark"
            }
        )
        .execute()
    )

    logger.info(f"{source_id}: State updated successfully.")

    def run_bronze_pipeline():

    logger.info("Starting Bronze ingestion pipeline.")

   # Read active configuration

    config_df = (
        spark.table(CONFIG_TABLE)
        .filter(F.col("is_active") == True)
    )

    # Process every configured source

    for config in config_df.toLocalIterator():

        source_id = config["source_id"]
        source_path = config["source_path"]

        logger.info(f"Starting ingestion for {source_id}")

        try:

            # 1. Read current state

            watermark = get_watermark(source_id)

            logger.info(f"{source_id}: Current watermark = {watermark}")

            # 2. Determine dates to process

            dates_to_process = get_dates_to_process(source_id, source_path)

            if not dates_to_process:

                logger.info(f"{source_id}: No new data to process.")

                continue

            logger.info(
                f"{source_id}: Dates to process = "
                f"{dates_to_process}"
            )

            # 3. Read JSON from S3
 
            df = read_source_data(config, dates_to_process)

            if df is None:

                logger.info(f"{source_id}: No data found.")

                continue
            # 4. Flatten source


            df = flatten_source(df, source_id)

            # ---------------------------------------
            # 5. Write to Bronze
            # ---------------------------------------

            write_to_bronze(df, config)

              # 6. Bronze succeeded
            #    Now update state


            new_watermark = max(dates_to_process)

            update_state(source_id, new_watermark)

            logger.info(f"{source_id}: Processing completed successfully.")

        except Exception as e:

            logger.exception(f"{source_id}: Bronze ingestion failed.")
            continue


        