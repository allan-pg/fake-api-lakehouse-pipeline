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


customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ]), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

df = spark.read.schema(customer_schema).json(
    "s3://fake-api-lakehouse-landing-2026/customers/"
)

df = df.select(
    F.col("customer_id"),
    F.col("name"),
    F.col("email"),
    F.col("phone"),
    F.col("address.street").alias("street"),
    F.col("address.city").alias("city"),
    F.col("address.country").alias("country"),
    F.col("created_at"),
    F.col("updated_at")
).withColumn("ingestion_date", F.current_timestamp(UTC = True))
display(df)