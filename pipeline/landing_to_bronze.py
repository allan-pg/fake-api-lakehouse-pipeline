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