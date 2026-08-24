import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Incremental load configuration

LOOKBACK_HOURS = 8

# list of buckets to create in s3
BUCKETS = ["fake-api-lakehouse-landing-2026"]

#get the url
API_URL = os.environ["url"]

# API endpoint configuration

ENDPOINTS = {
    "customers": {
        "id_field": "customer_id",
        "file_prefix": "customer",
        "data_field": "data",
        "watermark_field": "updated_at"
    },

    "orders": {
        "id_field": "order_id",
        "file_prefix": "order",
        "data_field": "data",
        "watermark_field": "audit.updated_at"
    }
}