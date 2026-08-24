import requests as r
import json
from utils.s3_bucket import s3_client, ClientError
from datetime import datetime, timezone

from utils.logging_config import get_logger
from dotenv import load_dotenv
import os


logger = get_logger(__name__)


# Load environment variables from .env file
load_dotenv()


#get the url
API_URL = os.environ["url"]


# different end points to fetch data from
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
        "watermark_field": "updated_at"
    }
}


# list of buckets to create in s3
BUCKETS = ["fake-api-lakehouse-landing-2026"]


# create an s3 bucket to store your json files
def create_s3_bucket():

    existing_buckets = [
        bucket["Name"]
        for bucket in s3_client.list_buckets()["Buckets"]
    ]

    # create an s3 bucket if doesnt exist in your aws
    for bucket in BUCKETS:

        try:

            if bucket in existing_buckets:

                logger.info(
                    "Bucket already exists in S3: %s",
                    bucket
                )

            else:

                s3_client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={
                        "LocationConstraint": "eu-north-1"
                    }
                )

                logger.info(
                    "Created bucket: %s",
                    bucket
                )

        except ClientError:

            logger.exception(
                "Error creating bucket: %s",
                bucket
            )

            raise

def get_nested_value(record, field_path):
    """
    Get a value from a record using a field path.
    """

    value = record

    for field in field_path.split("."):
        value = value[field]

    return value

def get_max_update_date(records, watermark_field):
    """
    Get the maximum updated_at datetime from the records.
    """

    if not records:
        return None

    max_update_date = None

    for record in records:

        update_date = datetime.fromisoformat(
            get_nested_value(record, watermark_field))

        if (max_update_date is None or update_date > max_update_date):
            max_update_date = update_date

    return max_update_date

# historical load that loads all the data to S3 then we can do incremental loads
def extract_and_land(url, endpoints, bucket_name):
    """
    Extract data from multiple API endpoints
    and land each record as a JSON file in S3.
    """

    endpoint_watermarks = {}

    for endpoint, config in endpoints.items():

        id_field = config["id_field"]
        file_prefix = config["file_prefix"]

        logger.info("Starting extraction | endpoint=%s", endpoint)

        # Fetch endpoint

        try:

            response = r.get(f"{url}/{endpoint}", timeout=30)

        except r.RequestException:

            logger.exception("API request failed | endpoint=%s", endpoint)

            raise

        # Validate response

        if response.status_code != 200:

            logger.error(
                "API request failed | endpoint=%s | "
                "status_code=%s | response=%s",
                endpoint,
                response.status_code,
                response.text
            )

            raise Exception(
                f"Failed to fetch {endpoint} | "
                f"status_code={response.status_code} | "
                f"response={response.text}"
            )

        # Parse response

        try:

            data_field = config["data_field"]

            response_data = response.json()

            if data_field not in response_data:

                raise KeyError(f"Expected '{data_field}' field missing from {endpoint}")

            records = response_data[data_field]

            if not isinstance(records, list):

                raise TypeError(
                    f"Expected '{data_field}' to be a list for {endpoint}, "
                    f"got {type(records).__name__}"
                )

        except ValueError:

            logger.exception("Failed to parse response as JSON | endpoint=%s", endpoint)

            raise

        except (KeyError, TypeError):

            logger.exception("Invalid API response structure | endpoint=%s", endpoint)

            raise

        logger.info(
            "Data fetched | endpoint=%s | records=%s",
            endpoint,
            len(records)
        )

        # Ingestion timestamp

        ingestion_time = datetime.now(timezone.utc)

        ingestion_date = ingestion_time.strftime("%Y-%m-%d")

        ingestion_timestamp = ingestion_time.strftime(
            "%Y%m%dT%H%M%S"
        )

        # Write records to S3

        for record in records:

            record_id = record[id_field]

            s3_key = (
                f"{endpoint}/"
                f"ingestion_date={ingestion_date}/"
                f"{file_prefix}_{record_id}_"
                f"{ingestion_timestamp}.json"
            )

            try:

                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=json.dumps(record),
                    ContentType="application/json"
                )

            except Exception:

                logger.exception(
                    "Failed to upload record | "
                    "endpoint=%s | id=%s | s3_key=%s",
                    endpoint,
                    record_id,
                    s3_key
                )

                raise

            logger.info(
                "Record uploaded | endpoint=%s | "
                "id=%s | s3_key=%s",
                endpoint,
                record_id,
                s3_key
            )

        # Get MAX updated_at only after
        # all records were successfully uploaded

        max_update_date = get_max_update_date(records, config["watermark_field"])

        endpoint_watermarks[endpoint] = max_update_date

        logger.info(
            "Endpoint completed | endpoint=%s | "
            "records=%s | max_updated_at=%s",
            endpoint,
            len(records),
            max_update_date
        )

    return endpoint_watermarks


def main():

    logger.info("Started source to landing pipeline")

    try:

        # create an s3 bucket
        create_s3_bucket()

        # extract customers & orders to s3
        endpoint_watermarks = extract_and_land(
            url=API_URL,
            endpoints=ENDPOINTS,
            bucket_name=BUCKETS[0]
        )

        # Save watermark only after the
        # extraction and landing succeeds

        for endpoint, watermark in endpoint_watermarks.items():

            save_watermark(
                bucket_name=BUCKETS[0],
                endpoint=endpoint,
                watermark=watermark
            )

        logger.info("Source to landing pipeline completed successfully")

    except Exception:

        logger.exception("Source to landing pipeline failed")

        raise


if __name__ == "__main__":
    main()

