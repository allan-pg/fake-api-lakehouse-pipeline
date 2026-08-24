import requests as r
import json

from utils.s3_bucket import s3_client, ClientError

from datetime import datetime, timezone, timedelta

from utils.logging_config import get_logger
from utils.config import LOOKBACK_HOURS, ENDPOINTS, BUCKETS, API_URL

from dotenv import load_dotenv


logger = get_logger(__name__)


# Load environment variables from .env file
load_dotenv()





# create an s3 bucket to store your json files if it doesn't exist
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

        update_date = datetime.fromisoformat(get_nested_value(record, watermark_field))

        if (max_update_date is None or update_date > max_update_date):

            max_update_date = update_date

    return max_update_date


def get_watermark(bucket_name, endpoint):
    """
    Read the last successful watermark for an endpoint from S3.
    """
    s3_key = f"control/watermarks/{endpoint}.json"

    try:

        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)

        watermark_data = json.loads(response["Body"].read().decode("utf-8"))

        watermark = datetime.fromisoformat(watermark_data["watermark"])

        logger.info("Watermark retrieved | endpoint=%s | watermark=%s", endpoint, watermark)

        return watermark

    except ClientError as e:

        error_code = e.response["Error"]["Code"]

        if error_code in ["NoSuchKey", "404"]:

            logger.info("No watermark found | endpoint=%s", endpoint)

            return None

        logger.exception("Failed to retrieve watermark | endpoint=%s", endpoint)

        raise


def extract_and_land(url, endpoints, bucket_name, lookback_hours):
    """
    Extract incremental data from multiple API endpoints
    and land each record as a JSON file in S3.

    The extraction uses the last successful watermark
    with a configurable lookback window for late-arriving data.
    """

    endpoint_watermarks = {}

    for endpoint, config in endpoints.items():

        id_field = config["id_field"]

        file_prefix = config["file_prefix"]

        watermark_field = config["watermark_field"]

        logger.info("Starting incremental extraction | endpoint=%s", endpoint)

        # Get the last successful watermark

        last_watermark = get_watermark(bucket_name=bucket_name, endpoint=endpoint)

        # Calculate the extraction timestamp

        if last_watermark is not None:

            extraction_start = (last_watermark - timedelta(hours=lookback_hours))

            logger.info( "Using incremental watermark | "
                "endpoint=%s | last_watermark=%s | "
                "lookback_hours=%s | extraction_start=%s",
                endpoint,
                last_watermark,
                lookback_hours,
                extraction_start
            )

        else:
	    # if no watermark is found perform a full historical load
            extraction_start = None

            logger.info(
                "No previous watermark found | "
                "endpoint=%s | performing full extraction", endpoint)

        # Keep collecting records from all pages
        all_records = []

	#start from page 1 and each page has 100 items
        page = 1
        page_size = 100

        while True:

            logger.info(
                "Fetching page | endpoint=%s | "
                "page=%s | page_size=%s",
                endpoint,
                page,
                page_size
            )

            # Fetch endpoint

            try:

                request_url = f"{url}/{endpoint}"

                params = {
                    "page": page,
                    "page_size": page_size
                }

                # Only send updated_after when
                # we have an existing watermark

                if extraction_start is not None:

                    params["updated_after"] = (extraction_start.isoformat())

                response = r.get(
                    request_url,
                    params=params,
                    timeout=30
                )

            except r.RequestException:

                logger.exception(
                    "API request failed | "
                    "endpoint=%s | page=%s",
                    endpoint,
                    page
                )

                raise

            # Validate response

            if response.status_code != 200:

                logger.error(
                    "API request failed | "
                    "endpoint=%s | page=%s | "
                    "status_code=%s | response=%s",
                    endpoint,
                    page,
                    response.status_code,
                    response.text
                )

                raise Exception(
                    f"Failed to fetch {endpoint} | "
                    f"page={page} | "
                    f"status_code={response.status_code} | "
                    f"response={response.text}"
                )

            # Parse response

            try:

                data_field = config["data_field"]

                response_data = response.json()

                if data_field not in response_data:

                    raise KeyError(
                        f"Expected '{data_field}' field missing "
                        f"from {endpoint}"
                    )

                records = response_data[data_field]

                if not isinstance(records, list):

                    raise TypeError(
                        f"Expected '{data_field}' to be a list "
                        f"for {endpoint}, "
                        f"got {type(records).__name__}"
                    )

            except ValueError:

                logger.exception(
                    "Failed to parse response as JSON | "
                    "endpoint=%s | page=%s",
                    endpoint,
                    page
                )

                raise

            except (KeyError, TypeError):

                logger.exception(
                    "Invalid API response structure | "
                    "endpoint=%s | page=%s",
                    endpoint,
                    page
                )

                raise

            logger.info(
                "Data fetched | endpoint=%s | "
                "page=%s | records=%s",
                endpoint,
                page,
                len(records)
            )

            # Add this page's records to all_records

            all_records.extend(records)

            # Check pagination information

            pagination = response_data.get(
                "pagination",
                {}
            )

            has_next = pagination.get("has_next", False)

            if not has_next:

                break
            page += 1

        logger.info(
            "All pages fetched | endpoint=%s | "
            "total_records=%s",
            endpoint,
            len(all_records)
        )

        # Ingestion timestamp

        ingestion_time = datetime.now(timezone.utc)
        ingestion_date = ingestion_time.strftime("%Y-%m-%d")

        ingestion_timestamp = ingestion_time.strftime("%Y%m%dT%H%M%S")

        # Write records to S3

        for record in all_records:

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

        # Calculate the new watermark

        new_watermark = get_max_update_date(all_records, watermark_field )

        endpoint_watermarks[endpoint] = new_watermark

        logger.info(
            "Endpoint extraction completed | "
            "endpoint=%s | records=%s | "
            "new_watermark=%s",
            endpoint,
            len(all_records),
            new_watermark
        )

    return endpoint_watermarks


def save_watermark(bucket_name, endpoint, watermark):
    """
    Save the latest successful watermark to the S3 control area.
    """

    if watermark is None:

        logger.info("No watermark to save | endpoint=%s", endpoint)

        return

    watermark_data = {
        "endpoint": endpoint,
        "watermark": watermark.isoformat()
    }

    s3_key = f"control/watermarks/{endpoint}.json"

    try:

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(
                watermark_data,
                indent=2
            ),
            ContentType="application/json"
        )

        logger.info(
            "Watermark saved | endpoint=%s | "
            "watermark=%s | s3_key=%s",
            endpoint,
            watermark,
            s3_key
        )

    except Exception:

        logger.exception("Failed to save watermark | endpoint=%s", endpoint)
        raise


def main():

    logger.info("Started source to landing pipeline")

    try:

        # create an s3 bucket

        create_s3_bucket()

        # extract customers & orders to s3

        endpoint_watermarks = extract_and_land(
            url=API_URL,
            endpoints=ENDPOINTS,
            bucket_name=BUCKETS[0],
            lookback_hours=LOOKBACK_HOURS
        )

        # Save watermark only after the extraction and landing succeeds

        for endpoint, watermark in endpoint_watermarks.items():

            save_watermark(
                bucket_name=BUCKETS[0],
                endpoint=endpoint,
                watermark=watermark
            )

        logger.info("Source to landing pipeline completed successfully")

    except Exception:

        logger.exception( "Source to landing pipeline failed")
        raise


if __name__ == "__main__":

    main()