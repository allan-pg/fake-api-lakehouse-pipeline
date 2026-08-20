import json
import logging
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s")

logger = logging.getLogger(__name__)

API_URL = "http://127.0.0.1:8000/orders"
RAW_DIR = Path("raw")


def fetch_orders(page, updated_after=None):

    params = {
        "page": page
    }

    if updated_after is not None:
        params["updated_after"] = updated_after

    try:

        response = requests.get(
            API_URL,
            params=params,
            timeout=30)

        logger.info(
            "API request | page=%s | status=%s | url=%s",
            page,
            response.status_code,
            response.url)

        response.raise_for_status()

        return response.json()

    except requests.exceptions.HTTPError:

        logger.error(
            "HTTP error | page=%s | status=%s | url=%s | response=%s",
            page,
            response.status_code,
            response.url,
            response.text
        )

        raise

    except requests.exceptions.ConnectionError:

        logger.error(
            "Connection error | page=%s | url=%s",
            page,
            API_URL)

        raise

    except requests.exceptions.Timeout:

        logger.error(
            "Timeout | page=%s | url=%s",
            page,
            API_URL)

        raise

    except requests.exceptions.RequestException:

        logger.exception(
            "Unexpected request error | page=%s",
            page
        )

        raise

    except ValueError:

        logger.error(
            "Invalid JSON response | page=%s | url=%s | response=%s",
            page,
            response.url,
            response.text
        )

        raise

def extract_orders(updated_after=None):

    page = 1
    all_orders = []

    while True:

        logger.info(
            "Fetching page %s",
            page
        )

        response = fetch_orders(
            page=page,
            updated_after=updated_after
        )

        orders = response["data"]

        pagination = response["pagination"]

        logger.info(
            "Page extracted | page=%s | records=%s | "
            "api_page_size=%s | total_records=%s | has_next=%s",
            pagination["page"],
            len(orders),
            pagination["page_size"],
            pagination["total_records"],
            pagination["has_next"]
        )

        all_orders.extend(orders)

        if not pagination["has_next"]:

            logger.info(
                "No more pages | total records extracted=%s",
                len(all_orders)
            )

            break

        page += 1

    return all_orders


def save_raw_json(orders):

    RAW_DIR.mkdir(exist_ok=True)

    file_path = RAW_DIR / "orders.json"

    with open(
        file_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            orders,
            file,
            indent=2
        )

    logger.info(
        "Raw data saved | records=%s | file=%s",
        len(orders),
        file_path
    )


def main():

    logger.info("Starting order extraction")

    orders = extract_orders()

    save_raw_json(orders)

    logger.info(
        "Extraction completed successfully | records=%s",
        len(orders)
    )


if __name__ == "__main__":
    main()