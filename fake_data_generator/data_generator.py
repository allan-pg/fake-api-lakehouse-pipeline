import json
import logging
import random
from datetime import timezone
from pathlib import Path

from faker import Faker


# Logging configuration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# Configuration
fake = Faker()

# Create path to data directory
DATA_DIR = Path("data")

CUSTOMERS_FILE = DATA_DIR / "customers.json"
ORDERS_FILE = DATA_DIR / "orders.json"

# Number of records to generate
NUMBER_OF_CUSTOMERS = 1000
NUMBER_OF_ORDERS = 10000


# Generate customers

def generate_customers():

    logger.info("Starting customer generation")

    customers = []

    for customer_id in range(1, NUMBER_OF_CUSTOMERS + 1):

        created_at = fake.date_time_between(
            start_date="-2y",
            end_date="-30d",
            tzinfo=timezone.utc
        )

        customer = {
            "customer_id": customer_id,
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "country": fake.country()
            },
            "created_at": created_at.isoformat(),
            "updated_at": created_at.isoformat()
        }

        customers.append(customer)

    logger.info(
        "Customer generation complete | records=%s",
        len(customers)
    )

    return customers


# Generate orders

def generate_orders(customers):

    logger.info("Starting order generation")

    orders = []

    statuses = [
        "pending",
        "processing",
        "completed",
        "cancelled"
    ]

    products = [
        {
            "product_id": 101,
            "product_name": "Laptop",
            "price": 1200.00
        },
        {
            "product_id": 102,
            "product_name": "Wireless Mouse",
            "price": 25.50
        },
        {
            "product_id": 103,
            "product_name": "Keyboard",
            "price": 75.00
        },
        {
            "product_id": 104,
            "product_name": "Monitor",
            "price": 350.00
        },
        {
            "product_id": 105,
            "product_name": "Headphones",
            "price": 120.00
        }
    ]

    for order_id in range(1, NUMBER_OF_ORDERS + 1):

        customer = random.choice(customers)

        created_at = fake.date_time_between(
            start_date="-1y",
            end_date="now",
            tzinfo=timezone.utc
        )

        status = random.choice(statuses)

        items = []

        number_of_items = random.randint(1, 4)

        for _ in range(number_of_items):

            product = random.choice(products)

            item = {
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "quantity": random.randint(1, 5),
                "unit_price": product["price"]
            }

            items.append(item)

        order = {
            "order_id": order_id,

            "customer": {
                "customer_id": customer["customer_id"],
                "name": customer["name"],
                "email": customer["email"]
            },

            "order": {
                "status": status,
                "items": items
            },

            "audit": {
                "created_at": created_at.isoformat(),
                "updated_at": created_at.isoformat()
            }
        }

        orders.append(order)

    logger.info(
        "Order generation complete | records=%s",
        len(orders)
    )

    return orders


# Save JSON

def save_json(data, file_path):

    logger.info(
        "Saving JSON | file=%s | records=%s",
        file_path,
        len(data)
    )

    try:

        with open(
            file_path,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                data,
                file,
                indent=2
            )

        logger.info(
            "JSON successfully saved | file=%s",
            file_path
        )

    except OSError:

        logger.exception(
            "Failed to save JSON | file=%s",
            file_path
        )

        raise



# Main

def main():

    logger.info("Starting data generation pipeline")

    try:

        DATA_DIR.mkdir(
            exist_ok=True
        )

        logger.info(
            "Data directory ready | path=%s",
            DATA_DIR
        )

        # Generate customers
        customers = generate_customers()

        # Generate orders
        orders = generate_orders(customers)

        # Save customers
        save_json(
            customers,
            CUSTOMERS_FILE
        )

        # Save orders
        save_json(
            orders,
            ORDERS_FILE
        )

        logger.info(
            "Data generation completed successfully"
        )

        logger.info(
            "Customers file | path=%s | records=%s",
            CUSTOMERS_FILE,
            len(customers)
        )

        logger.info(
            "Orders file | path=%s | records=%s",
            ORDERS_FILE,
            len(orders)
        )

    except Exception:

        logger.exception(
            "Data generation pipeline failed"
        )

        raise


if __name__ == "__main__":
    main()