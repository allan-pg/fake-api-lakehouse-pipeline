import json
import random
from datetime import timezone
from pathlib import Path

from faker import Faker

from utils.logging_config import get_logger

logger = get_logger(__name__)


# Configuration
fake = Faker()

# Create path to data directory
DATA_DIR = Path("data")

CUSTOMERS_FILE = DATA_DIR / "customers.json"
ORDERS_FILE = DATA_DIR / "orders.json"

"""
# Initial Number of records to generate
NUMBER_OF_CUSTOMERS = 1000
NUMBER_OF_ORDERS = 10000
"""

# incremental records added 
NUMBER_OF_NEW_CUSTOMERS = 4
NUMBER_OF_NEW_ORDERS = 10

# read the existing data
def load_existing_json(file_path):

    if not file_path.exists():
        logger.info(
            "File does not exist | file=%s | starting with empty dataset",
            file_path
        )
        return []

    try:
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        logger.info("Existing data loaded | file=%s | records=%s", file_path, len(data))

        return data

    except (OSError, json.JSONDecodeError):

        logger.exception("Failed to load existing JSON | file=%s", file_path)

        raise


# Generate customers

def generate_customers(start_id, number_of_customers):

    logger.info("Starting customer generation | start_id=%s | records=%s", start_id, number_of_customers)

    customers = []

    for customer_id in range(start_id, start_id + number_of_customers):

        created_at = fake.date_time_between(
            start_date="-30d",
            end_date="now",
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

    return customers


# Generate orders
def generate_orders(customers, start_id, number_of_orders):

    logger.info("Starting order generation | start_id=%s | records=%s", start_id,  number_of_orders)

    orders = []

    statuses = ["pending", "processing", "completed", "cancelled"]

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

    for order_id in range(start_id, start_id + number_of_orders):

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

    logger.info("Order generation complete | records=%s", len(orders))

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

        logger.info("JSON successfully saved | file=%s", file_path)

    except OSError:

        logger.exception("Failed to save JSON | file=%s", file_path)

        raise



# Main to generate fake data and save it locally for the fast api app since we are not saving it in a no sql db

def main():

    logger.info("Starting incremental data generation")

    try:

        DATA_DIR.mkdir(exist_ok=True)

        # Load existing data
        existing_customers = load_existing_json(CUSTOMERS_FILE)
        existing_orders = load_existing_json(ORDERS_FILE)

        # Determine next IDs
        next_customer_id = (
            max(
                customer["customer_id"]
                for customer in existing_customers
            ) + 1
            if existing_customers
            else 1
        )

        next_order_id = (
            max(
                order["order_id"]
                for order in existing_orders
            ) + 1
            if existing_orders
            else 1
        )

        # Generate NEW customers
        new_customers = generate_customers(
            start_id=next_customer_id,
            number_of_customers=NUMBER_OF_NEW_CUSTOMERS
        )

        # Combine existing + new
        customers = existing_customers + new_customers

        # Generate NEW orders
        # Include all customers so orders can belong to
        # either old or newly created customers
        new_orders = generate_orders(customers=customers, start_id=next_order_id, number_of_orders=NUMBER_OF_NEW_ORDERS)

        orders = existing_orders + new_orders

        # Save combined datasets
        save_json(customers, CUSTOMERS_FILE)
        save_json(orders, ORDERS_FILE)

        logger.info(
            "Incremental generation completed | "
            "new_customers=%s | new_orders=%s",
            len(new_customers),
            len(new_orders)
        )

    except Exception:
        logger.exception("Incremental data generation failed")

        raise


if __name__ == "__main__":
    main()