import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker


fake = Faker()

DATA_DIR = Path("data")

CUSTOMERS_FILE = DATA_DIR / "customers.json"
ORDERS_FILE = DATA_DIR / "orders.json"

# set the number of customers and orders you need to generate
NUMBER_OF_CUSTOMERS = 1000
NUMBER_OF_ORDERS = 10000

# generate customer jsons
def generate_customers():
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

    return customers

# generate order jsons
def generate_orders(customers):
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

    return orders

# save the json files in data dir
def save_json(data, file_path):

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            indent=2
        )

def main():

    DATA_DIR.mkdir(exist_ok=True)

    print("Generating customers...")

    customers = generate_customers()

    print(f"Generated {len(customers)} customers")

    print("Generating orders...")

    orders = generate_orders(customers)

    print(f"Generated {len(orders)} orders")

    save_json(
        customers,
        CUSTOMERS_FILE
    )

    save_json(
        orders,
        ORDERS_FILE
    )

    print()
    print("Data generation complete.")
    print(f"Customers: {CUSTOMERS_FILE}")
    print(f"Orders: {ORDERS_FILE}")


if __name__ == "__main__":
    main()