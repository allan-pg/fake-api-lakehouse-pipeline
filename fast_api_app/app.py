import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Query

# create an api application
app = FastAPI(
    title="Fake Orders API",
    description="API for practising incremental data extraction",
    version="1.0.0"
)

# path for getting jsons files from the data directory
DATA_DIR = Path("data")

CUSTOMERS_FILE = DATA_DIR / "customers.json"
ORDERS_FILE = DATA_DIR / "orders.json"

# function to read json files
def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)

# create a root endpoint
@app.get("/")
def root():
    return {
        "message": "Fake Orders API is running"
    }

# create a health check endpoint
@app.get("/health")
def health():
    return {
        "status": "healthy"
    }

# create a customers endpoint
# create a customers endpoint
@app.get("/customers")
def get_customers(
    updated_after: datetime | None = None
):

    customers = load_json(CUSTOMERS_FILE)

    if updated_after is not None:

        # Make sure the incoming timestamp is timezone-aware
        if updated_after.tzinfo is None:
            updated_after = updated_after.replace(
                tzinfo=timezone.utc
            )

        filtered_customers = []

        for customer in customers:

            customer_updated_at = datetime.fromisoformat(
                customer["updated_at"]
            )

            if customer_updated_at > updated_after:
                filtered_customers.append(customer)

        customers = filtered_customers

    return {
        "data": customers,
        "total_records": len(customers)
    }


# create orders endpoint and have them paginated such that 1 page has 100 orders
@app.get("/orders")
def get_orders(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    updated_after: datetime | None = None
):

    orders = load_json(ORDERS_FILE)
    # check if the url has been filtered based on update at to only return records after that datetime
    if updated_after is not None:

        # Make sure the incoming timestamp is timezone-aware
        if updated_after.tzinfo is None:
            updated_after = updated_after.replace(
                tzinfo=timezone.utc
            )

        filtered_orders = []

        for order in orders:

            order_updated_at = datetime.fromisoformat(
                order["audit"]["updated_at"]
            )

            if order_updated_at > updated_after:
                filtered_orders.append(order)

        orders = filtered_orders

    total_records = len(orders)

    start = (page - 1) * page_size
    end = start + page_size

    paginated_orders = orders[start:end]

    total_pages = (
        total_records + page_size - 1
    ) // page_size

    return {
        "data": paginated_orders,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_records": total_records,
            "total_pages": total_pages,
            "has_next": page < total_pages
        }
    }