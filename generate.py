import random
from faker import Faker
import pandas as pd
from datetime import datetime

fake = Faker()

PRODUCTS = [
    "Laptop",
    "Monitor",
    "Keyboard",
    "Mouse",
    "Tablet",
    "Phone",
    "Printer",
    "Headset"
]

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "PayPal",
    "Bank Transfer"
]

COUNTRIES = [
    "USA",
    "Canada",
    "UK",
    "Germany",
    "India",
    "Australia"
]


def generate_sales_record():
    quantity = random.randint(1, 5)

    unit_price = round(random.uniform(50, 2000), 2)

    total_amount = round(quantity * unit_price, 2)

    return {
        "transaction_id": fake.uuid4(),
        "customer_id": fake.uuid4(),
        "customer_name": fake.name(),
        "email": fake.email(),
        "country": random.choice(COUNTRIES),
        "product": random.choice(PRODUCTS),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "transaction_timestamp": fake.date_time_this_year()
    }