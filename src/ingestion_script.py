import json
import random
import time
from datetime import datetime, timezone
import boto3
from faker import Faker

fake = Faker()

# AWS Configuration
S3_BUCKET_NAME = "cdc-inventory-sf"
S3_PREFIX = "raw-cdc-data"
REGION_NAME = "us-east-1"

s3_client = boto3.client("s3", region_name=REGION_NAME)

ACTIONS = ["INSERT", "UPDATE", "DELETE"]
CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Toys"]


def generate_inventory_event():
    """Generates a single CDC inventory event record."""
    action = random.choices(ACTIONS, weights=[0.2, 0.7, 0.1])[0]

    event = {
        "event_id": fake.uuid4(),
        "event_type": action,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "seller_id": f"SEL-{random.randint(100, 150)}",
            "product_id": f"PROD-{random.randint(1000, 1050)}",
            "product_name": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "stock_quantity": random.randint(0, 500) if action != "DELETE" else 0,
            "unit_price": round(random.uniform(5.00, 500.00), 2),
            "warehouse_location": fake.city()
        }
    }
    return event


def stream_events_to_s3(batch_size=10, interval_seconds=30):
    """Generates a batch of events and pushes a JSON file to S3 periodically."""
    print(f"Starting CDC event generator. Writing to s3://{S3_BUCKET_NAME}/{S3_PREFIX}/...")

    try:
        while True:
            events = [generate_inventory_event() for _ in range(batch_size)]

            # File naming convention based on timestamp for easy partition debugging
            now = datetime.now(timezone.utc)
            file_name = f"{S3_PREFIX}/year={now.year}/month={now.month:02d}/day={now.day:02d}/cdc_batch_{int(now.timestamp())}.json"

            # Upload JSON payload
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(events, indent=2),
                ContentType="application/json"
            )

            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S UTC')}] Uploaded {batch_size} CDC records to {file_name}")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nCDC Event Generator stopped by user.")


if __name__ == "__main__":
    stream_events_to_s3(batch_size=5, interval_seconds=10)