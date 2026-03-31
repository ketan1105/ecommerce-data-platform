import json
import time
import random
import argparse
from datetime import datetime
from faker import Faker
from google.cloud import pubsub_v1

fake = Faker("en_IN")  # Indian locale — names, cities etc.

# Config
CATEGORIES   = ["Electronics", "Clothing", "Books", "Home & Kitchen", "Sports"]
STATUSES     = ["placed", "confirmed", "shipped", "delivered", "cancelled"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD"]

# Event generator
def generate_order_event() -> dict:
    num_items = random.randint(1, 5)
    items = []
    order_total = 0.0

    for _ in range(num_items):
        price = round(random.uniform(99, 9999), 2)
        qty   = random.randint(1, 3)
        items.append({
            "product_id" : f"PROD-{random.randint(1000, 1099)}",
            "price"      : price,
            "quantity"   : qty,
        })
        order_total += price * qty

    return {
        "order_id"        : f"ORD-{fake.uuid4()[:8].upper()}",
        "customer_id"     : f"CUST-{random.randint(1000, 1099)}",
        "items"           : items,
        "order_total"     : round(order_total, 2),
        "status"          : random.choice(STATUSES),
        "payment_method"  : random.choice(PAYMENT_METHODS),
        "event_timestamp" : datetime.utcnow().isoformat() + "Z",
    }

# Publisher
def publish_events(project_id: str, topic_id: str, num_events: int, delay: float):
    """Publish order events to Pub/Sub topic."""
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    print(f"Publishing to: {topic_path}")
    print(f"Events: {num_events} | Delay: {delay}s between each\n")

    for i in range(1, num_events + 1):
        event   = generate_order_event()
        payload = json.dumps(event).encode("utf-8")

        future = publisher.publish(
            topic_path,
            payload,
            # Pub/Sub supports message attributes — useful for filtering
            event_type ="order_event",
            source     ="order_simulator",
        )
        print(f"[{i}/{num_events}] Published order {event['order_id']} "
              f"| {event['status']} | ₹{event['order_total']:.2f} "
              f"| msg_id: {future.result()}")

        time.sleep(delay)

    print("\nDone. All events published.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce order event simulator")
    parser.add_argument("--project_id", required=True,  help="GCP project ID")
    parser.add_argument("--topic_id",   default="orders-topic")
    parser.add_argument("--num_events", type=int,   default=20)
    parser.add_argument("--delay",      type=float, default=0.5,
                        help="Seconds between events")
    args = parser.parse_args()

    publish_events(args.project_id, args.topic_id, args.num_events, args.delay)