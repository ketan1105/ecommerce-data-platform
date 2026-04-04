import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import TypeOptions
import json
import logging
from datetime import datetime

# Config 
PROJECT_ID        = "arboreal-tracer-491416-r1"
SUBSCRIPTION      = f"projects/{PROJECT_ID}/subscriptions/orders-subscription"
BQ_TABLE          = f"{PROJECT_ID}:ecommerce_raw.orders"
LARGE_ORDER_LIMIT = 5000.0   # orders above ₹5000 flagged as large

# Transform functions
def parse_message(message):
    """Convert raw Pub/Sub bytes → Python dict."""
    try:
        record = json.loads(message.decode("utf-8"))
        return record
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error(f"Failed to parse message: {e}")
        return None

def enrich_order(record):
    """Add computed fields to each order."""
    order_total = record.get("order_total", 0)

    # Calculate discount based on order total
    if order_total >= 10000:
        discount_pct = 15.0
    elif order_total >= 5000:
        discount_pct = 10.0
    elif order_total >= 2000:
        discount_pct = 5.0
    else:
        discount_pct = 0.0

    record["discount_pct"]    = discount_pct
    record["is_large_order"]  = order_total >= LARGE_ORDER_LIMIT
    record["processed_at"]    = datetime.utcnow().isoformat() + "Z"

    # Convert items list → JSON string for BQ storage
    if isinstance(record.get("items"), list):
        record["items"] = json.dumps(record["items"])

    return record

def is_valid(record):
    """Filter out None records and those missing critical fields."""
    if record is None:
        return False
    required_fields = ["order_id", "customer_id", "order_total", "status"]
    for field in required_fields:
        if field not in record or record[field] is None:
            logging.warning(f"Record missing field '{field}': {record}")
            return False
    return True

# BigQuery schema
BQ_SCHEMA = {
    "fields": [
        {"name": "order_id",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "customer_id",     "type": "STRING",    "mode": "REQUIRED"},
        {"name": "order_total",     "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "status",          "type": "STRING",    "mode": "REQUIRED"},
        {"name": "payment_method",  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "items",           "type": "STRING",    "mode": "NULLABLE"},
        {"name": "discount_pct",    "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "is_large_order",  "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "processed_at",    "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

#  Pipeline 
def run():
    options = PipelineOptions([
        f"--project={PROJECT_ID}",
        "--runner=DirectRunner",
        "--streaming",
    ])
    options.view_as(StandardOptions).streaming = True

    # Tell Beam to skip type checking — avoids false positives
    pipeline = beam.Pipeline(options=options)
    pipeline._options.view_as(
        beam.options.pipeline_options.TypeOptions
    ).pipeline_type_check = False

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub"  >> ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse JSON"         >> beam.Map(parse_message)
            | "Filter valid"       >> beam.Filter(is_valid)
            | "Enrich order"       >> beam.Map(enrich_order)
            | "Write to BigQuery"  >> WriteToBigQuery(
                table              = BQ_TABLE,
                schema             = BQ_SCHEMA,
                write_disposition  = BigQueryDisposition.WRITE_APPEND,
                create_disposition = BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()