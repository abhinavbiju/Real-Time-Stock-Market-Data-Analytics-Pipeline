"""
Kinesis Stream Processor (AWS Lambda)

Triggered by Kinesis Data Streams. For each batch of stock records:
  1. Stores the raw JSON to S3 (partitioned by date and symbol).
  2. Writes the processed record to DynamoDB for real-time lookups.
"""

import json
import os
import base64
import logging
from datetime import datetime, timezone

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("RAW_DATA_BUCKET", "stock-market-raw-data")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "stock-data")

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)


def lambda_handler(event, context):
    """Process a batch of Kinesis records."""
    records_written = 0

    for kinesis_record in event["Records"]:
        try:
            payload = base64.b64decode(kinesis_record["kinesis"]["data"])
            record = json.loads(payload)
            _store_to_s3(record)
            _store_to_dynamodb(record)
            records_written += 1
        except Exception:
            log.exception(
                "Failed to process record %s",
                kinesis_record.get("eventID", "unknown"),
            )

    log.info("Processed %d / %d records", records_written, len(event["Records"]))
    return {"statusCode": 200, "processed": records_written}


def _store_to_s3(record: dict) -> None:
    """Write the raw JSON record to S3, partitioned by date and symbol."""
    trade_dt = datetime.fromisoformat(record["trade_date"])
    now = datetime.now(timezone.utc)

    key = (
        f"raw/"
        f"year={trade_dt.year}/"
        f"month={trade_dt.month:02d}/"
        f"day={trade_dt.day:02d}/"
        f"{record['symbol']}_{now.strftime('%Y%m%dT%H%M%S%f')}.json"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )


def _store_to_dynamodb(record: dict) -> None:
    """Write the processed record to DynamoDB.

    Partition key: symbol
    Sort key:      trade_date (ISO-8601)
    """
    from decimal import Decimal

    item = {
        "symbol": record["symbol"],
        "trade_date": record["trade_date"],
        "open": Decimal(str(record["open"])),
        "high": Decimal(str(record["high"])),
        "low": Decimal(str(record["low"])),
        "close": Decimal(str(record["close"])),
        "volume": record["volume"],
        "ingested_at": record.get(
            "ingested_at", datetime.now(timezone.utc).isoformat()
        ),
    }

    table.put_item(Item=item)
