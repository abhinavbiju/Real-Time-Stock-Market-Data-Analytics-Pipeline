"""
Stock Market Data Producer

Continuously fetches real-time stock data from Yahoo Finance
and publishes records to an Amazon Kinesis Data Stream.
"""

import json
import time
import logging
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

STREAM_NAME = "stock-market-stream"
REGION = "us-east-1"
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM"]
FETCH_INTERVAL_SECONDS = 60

kinesis = boto3.client("kinesis", region_name=REGION)


def fetch_stock_data(symbols: list[str]) -> list[dict]:
    """Download the latest 1-minute bar for each symbol via yfinance."""
    records = []
    tickers = yf.Tickers(" ".join(symbols))

    for symbol in symbols:
        try:
            hist = tickers.tickers[symbol].history(period="1d", interval="1m")
            if hist.empty:
                log.warning("No data returned for %s", symbol)
                continue

            latest = hist.iloc[-1]
            record = {
                "symbol": symbol,
                "open": float(round(Decimal(str(latest["Open"])), 4)),
                "high": float(round(Decimal(str(latest["High"])), 4)),
                "low": float(round(Decimal(str(latest["Low"])), 4)),
                "close": float(round(Decimal(str(latest["Close"])), 4)),
                "volume": int(latest["Volume"]),
                "trade_date": latest.name.isoformat(),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            records.append(record)
        except Exception:
            log.exception("Failed to fetch data for %s", symbol)

    return records


def publish_to_kinesis(records: list[dict]) -> None:
    """Send a batch of stock records to Kinesis using PutRecords."""
    if not records:
        return

    entries = [
        {
            "Data": json.dumps(record).encode("utf-8"),
            "PartitionKey": record["symbol"],
        }
        for record in records
    ]

    # Kinesis PutRecords supports up to 500 records per call
    for i in range(0, len(entries), 500):
        batch = entries[i : i + 500]
        response = kinesis.put_records(StreamName=STREAM_NAME, Records=batch)

        failed = response.get("FailedRecordCount", 0)
        if failed:
            log.warning("%d record(s) failed in batch starting at index %d", failed, i)
        else:
            log.info("Published %d record(s) to Kinesis", len(batch))


def main() -> None:
    log.info(
        "Starting producer — stream=%s, symbols=%s, interval=%ds",
        STREAM_NAME,
        SYMBOLS,
        FETCH_INTERVAL_SECONDS,
    )

    while True:
        try:
            records = fetch_stock_data(SYMBOLS)
            log.info("Fetched %d record(s) from Yahoo Finance", len(records))
            publish_to_kinesis(records)
        except Exception:
            log.exception("Unhandled error in producer loop")

        time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
