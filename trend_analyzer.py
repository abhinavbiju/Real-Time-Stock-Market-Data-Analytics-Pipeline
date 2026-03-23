"""
Trend Analyzer (AWS Lambda)

Triggered by DynamoDB Streams on the stock-data table.
For every new stock record:
  1. Queries the last 20 closing prices for that symbol.
  2. Computes SMA-5 (short-term) and SMA-20 (long-term).
  3. Detects bullish / bearish crossovers.
  4. Publishes buy/sell signal alerts to an SNS topic.
"""

import os
import logging
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

log = logging.getLogger()
log.setLevel(logging.INFO)

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "stock-data")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
SMA_SHORT = 5
SMA_LONG = 20

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)
sns = boto3.client("sns")


def lambda_handler(event, context):
    """Process DynamoDB Stream events and check for SMA crossovers."""
    symbols_checked = set()

    for record in event["Records"]:
        if record["eventName"] not in ("INSERT", "MODIFY"):
            continue

        new_image = record["dynamodb"].get("NewImage", {})
        symbol = new_image.get("symbol", {}).get("S")
        if not symbol or symbol in symbols_checked:
            continue

        symbols_checked.add(symbol)

        try:
            signal = _analyze(symbol)
            if signal:
                _publish_alert(signal)
        except Exception:
            log.exception("Error analyzing %s", symbol)

    log.info("Checked %d symbol(s)", len(symbols_checked))
    return {"statusCode": 200, "symbolsChecked": len(symbols_checked)}


def _get_recent_closes(symbol: str, limit: int = SMA_LONG) -> list[Decimal]:
    """Fetch the most recent closing prices for a symbol from DynamoDB."""
    response = table.query(
        KeyConditionExpression=Key("symbol").eq(symbol),
        ScanIndexForward=False,
        Limit=limit,
        ProjectionExpression="close, trade_date",
    )

    items = response.get("Items", [])
    return [item["close"] for item in items]


def _sma(prices: list[Decimal], window: int) -> Decimal | None:
    """Compute a Simple Moving Average over the given window."""
    if len(prices) < window:
        return None
    subset = prices[:window]
    return sum(subset) / Decimal(window)


def _analyze(symbol: str) -> dict | None:
    """Compute SMA-5 and SMA-20 and detect a crossover signal.

    Returns a signal dict if a crossover is detected, otherwise None.
    A crossover is identified by comparing the current and previous
    SMA relationship:
      - BUY:  SMA-5 crosses above SMA-20
      - SELL: SMA-5 crosses below SMA-20
    """
    closes = _get_recent_closes(symbol, limit=SMA_LONG + 1)
    if len(closes) < SMA_LONG + 1:
        log.info(
            "%s: only %d data points, need %d for crossover detection",
            symbol,
            len(closes),
            SMA_LONG + 1,
        )
        return None

    current_prices = closes[:-1]  # most recent 20
    previous_prices = closes[1:]  # shifted back by 1

    sma5_now = _sma(current_prices, SMA_SHORT)
    sma20_now = _sma(current_prices, SMA_LONG)
    sma5_prev = _sma(previous_prices, SMA_SHORT)
    sma20_prev = _sma(previous_prices, SMA_LONG)

    if None in (sma5_now, sma20_now, sma5_prev, sma20_prev):
        return None

    bullish_cross = sma5_prev <= sma20_prev and sma5_now > sma20_now
    bearish_cross = sma5_prev >= sma20_prev and sma5_now < sma20_now

    if bullish_cross:
        return _build_signal(symbol, "BUY", sma5_now, sma20_now, current_prices[0])
    if bearish_cross:
        return _build_signal(symbol, "SELL", sma5_now, sma20_now, current_prices[0])

    return None


def _build_signal(
    symbol: str,
    action: str,
    sma5: Decimal,
    sma20: Decimal,
    latest_close: Decimal,
) -> dict:
    return {
        "symbol": symbol,
        "action": action,
        "latest_close": float(latest_close),
        "sma5": float(round(sma5, 4)),
        "sma20": float(round(sma20, 4)),
    }


def _publish_alert(signal: dict) -> None:
    """Send a buy/sell alert to the SNS topic."""
    if not SNS_TOPIC_ARN:
        log.warning("SNS_TOPIC_ARN not set — skipping alert publish")
        return

    action = signal["action"]
    symbol = signal["symbol"]
    emoji = "\u2B06\uFE0F" if action == "BUY" else "\u2B07\uFE0F"

    subject = f"{emoji} {action} Signal: {symbol}"
    message = (
        f"Stock Trend Alert\n"
        f"{'=' * 30}\n"
        f"Symbol:       {symbol}\n"
        f"Signal:       {action}\n"
        f"Latest Close: ${signal['latest_close']:.2f}\n"
        f"SMA-5:        ${signal['sma5']:.4f}\n"
        f"SMA-20:       ${signal['sma20']:.4f}\n"
        f"\n"
        f"{'Bullish crossover — SMA-5 crossed above SMA-20.' if action == 'BUY' else 'Bearish crossover — SMA-5 crossed below SMA-20.'}"
    )

    sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
    log.info("Published %s alert for %s", action, symbol)
