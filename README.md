# Real-Time Stock Market Data Analytics Pipeline on AWS

A serverless, event-driven pipeline that ingests live stock market data, processes it in real time, stores it for historical analysis, and delivers actionable trend alerts via SMS/Email.

## Architecture Overview

| Step | Component | Role |
|------|-----------|------|
| 1 | **Python Script** | Continuously fetches real-time stock data from APIs (e.g. yfinance) and pushes records into Kinesis |
| 2 | **Amazon Kinesis Data Streams** | Acts as the real-time ingestion pipeline, buffering incoming stock records |
| 3 | **AWS Lambda (Processor)** | Triggered by Kinesis — cleans, transforms, and routes data to S3 and DynamoDB |
| 4 | **Amazon S3 (Raw Data)** | Stores raw stock data for historical analysis and batch querying |
| 5 | **AWS Glue Data Catalog** | Crawls S3 data to build a structured schema for Athena |
| 6 | **Amazon Athena** | Serverless SQL engine for ad-hoc analytical queries over the cataloged data |
| 7 | **Amazon S3 (Query Results)** | Dedicated bucket for Athena query output |
| 8 | **Amazon DynamoDB** | Stores processed stock records for low-latency, real-time lookups |
| 9 | **AWS Lambda (Trend Analyzer)** | Triggered by DynamoDB Streams — computes moving averages (SMA-5, SMA-20) and detects buy/sell signals |
| 10 | **Amazon SNS** | Delivers stock trend alerts to subscribers via Email and SMS |

## Data Flow

```
User ──▶ Python Script ──▶ Kinesis Data Streams ──▶ Lambda (Processor)
                                                        │
                                    ┌───────────────────┼───────────────────┐
                                    ▼                                       ▼
                              Amazon S3                              DynamoDB
                                    │                                       │
                          Glue Crawler                          DynamoDB Streams
                                    │                                       │
                                    ▼                                       ▼
                          Athena Queries                     Lambda (Trend Analyzer)
                                    │                                       │
                                    ▼                                       ▼
                           S3 (Results)                              Amazon SNS
                                                                        │
                                                                 Email / SMS Alerts
```

## Prerequisites

- AWS account with appropriate IAM permissions
- Python 3.9+
- AWS CLI configured with valid credentials
- Terraform or AWS CloudFormation (if using IaC for provisioning)

## AWS Services Used

- **Amazon Kinesis Data Streams** — real-time data ingestion
- **AWS Lambda** — serverless compute for processing and trend analysis
- **Amazon S3** — durable object storage for raw data and query results
- **AWS Glue Data Catalog** — schema discovery and metadata management
- **Amazon Athena** — serverless interactive SQL analytics
- **Amazon DynamoDB** — single-digit-millisecond NoSQL lookups
- **Amazon SNS** — pub/sub notifications (Email, SMS)

## Getting Started

### 1. Provision Infrastructure

Create the following resources in your AWS account (manually, via CloudFormation, or Terraform):

- A Kinesis Data Stream (e.g. `stock-market-stream`)
- Two S3 buckets — one for raw data, one for Athena query results
- A DynamoDB table (e.g. `stock-data`) with DynamoDB Streams enabled
- A Glue Crawler pointing at the raw-data S3 bucket
- An SNS topic with Email/SMS subscriptions
- Two Lambda functions (Processor and Trend Analyzer) with the appropriate triggers

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

Key libraries: `boto3`, `yfinance`

### 3. Run the Producer Script

```bash
python producer.py
```

The script fetches live stock prices and publishes them to Kinesis Data Streams in a continuous loop.

### 4. Query with Athena

After the Glue Crawler has cataloged the S3 data, open the Athena console and run SQL queries such as:

```sql
SELECT symbol, close, volume, trade_date
FROM stock_data
WHERE symbol = 'AAPL'
ORDER BY trade_date DESC
LIMIT 100;
```

### 5. Receive Alerts

Subscribe to the SNS topic via Email or SMS. When the Trend Analyzer Lambda detects a crossover between SMA-5 and SMA-20, you will receive a buy/sell signal notification.

## Trend Analysis Logic

The Trend Analyzer Lambda computes two Simple Moving Averages for each stock:

| Indicator | Window | Purpose |
|-----------|--------|---------|
| **SMA-5** | 5 periods | Short-term trend |
| **SMA-20** | 20 periods | Long-term trend |

- **Buy signal** — SMA-5 crosses above SMA-20 (bullish crossover)
- **Sell signal** — SMA-5 crosses below SMA-20 (bearish crossover)

## Project Structure

```
├── producer.py              # Python script that fetches and streams stock data
├── lambda/
│   ├── processor.py         # Kinesis-triggered Lambda for data transformation
│   └── trend_analyzer.py    # DynamoDB-triggered Lambda for moving-average alerts
├── infrastructure/          # IaC templates (CloudFormation / Terraform)
├── requirements.txt         # Python dependencies
└── README.md
```
