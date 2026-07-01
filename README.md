# Stock Watch

An automated pipeline for fetching, processing, and preparing stock market news articles for AI-powered analysis. Built with Apache Airflow, PostgreSQL, and AWS S3.

## What It Does

1. Fetches news articles for monitored tickers (AAPL, MSFT, META, NFLX) from Finnhub
2. Stores raw metadata in PostgreSQL and raw JSON in S3
3. Scrapes and extracts full article content, cleaned via OpenRouter AI
4. Stores processed article text in S3, partitioned by ticker and date
5. (In progress) Generates vector embeddings for semantic search and AI analysis

## Architecture

```
Finnhub API
    ↓
[stock_news_ingestion] — runs 4x daily
    → stores raw JSON in S3 (stock-watcher-raw-data)
    → triggers Dataset event
    ↓
[insert_raw_json_to_sql] — triggered by Dataset event
    → reads S3, inserts article metadata into PostgreSQL
    ↓
[parse_full_articles] — runs hourly
    → fetches full article HTML, extracts text via trafilatura + AI
    → stores cleaned text in S3 (stock-watcher-article-content)
    → marks articles as fetched in PostgreSQL
    ↓
[create_embedding] — (in progress)
    → generates vector embeddings for semantic search
```

### S3 Path Structure

```
stock-watcher-raw-data/
  raw/ticker={ticker}/year={Y}/month={M}/day={D}/hour={H}/{id}.json

stock-watcher-article-content/
  article_content/ticker={ticker}/year={Y}/month={M}/day={D}/hour={H}/{id}.txt
```

## Tech Stack

- **Orchestration**: Apache Airflow 2.10.5 (LocalExecutor)
- **Database**: PostgreSQL 16
- **Storage**: AWS S3 (aioboto3)
- **HTTP**: httpx (async)
- **Content Extraction**: trafilatura
- **AI**: OpenRouter (Cohere Command-R for article cleaning)
- **News API**: Finnhub

## Setup

### Prerequisites

- Docker & Docker Compose
- Finnhub API key
- OpenRouter API key
- AWS credentials with S3 access

### Environment Variables

Create a `.env` file in the project root:

```env
FINNHUB_API_KEY=
OPENROUTER_API_KEY=

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-east-1

DB_USER=postgres
DB_PASSWORD=
DB_HOST=news-db
DB_NAME=stock_news

AIRFLOW_UID=1000
```

### Running

```bash
docker-compose up -d
```

Airflow UI is available at `http://localhost:8080` (default credentials: `admin` / `admin`).

```bash
# Shut down
docker-compose down

# Shut down and remove volumes (deletes all data)
docker-compose down -v
```

## DAGs

| DAG | Schedule | Trigger |
|-----|----------|---------|
| `stock_news_ingestion` | 1:00, 9:00, 12:00, 15:00, 18:00 UTC | Scheduled |
| `insert_raw_json_to_sql` | — | Dataset event from `stock_news_ingestion` |
| `parse_full_articles` | Hourly at :01 | Scheduled |
| `create_embedding` | TBD | TBD |
