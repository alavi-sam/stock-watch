# Stock Watch

An automated pipeline for fetching, processing, and preparing stock market news articles for AI-powered analysis. Built with Apache Airflow, PostgreSQL, and AWS S3.

## What It Does

1. Fetches news articles for monitored tickers (AAPL, MSFT, META, NFLX) from Finnhub
2. Stores raw metadata in PostgreSQL and raw JSON in S3
3. Scrapes and extracts full article content, cleaned via OpenRouter AI
4. Stores processed article text in S3, partitioned by ticker and date
5. Generates vector embeddings (NVIDIA Llama Nemotron, 2048-dim) for semantic search
6. Answers natural-language questions about stocks via RAG using retrieved articles as context

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
[create_and_load_embedding] — runs hourly at :30
    → generates 2048-dim vector embeddings (NVIDIA Llama Nemotron via OpenRouter)
    → stores in PostgreSQL article_embedding table (pgvector)
    ↓
[RAG query] — on demand
    → performs cosine similarity search against embeddings
    → fetches full article text from S3
    → answers user question via OpenRouter (Cohere Command-R)
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
- **Embeddings**: OpenRouter (NVIDIA Llama Nemotron Embed VL 1B, 2048-dim)
- **RAG / LLM**: OpenRouter (Cohere Command-R for article cleaning and Q&A)
- **Vector Search**: pgvector (cosine similarity)
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
| `create_and_load_embedding` | Hourly at :30 | Scheduled |

## Querying

Use `services/RAG.py` to ask natural-language questions about stock news:

```python
from services.RAG import retrieve
import asyncio

results = asyncio.run(retrieve("How is Apple performing this quarter?"))
```

`retrieve()` performs a cosine similarity search against stored embeddings, fetches the full article text from S3, and returns the top 10 matching articles with metadata and content.

The `test.py` script demonstrates a full end-to-end RAG query: it retrieves relevant articles and passes them as context to an LLM (Cohere Command-R via OpenRouter) to generate an answer.
