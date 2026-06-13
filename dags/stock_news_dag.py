from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.datasets import Dataset

from tasks.fetch import fetch_ticker_news
from tasks.storage import insert_raw_json

TICKERS = ['AAPL', 'MSFT', 'META', 'NFLX']
raw_data = Dataset('s3://stock-watcher-raw-data/raw')

@dag(
    dag_id="stock_news_ingestion",
    schedule="1 9,12,15,18 * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["stock-watch"],
)
def stock_news_ingestion():

    @task
    def fetch_and_store(ticker: str) -> int:
        ds = get_current_context()["ds"]
        run_date = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        async def _run() -> int:
            stored = 0
            async with httpx.AsyncClient() as client:
                articles = await fetch_ticker_news(ticker, ds, ds, client)
                for article in articles:
                    await insert_raw_json(article, run_date=run_date)
                    stored += 1
            return stored

        return asyncio.run(_run())

    @task(outlets=[raw_data])
    def done_flag():
        return True

    fetch_and_store.expand(ticker=TICKERS) >> done_flag()

stock_news_ingestion()
