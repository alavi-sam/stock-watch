import asyncio
import json
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.datasets import Dataset
from services.s3_wrapper import BucketWrapper
from sqlalchemy import text
from dags.stock_news_dag import TICKERS
from datetime import datetime, timezone
from tasks.connection import async_session_local


raw_data = Dataset('s3://stock-watcher-raw-data/raw')

@dag(
    dag_id='insert_raw_json_to_sql',
    schedule=raw_data,
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
)
def transfer_to_sql():

    s3_client = BucketWrapper('stock-watcher-raw-data')

    @task
    def read_data(ticker: str) -> list[str]:
        context = get_current_context()
        event_extra = context['triggering_dataset_events'][raw_data.uri][-1].extra
        if not event_extra.get('has_new_data'):
            return []
        run_time = datetime.fromisoformat(event_extra['logical_date'])

        prefix = f"raw/ticker={ticker}/year={run_time.year}/month={run_time.month:02d}/day={run_time.day:02d}/hour={run_time.hour:02d}"
        return asyncio.run(s3_client.list_objects(prefix=prefix))

        
    @task
    def flatten(keys: list[list]):
        return [key for sublist in keys for key in sublist]


    @task
    def insert_sql(key: str):
        async def _run_insert():
            obj = await s3_client.get_object(key)
            obj = json.loads(obj)
            async with async_session_local() as session:
                res = await session.execute(
                    text("select id from articles where id = :id"),
                    {
                        "id": obj["id"]
                    }
                )

                if res.first():
                    return True

                await session.execute(
                    text(
                        "INSERT INTO articles (id, ticker, category, datetime, headline, image, source, summary, url)" \
                        "VALUES (:id, :related, :category, :datetime, :headline, :image, :source, :summary, :url)"
                    ),
                    {
                        "id": obj["id"],
                        "related": obj["related"],
                        "category": obj["category"],
                        "datetime": datetime.fromtimestamp(obj["datetime"], tz=timezone.utc),
                        "headline": obj["headline"],
                        "image": obj["image"],
                        "source": obj["source"],
                        "summary": obj["summary"],
                        "url": obj["url"]
                    }
                )

                await session.commit()
                
        asyncio.run(_run_insert())

    data = read_data.expand(ticker=TICKERS)

    flatten_data = flatten(data)

    insert_sql.expand(key=flatten_data)

transfer_to_sql()
