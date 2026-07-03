import asyncio

from airflow.decorators import dag, task
from db.connection import async_session_local
from tasks.fetch import parse_article_content
from sqlalchemy import text
import pendulum
from datetime import datetime
from services.s3_wrapper import BucketWrapper

from logging import getLogger
logger = getLogger(__name__)


@dag(
        dag_id='parse_full_articles',
        schedule="1 * * * *",
        start_date=pendulum.datetime(2026, 6, 21),
        catchup=False,
        max_active_runs=1,
)
def parse_articles():
    s3_client = BucketWrapper('stock-watcher-article-content')

    @task
    def create_bucket():
        asyncio.run(s3_client.create_bucket())

    @task
    def query_non_fetched_news():
        async def _query():
            async with async_session_local() as client:
                result = await client.execute(
                    text(
                        "SELECT id, url, datetime, ticker FROM articles WHERE fetched = FALSE ORDER BY COALESCE(num_try, 0) ASC LIMIT 100"
                    )
                )
                return result.mappings().fetchall()
        rows = asyncio.run(_query())
        return [
                  {"url": r["url"], "id": r["id"], "ticker": r["ticker"], "news_dt": r["datetime"]}
                  for r in rows
              ]

    @task
    def parse_article(row: dict):
        async def _parse():
            await asyncio.sleep(2)
            try:
                content = await parse_article_content(row['url'])
                logger.info(f"article parse at {row['url']}!")
                return {**row, 'content': content}
            except Exception as e:
                logger.error(f"failed to parse {row['url']}! Error: {e}")
                return {**row, 'content': None}

        return asyncio.run(_parse())

    @task
    def insert_s3(row: dict):
        if row['content'] is None:
            logger.info(f"skipping s3 insert for row {row['id']}, no content parsed")
            return {**row, 'uploaded': False}

        ticker, news_dt, content, id = row['ticker'], row['news_dt'], row['content'], row['id']
        prefix = f'article_content/ticker={ticker}/year={news_dt.year}/month={news_dt.month}/day={news_dt.day}/hour={news_dt.hour}/'

        async def _insert_s3():
            if await s3_client.object_exists(prefix + f'{id}.txt'):
                logger.info(f"object {id} already exists. Skipping!")
                return True
            try:
                await s3_client.put_object(key=prefix + f'{id}.txt', body=content)
                logger.info(f"object {id} inserted in bucket successfully!")
                return True
            except Exception as e:
                logger.error(f"could not insert object {id} in bucket! Error: {e}")
                return False
        uploaded = asyncio.run(_insert_s3())
        return {**row, 'uploaded': uploaded}

    @task
    def mark_as_parsed(row: dict):

        async def _query(row):
            async with async_session_local() as client:
                if row['uploaded']:
                    res = await client.execute(
                        text(
                            "UPDATE articles " \
                            "SET fetched = TRUE, num_try = COALESCE(num_try, 0) + 1 "
                            "WHERE id = :id"
                        ),
                        {'id': row['id']}
                    )
                else:
                    res = await client.execute(
                        text(
                            "UPDATE articles " \
                            "SET num_try = COALESCE(num_try, 0) + 1 " \
                            "WHERE id = :id"
                        ),
                        {'id': row['id']}
                    )
                await client.commit()
                if res.rowcount:
                    logger.info(f"marked row {row['id']} as fetched!")
                else:
                    logger.error(f"failed to mark row {row['id']} as fetched!")
                return res.rowcount
        return asyncio.run(_query(row))

    bucket = create_bucket()
    rows = query_non_fetched_news()
    parsed_rows = parse_article.expand(row=rows)
    inserted_rows = insert_s3.expand(row=parsed_rows)
    mark_as_parsed.expand(row=inserted_rows)

    bucket >> rows


parse_articles()
    






        




            

