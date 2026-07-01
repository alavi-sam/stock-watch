from airflow.decorators import dag, task
import pendulum
from tasks.embedding import create_embedding, insert_embedding
from tasks.connection import async_session_local
from services.s3_wrapper import BucketWrapper
from sqlalchemy import text
import asyncio


@dag(
    dag_id='create_and_load_embedding',
    start_date=pendulum.datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    schedule="30 * * * *"
)
def embed_news():
    s3_client = BucketWrapper('stock-watcher-article-content')

    @task
    def query_non_embedded():
        async def _query():
            async with async_session_local() as session:
                res = await session.execute(
                    text(
                        "SELECT "
                        "a.id, "
                        "'article_content/ticker=' || a.ticker "
                        "|| '/year=' || EXTRACT(YEAR FROM a.datetime) "
                        "|| '/month=' || EXTRACT(MONTH FROM a.datetime) "
                        "|| '/day=' || EXTRACT(DAY FROM a.datetime) "
                        "|| '/hour=' || EXTRACT(HOUR FROM a.datetime) "
                        "|| '/' || a.id || '.txt' AS s3_key "
                        "FROM articles a "
                        "LEFT JOIN article_embedding e ON a.id = e.id "
                        "WHERE e.id IS NULL AND a.fetched = TRUE"
                    )
                )
                return res.mappings().fetchall()
        rows = asyncio.run(_query())
        return [{"id": r["id"], "s3_key": r["s3_key"]} for r in rows]

    @task
    def generate_and_insert_embedding(row: dict):
        async def _run():
            article = await s3_client.get_object(row["s3_key"])
            embedding = await create_embedding(article)
            await insert_embedding(row["id"], embedding)
        asyncio.run(_run())

    rows = query_non_embedded()
    generate_and_insert_embedding.expand(row=rows)


embed_news()
