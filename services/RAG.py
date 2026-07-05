from openai import AsyncOpenAI
from tasks.embedding import create_embedding
from db.connection import async_session_local
from sqlalchemy import text
from services.s3_wrapper import BucketWrapper
import os
import asyncio
from dotenv import load_dotenv
load_dotenv()



async def find_similar_db(input: str):
    embedding = await create_embedding(input)
    vector_str = "[" + ",".join(str(x) for x in embedding) + "]"
    async with async_session_local() as session:
        res = await session.execute(
            text(
                "SELECT " \
                "a.id as article_id, " \
                "a.ticker as ticker, " \
                "a.datetime as article_datetime, " \
                "a.headline as title, " \
                "a.url as url, " \
                "1-(e.embedding <=> CAST(:embedding AS VECTOR)) as confidence " \
                "FROM articles as a " \
                "INNER JOIN article_embedding as e " \
                "ON a.id = e.id " \
                "ORDER BY e.embedding <=> CAST(:embedding AS VECTOR) ASC " \
                "LIMIT 10;"
            ),
            {'embedding': vector_str}
        )
        return res.mappings().fetchall()
    

async def retrieve(input: str):
    s3_client = BucketWrapper('stock-watcher-article-content')
    rows = await find_similar_db(input)
    results = []
    for row in rows:
        s3_key = f"article_content/ticker={row['ticker']}/year={row['article_datetime'].year}" \
        f"/month={row['article_datetime'].month}/day={row['article_datetime'].day}" \
        f"/hour={row['article_datetime'].hour}/{row['article_id']}.txt"
        result = dict(row)
        result['article'] = await s3_client.get_object(key=s3_key)
        results.append(result)
    return results





