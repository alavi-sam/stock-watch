from openai import AsyncOpenAI
import os
from dotenv import load_dotenv
from tasks.connection import async_session_local
from sqlalchemy import text
load_dotenv()

client = AsyncOpenAI(api_key=os.getenv("OPENROUTER_API_KEY"), base_url="https://openrouter.ai/api/v1")


async def create_embedding(input: str | bytes) -> list[float]:
    if isinstance(input, bytes):
        input = input.decode("utf-8")
    res = await client.embeddings.create(
        model="nvidia/llama-nemotron-embed-vl-1b-v2:free",
        input=input,
        encoding_format="float"
    )
    return res.data[0].embedding


async def insert_embedding(article_id: int, embedding: list[float]):
    vector_str = "[" + ",".join(str(x) for x in embedding) + "]"
    async with async_session_local() as session:
        await session.execute(
            text(
                "INSERT INTO article_embedding (id, embedding) "
                "VALUES (:id, CAST(:embedding AS vector)) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"id": article_id, "embedding": vector_str}
        )
        await session.commit()
