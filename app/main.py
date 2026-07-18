from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, StreamingResponse
from services.RAG import retrieve
import os
import uuid
import json
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

BASE_PATH = os.getenv("BASE_PATH", "")

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

with open("app/static/index.html") as f:
    _HTML_TEMPLATE = f.read()


def render_html():
    return HTMLResponse(_HTML_TEMPLATE.replace("__BASE_PATH__", BASE_PATH))


@app.get("/")
@app.get("/chat")
async def index():
    return render_html()


client = AsyncOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
)

SYSTEM_PROMPT = (
    "You are a financial news assistant. "
    "Answer the user's question using only the provided articles. "
    "Only cite articles that are directly relevant to the question — ignore unrelated ones. "
    "If the articles don't contain enough information, say so briefly and stop. "
    "Do not add generic financial advice or disclaimers."
)

# session_id -> list of {"role": ..., "content": ...}
sessions: dict[str, list[dict]] = {}
# session_id -> list of raw questions (used for retrieval enrichment)
session_queries: dict[str, list[str]] = {}


def build_context(results: list) -> str:
    parts = []
    for r in results:
        article_text = r.get('article', b'')
        if isinstance(article_text, bytes):
            article_text = article_text.decode('utf-8', errors='ignore')
        parts.append(
            f"[{r['ticker']} | {r['article_datetime']} | confidence: {r['confidence']:.2f}]\n"
            f"Headline: {r['title']}\n"
            f"URL: {r['url']}\n"
            f"Content:\n{article_text}\n"
        )
    return "\n---\n".join(parts)


async def ask_stream(session_id: str, question: str):
    history = sessions.setdefault(session_id, [])
    queries = session_queries.setdefault(session_id, [])

    if queries:
        retrieval_query = " ".join(queries[-4:]) + " " + question
    else:
        retrieval_query = question

    results = await retrieve(retrieval_query)
    context = build_context(results)

    history.append({
        "role": "user",
        "content": f"Relevant articles:\n\n{context}\n\n---\n\nQuestion: {question}",
    })

    stream = await client.chat.completions.create(
        model='cohere/command-r-08-2024',
        temperature=0.0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            *history,
        ],
        stream=True,
    )

    full_answer = ""
    async for chunk in stream:
        delta = chunk.choices[0].delta.content or ""
        if delta:
            full_answer += delta
            yield f"data: {json.dumps({'token': delta})}\n\n"

    history.append({"role": "assistant", "content": full_answer})
    queries.append(question)
    yield f"data: {json.dumps({'session_id': session_id, 'done': True})}\n\n"


@app.post('/chat')
async def chat(request: Request):
    body = await request.json()
    session_id = body.get("session_id") or str(uuid.uuid4())
    question = body.get("question") or body.get("conversation")
    if not question:
        raise HTTPException(status_code=400, detail="Missing 'question' field")
    return StreamingResponse(
        ask_stream(session_id, question),
        media_type="text/event-stream",
    )


@app.delete('/chat/{session_id}')
async def clear_session(session_id: str):
    sessions.pop(session_id, None)
    session_queries.pop(session_id, None)
    return {"cleared": session_id}
