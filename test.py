from services.RAG import retrieve
import asyncio
from openai import OpenAI
import os
from dotenv import load_dotenv
load_dotenv()




client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=os.getenv("OPENROUTER_API_KEY"),
    )

def build_context(results: list) -> str:
    parts = []
    for r in results:
        parts.append(
            f"[{r['ticker']} | {r['article_datetime']} | confidence: {r['confidence']:.2f}]\n"
            f"Headline: {r['title']}\n"
            f"URL: {r['url']}\n"
            f"Content:\n{r['article']}\n"
        )
    return "\n---\n".join(parts)


def ask(question: str) -> str:
    results = asyncio.run(retrieve(question))
    context = build_context(results)

    response = client.chat.completions.create(
        model='cohere/command-r-08-2024',
        temperature=0.0,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a financial news assistant. "
                    "Answer the user's question using only the provided articles. "
                    "If the articles don't contain enough information, say so."
                ),
            },
            {
                "role": "user",
                "name": "rag",
                "content": f"Here are relevant articles:\n\n{context}",
            },
            {
                "role": "user",
                "name": "user",
                "content": question,
            },
        ],
    )
    return response.choices[0].message.content


question = "How do you think Microsoft will perform? bullish or bearish? analyze why?"    
print(ask(question))



