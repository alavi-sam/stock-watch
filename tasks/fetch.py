import os
import asyncio
import logging

import httpx
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

import trafilatura
from openai import AsyncOpenAI


load_dotenv()
logger = logging.getLogger(__name__)

API_KEY = os.getenv('FINNHUB_API_KEY')
BASE_URL = "https://finnhub.io/api/v1"


async def fetch_ticker_news(ticker: str, date_from: str, date_to: str, client: httpx.AsyncClient) -> list[dict]:
    url = BASE_URL + '/company-news'
    try:
        response = await client.get(
            url=url,
            params={
                'symbol': ticker,
                'from': date_from,
                'to': date_to,
                'token': API_KEY
            }
        )
        logger.info(f'Fetched {ticker} successfully!')
        return response.json()
    except Exception as e:
        logger.error(f"failed fetching {ticker}. Error: {str(e)}")
        return []



_IMPERSONATE_ORDER = ["chrome131", "chrome124", "safari17_0", "edge101"]


async def _resolve_redirect(url: str) -> str:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
    return str(r.url)


async def parse_article_content(url):
    article_url = await _resolve_redirect(url)
    logger.info(f"Resolved {url} -> {article_url}")

    response = None
    last_status = None
    for browser in _IMPERSONATE_ORDER:
        async with AsyncSession(impersonate=browser) as client:
            r = await client.get(article_url, allow_redirects=True)
        if r.status_code < 400:
            response = r
            break
        last_status = r.status_code
        logger.warning(f"Got {r.status_code} with {browser} for {article_url}, trying next impersonation")
        await asyncio.sleep(1)

    if response is None:
        raise Exception(f"All impersonations failed for {article_url}, last status: {last_status}")

    cleaned_html_text = trafilatura.extract(response.content, favor_recall=True)

    client = AsyncOpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=os.getenv("OPENROUTER_API_KEY"),
    )

    response = await client.chat.completions.create(
        model='cohere/command-r-08-2024',
        temperature=0.0,
        messages=[
            {
                'role': 'system',
                'content': (
                    "You are a strict data extraction tool. Your ONLY task is to extract "
                    "the core stock market news article text found inside the <html_content> tags.\n\n"
                    "CRITICAL RULES:\n"
                    "1. Output the raw extracted news text and body paragraphs only.\n"
                    "2. Exclude all advertisement blocks, sign-up forms, copyright lines, and link lists.\n"
                    "3. ABSOLUTE SILENCE except for the news text. No explanations, no introduction, "
                    "no conversation, no analysis of the code or text."
                )
            },
            {
                'role': 'user',
                'content': f"Extract the news article text from this content:\n<html_content>\n{cleaned_html_text}\n</html_content>"
            }
        ]
    )

    return response.choices[0].message.content
