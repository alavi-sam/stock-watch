import os
import logging

import httpx
from dotenv import load_dotenv

import trafilatura


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



async def parse_article_content(url):
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(
            url=url,
            headers={
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
                )
            }
        )

    response.raise_for_status()

    return trafilatura.extract(response.content, favor_recall=True)
