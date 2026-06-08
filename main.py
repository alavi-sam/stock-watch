import logging
from fetch_news import fetch_ticker_news
import asyncio
import httpx


logging.basicConfig(
    level=logging.INFO,                                 # Capture INFO and more severe logs
    format="%(asctime)s [%(levelname)s] %(message)s",   # Define line format
    datefmt="%Y-%m-%d %H:%M:%S",                        # Customize timestamp format
    filename="app.log",                                 # File destination (removes console print)
    filemode="a"                                        # 'a' to append, 'w' to overwrite
)



tickers = [
    'AAPL',
    'MSFT',
    'META',
    'NFLX'
]

res = []


async def main():
    async with httpx.AsyncClient() as client:
        for ticker in tickers:
            result = await fetch_ticker_news(ticker, '2026-06-08', '2026-06-08', client)
            res.append(result)


if __name__ == '__main__':
    asyncio.run(main())