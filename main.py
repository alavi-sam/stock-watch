import logging
from tasks.fetch import fetch_ticker_news
from tasks.storage import insert_raw_json
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
            for item in result:
                await insert_raw_json(item)
            res.extend(result)


if __name__ == '__main__':
    asyncio.run(main())
    print(res[0])