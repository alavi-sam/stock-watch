import json
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv

from services.s3_wrapper import BucketWrapper

load_dotenv()
logger = logging.getLogger(__name__)

s3_client = BucketWrapper('stock-watcher-raw-data')


async def insert_raw_json(content: dict, run_date: datetime | None = None):
    run_date = run_date or datetime.now(timezone.utc)

    prefix = (
        f"raw/ticker={content['related']}"
        f"/year={run_date.year}/month={run_date.month:02d}/day={run_date.day:02d}/hour={run_date.hour:02d}"
    )


    if not await s3_client.object_exists(f"{prefix}/{content['id']}.json"):
        await s3_client.put_object(
            key=f"{prefix}/{content['id']}.json",
            body=json.dumps(content).encode('utf-8')
        )
        logger.info(f"Uploaded successfully! {prefix}/{content['id']}.json")
        return True

    logger.info("Record exists. Skipping!")
    return False
