import os
import json
from dotenv import load_dotenv
from services.s3_wrapper import BucketWrapper
from datetime import datetime
import boto3
import logging
load_dotenv()
logger = logging.getLogger(__name__)


s3_client = BucketWrapper('stock-watcher-raw-data')
            
async def insert_raw_json(content: dict):
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    hour = datetime.now().hour

    prefix = f"raw/ticker={content['related']}/year={year}/month={month}/day={day}/hour={hour}"
    
    await s3_client.put_object(
        key=f"{prefix}/{content['id']}.json",
        body=json.dumps(content)
    )





