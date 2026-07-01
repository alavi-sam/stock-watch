import logging

import aioboto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class BucketWrapper:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.session = aioboto3.Session()

    async def bucket_exists(self, bucket_name: str | None = None) -> bool:
        bucket_name = bucket_name or self.bucket_name
        try:
            async with self.session.client('s3') as client:
                await client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchBucket'):
                return False
            logger.error(f"Could not check existence of bucket {bucket_name}! Error: {str(e)}")
            raise

    async def create_bucket(self, bucket_name: str | None = None):
        bucket_name = bucket_name or self.bucket_name
        if await self.bucket_exists(bucket_name):
            logger.info(f"Bucket already exists, skipping creation: {bucket_name}")
            return
        try:
            async with self.session.client('s3') as client:
                await client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket created: {bucket_name}")
        except ClientError as e:
            logger.error(f"Could not create bucket {bucket_name}! Error: {str(e)}")
            raise

    async def list_objects(self, prefix: str | None = None) -> list[str]:
        try:
            async with self.session.client('s3') as client:
                params = {'Bucket': self.bucket_name}
                if prefix:
                    params['Prefix'] = prefix
                response = await client.list_objects_v2(**params)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            logger.error(f"Could not list objects in {self.bucket_name}! Error: {str(e)}")
            raise

    async def directory_exists(self, prefix: str) -> bool:
        if not prefix.endswith('/'):
            prefix += '/'
        try:
            async with self.session.client('s3') as client:
                response = await client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=prefix, MaxKeys=1
                )
            return response.get('KeyCount', 0) > 0
        except ClientError as e:
            logger.error(f"Could not check prefix {prefix}! Error: {str(e)}")
            raise

    async def object_exists(self, key: str) -> bool:
        try:
            async with self.session.client('s3') as client:
                await client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
                return False
            logger.error(f"Could not check existence of {key}! Error: {str(e)}")
            raise

    async def upload_file(self, file_path: str, key: str):
        try:
            async with self.session.client('s3') as client:
                await client.upload_file(file_path, self.bucket_name, key)
            logger.info(f"File uploaded successfully! key: {key}")
        except ClientError as e:
            logger.error(f"Could not upload file! key: {key}, error: {str(e)}")
            raise

    async def put_object(self, key: str, body: bytes | str):
        try:
            async with self.session.client('s3') as client:
                await client.put_object(Bucket=self.bucket_name, Key=key, Body=body)
            logger.info(f"Object put successfully! key: {key}")
        except ClientError as e:
            logger.error(f"Could not put object! key: {key}, error: {str(e)}")
            raise

    async def download_file(self, key: str, file_path: str):
        try:
            async with self.session.client('s3') as client:
                await client.download_file(self.bucket_name, key, file_path)
            logger.info(f"File downloaded successfully! key: {key}")
        except ClientError as e:
            logger.error(f"Could not download file! key: {key}, error: {str(e)}")
            raise

    async def get_object(self, key: str) -> bytes:
        try:
            async with self.session.client('s3') as client:
                response = await client.get_object(Bucket=self.bucket_name, Key=key)
                async with response['Body'] as stream:
                    return await stream.read()
        except ClientError as e:
            logger.error(f"Could not get object! key: {key}, error: {str(e)}")
            raise

    async def delete_file(self, key: str):
        try:
            async with self.session.client('s3') as client:
                await client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"File deleted successfully! key: {key}")
        except ClientError as e:
            logger.error(f"Could not delete file! key: {key}, error: {str(e)}")
            raise
