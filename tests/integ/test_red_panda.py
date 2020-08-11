import pytest
import os
import logging
import boto3
from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
STACK_NAME = os.getenv("STACK_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)
redshift_client = boto3.client(
    "redshift",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

S3_BUCKET = [
    b["Name"] for b in s3_client.list_buckets()["Buckets"] if b["Name"].startswith(STACK_NAME)
][0]
REDSHIFT_HOST = [
    c
    for c in redshift_client.describe_clusters()["Clusters"]
    if c["ClusterIdentifier"].startswith(STACK_NAME)
]
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")


def test_integ_test():
    LOGGER.info("This is integ test.")
    LOGGER.info(S3_BUCKET)
    assert True
