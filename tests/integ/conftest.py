import pytest
import os
import logging
import boto3
from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger(__name__)

STACK_NAME = os.getenv("STACK_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


@pytest.fixture(scope="module")
def aws_config():
    return {
        aws_access_key_id: AWS_ACCESS_KEY_ID,
        aws_secret_access_key: AWS_SECRET_ACCESS_KEY,
    }


@pytest.fixture(scope="module")
def s3_bucket():
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    return [
        b["Name"]
        for b in s3_client.list_buckets()["Buckets"]
        if b["Name"].startswith(STACK_NAME)
    ][0]


@pytest.fixture(scope="module")
def redshift_config():
    redshift_client = boto3.client(
        "redshift",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    return {
        "user": os.environ("REDSHIFT_USERNAME"),
        "password": os.environ("REDSHIFT_PASSWORD"),
        "host": [
            c["Address"]
            for c in redshift_client.describe_clusters()["Clusters"]
            if c["ClusterIdentifier"].startswith(STACK_NAME)
        ][0],
        "port": os.environ("REDSHIFT_PORT"),
        "dbname": os.environ("REDSHIFT_DB"),
    }
