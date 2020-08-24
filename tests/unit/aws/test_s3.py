import pytest
from moto.s3 import mock_s3
import boto3

from red_panda.aws.s3 import S3Utils

import logging


LOGGER = logging.getLogger(__name__)

MOCK_AWS_CONFIG = {
    "aws_access_key_id": "your-aws-access-key-id",
    "aws_secret_access_key": "your-aws-secret-access-key",
}


@pytest.fixture
def s3_utils():
    LOGGER.info(f">>>>> Setup mock S3")
    with mock_s3():
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket="EXISTINGBUCKET")
        yield S3Utils(MOCK_AWS_CONFIG)
    LOGGER.info(f">>>>> Teardown mock S3")


def test_check_s3_bucket_existence_on_non_existent_bucket(s3_utils):
    assert not s3_utils._check_s3_bucket_existence("DOESNOTEXIST")


def test_check_s3_bucket_existence_on_existing_bucket(s3_utils):
    assert s3_utils._check_s3_bucket_existence("EXISTINGBUCKET")


def test_list_buckets(s3_utils):
    assert "EXISTINGBUCKET" in s3_utils.list_buckets()
