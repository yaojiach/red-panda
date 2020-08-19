import pytest
from red_panda.aws.s3 import S3Utils

import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def s3_utils(aws_config):
    return S3Utils(aws_config)


def test_check_s3_bucket_existence_on_existing_bucket(s3_utils, s3_bucket):
    assert s3_utils._check_s3_bucket_existence(s3_bucket)


def test_check_s3_bucket_existence_on_non_existent_bucket(s3_utils):
    assert not s3_utils._check_s3_bucket_existence("BUCKET_SHOULD_NOT_EXIST")


def test_check_s3_key_existence_on_non_existent_key(s3_utils, s3_bucket):
    assert not s3_utils._check_s3_key_existence(s3_bucket, "KEY_SHOULD_NOT_EXIST")


def test_list_buckets(s3_utils, s3_bucket):
    assert s3_bucket in s3_utils.list_buckets()
