import pytest
from moto import mock_s3
from red_panda.red_panda import S3Utils

import logging

LOGGER = logging.getLogger(__name__)

MOCK_AWS_CONFIG = {
    "aws_access_key_id": "your-aws-access-key-id",
    "aws_secret_access_key": "your-aws-secret-access-key",
}


@pytest.fixture
@mock_s3
def s3_utils():
    return S3Utils(MOCK_AWS_CONFIG)


def test_check_s3_bucket_existence_on_non_existent_bucket(s3_utils):
    assert not s3_utils._check_s3_bucket_existence("DOESNOTEXIST")
