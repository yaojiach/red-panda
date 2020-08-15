import pytest
from red_panda.aws.s3 import S3Utils

import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def s3_utils(aws_config):
    return S3Utils(aws_config)

def test_check_s3_bucket_existence_on_non_existent_bucket(s3_utils):
    assert not s3_utils._check_s3_bucket_existence("SHOULDNOTEXIST")
