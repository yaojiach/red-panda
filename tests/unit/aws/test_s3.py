import pytest
from moto.s3 import mock_s3
import boto3
import pandas as pd

from red_panda.aws.s3 import S3Utils

import logging


LOGGER = logging.getLogger(__name__)

MOCK_AWS_CONFIG = {
    "aws_access_key_id": "your-aws-access-key-id",
    "aws_secret_access_key": "your-aws-secret-access-key",
}

S3_BUCKET_NAME = "EXISTINGBUCKET"
S3_KEY = "sample.csv"
SAMPLE_BODY = "col0\n1"
SAMPLE_BODY_BYTES = b"col0\n1"
SAMPLE_DF = pd.DataFrame([{"col0": 1}])


def get_bucket_names():
    return [b["Name"] for b in boto3.client("s3").list_buckets()["Buckets"]]


@pytest.fixture(scope="module")
def s3_utils():
    LOGGER.info(f">>>>> Setup mock S3")
    with mock_s3():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=S3_BUCKET_NAME)
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY, Body=b"col0\n1")
        yield S3Utils(MOCK_AWS_CONFIG)
    LOGGER.info(f">>>>> Teardown mock S3")


def test_check_s3_bucket_existence_on_non_existent_bucket(s3_utils):
    assert not s3_utils._check_s3_bucket_existence("DOESNOTEXIST")


def test_check_s3_bucket_existence_on_existing_bucket(s3_utils):
    assert s3_utils._check_s3_bucket_existence(S3_BUCKET_NAME)


def test_create_bucket(s3_utils):
    s3_bucket_name = "test-create"
    s3_utils.create_bucket(bucket=s3_bucket_name)
    assert s3_bucket_name in get_bucket_names()


def test_create_bucket_raises(s3_utils):
    with pytest.raises(ValueError):
        s3_utils.create_bucket(bucket=S3_BUCKET_NAME, error="raise")


def test_create_bucket_warns(s3_utils):
    with pytest.warns(UserWarning):
        s3_utils.create_bucket(bucket=S3_BUCKET_NAME, error="warn")


def test_list_buckets(s3_utils):
    assert S3_BUCKET_NAME in s3_utils.list_buckets()


def test_s3_to_obj(s3_utils):
    content = s3_utils.s3_to_obj(bucket=S3_BUCKET_NAME, key=S3_KEY).getvalue()
    assert content == SAMPLE_BODY_BYTES


def test_s3_to_file(s3_utils, tmpdir):
    sample_file = tmpdir / "sample.csv"
    s3_utils.s3_to_file(bucket=S3_BUCKET_NAME, key=S3_KEY, file_name=str(sample_file))
    assert sample_file.read() == SAMPLE_BODY


def test_s3_to_df(s3_utils):
    df = s3_utils.s3_to_df(bucket=S3_BUCKET_NAME, key=S3_KEY)
    assert df.equals(SAMPLE_DF)

