import pytest
import pandas as pd
from rsa import parallel

from red_panda import RedPanda

import logging

LOGGER = logging.getLogger(__name__)

TABLE_NAME = "test_table"
DOWNLOAD_SQL = f"select * from {TABLE_NAME}"
DOWNLOAD_DF = pd.DataFrame(data={"index": [0, 1], "col1": [1, 2], "col2": ["x", "y"]})
UPLOAD_DF = DOWNLOAD_DF[["col1", "col2"]]
CONTENT_BYTE_STR = b"0|1|x\n1|2|y\n"
CONTENT_BYTE_PARALLEL_WITH_HEADER = b"index|col1|col2\n0|1|x\n1|2|y\n"
CONTENT_BYTE_STR_CSV = b"0,1,x\n1,2,y\n"


@pytest.fixture
def red_panda(redshift_config, aws_config, s3_bucket):
    # RedPanda with Default Bucket
    return RedPanda(redshift_config, aws_config, s3_bucket)


def test_df_to_and_from_redshift_with_credentials(red_panda):
    red_panda.df_to_redshift(UPLOAD_DF, TABLE_NAME, append=False)
    queried = (
        red_panda.redshift_to_df(DOWNLOAD_SQL)
        .sort_values("col1")
        .reset_index(drop=True)
    )
    assert queried.equals(DOWNLOAD_DF)


def test_df_to_and_from_redshift_with_iam(red_panda, iam_role_arn):
    red_panda.df_to_redshift(UPLOAD_DF, TABLE_NAME, append=False, iam_role=iam_role_arn)
    queried = (
        red_panda.redshift_to_df(DOWNLOAD_SQL)
        .sort_values("col1")
        .reset_index(drop=True)
    )
    assert queried.equals(DOWNLOAD_DF)


def test_redshift_to_s3_base_format_parallel_off(red_panda, s3_client, s3_bucket):
    EXACT_FILE_PATH = "redshift-to-s3-parallel-off"
    red_panda.df_to_redshift(UPLOAD_DF, TABLE_NAME, append=False)
    red_panda.redshift_to_s3(DOWNLOAD_SQL, path=EXACT_FILE_PATH, parallel="OFF")
    res = (
        s3_client.get_object(Bucket=s3_bucket, Key=f"{EXACT_FILE_PATH}/000")
        .get("Body")
        .read()
    )
    assert sorted(res) == sorted(CONTENT_BYTE_STR)


def test_redshift_to_s3_base_format(red_panda, s3_client, s3_bucket):
    EXACT_FILE_PATH = "redshift-to-s3-base"
    red_panda.df_to_redshift(UPLOAD_DF, TABLE_NAME, append=False)
    red_panda.redshift_to_s3(DOWNLOAD_SQL, path=EXACT_FILE_PATH)
    res = (
        s3_client.get_object(Bucket=s3_bucket, Key=f"{EXACT_FILE_PATH}/0000_part_00")
        .get("Body")
        .read()
    )
    assert res in CONTENT_BYTE_STR


def test_redshift_to_s3_with_header(red_panda, s3_client, s3_bucket):
    EXACT_FILE_PATH = "redshift-to-s3-header"
    red_panda.df_to_redshift(UPLOAD_DF, TABLE_NAME, append=False)
    red_panda.redshift_to_s3(
        DOWNLOAD_SQL, path=EXACT_FILE_PATH, header=True, parallel="OFF"
    )
    res = (
        s3_client.get_object(Bucket=s3_bucket, Key=f"{EXACT_FILE_PATH}/000")
        .get("Body")
        .read()
    )
    assert sorted(res) == sorted(CONTENT_BYTE_PARALLEL_WITH_HEADER)


def test_redshift_to_s3_csv_format(red_panda, s3_client, s3_bucket):
    EXACT_FILE_PATH = "redshift-to-s3-csv"
    red_panda.df_to_redshift(UPLOAD_DF, TABLE_NAME, append=False)
    red_panda.redshift_to_s3(
        DOWNLOAD_SQL,
        path=EXACT_FILE_PATH,
        file_format="CSV",
        addquotes=False,
        parallel="OFF",
    )
    res = (
        s3_client.get_object(Bucket=s3_bucket, Key=f"{EXACT_FILE_PATH}/000")
        .get("Body")
        .read()
    )
    assert sorted(res) == sorted(CONTENT_BYTE_STR_CSV)
