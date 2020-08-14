import pytest
from red_panda import RedPanda
import pandas as pd

import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def red_panda(redshift_config, aws_config, s3_bucket):
    # RedPanda with Default Bucket
    return RedPanda(redshift_config, aws_config, s3_bucket)


def test_df_to_and_from_redshift(red_panda):
    TABLE_NAME = "test_table"
    DOWNLOAD_SQL = f"select * from {TABLE_NAME}"
    DOWNLOAD_DF = pd.DataFrame(
        data={"index": [0, 1], "col1": [1, 2], "col2": ["x", "y"]}
    )
    UPLOAD_DF = DOWNLOAD_DF[["col1", "col2"]]
    red_panda.df_to_redshift(UPLOAD_DF, TABLE_NAME, append=False)
    assert all(red_panda.redshift_to_df(DOWNLOAD_SQL) == DOWNLOAD_DF)


def test_df_to_and_from_redshift_with_iam(red_panda, iam_role_arn):
    TABLE_NAME = "test_table"
    DOWNLOAD_SQL = f"select * from {TABLE_NAME}"
    DOWNLOAD_DF = pd.DataFrame(
        data={"index": [0, 1], "col1": [1, 2], "col2": ["x", "y"]}
    )
    UPLOAD_DF = DOWNLOAD_DF[["col1", "col2"]]
    red_panda.df_to_redshift(UPLOAD_DF, TABLE_NAME, append=False, iam_role=iam_role_arn)
    assert all(red_panda.redshift_to_df(DOWNLOAD_SQL) == DOWNLOAD_DF)
