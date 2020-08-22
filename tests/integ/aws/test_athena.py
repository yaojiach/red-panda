import pytest
from red_panda.aws.athena import AthenaUtils

import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def athena_utils(aws_config, athena_result_location, aws_region):
    return AthenaUtils(aws_config, athena_result_location, aws_region)


def test_run_query_return_df(athena_utils, glue_data):
    sql = "select * from redpandatestgluedb.redpandatestgluetable"
    assert athena_utils.run_query(sql, as_df=True).equals(glue_data)


def test_run_query_return_list(athena_utils, glue_data):
    sql = "select * from redpandatestgluedb.redpandatestgluetable"
    assert all(
        [
            x == y
            for x, y in zip(
                athena_utils.run_query(sql, as_df=False), glue_data.to_dict("records")
            )
        ]
    )
