import pytest
from red_panda.aws.athena import AthenaUtils

import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def athena_utils(aws_config, athena_result_location, aws_region):
    return AthenaUtils(aws_config, athena_result_location, aws_region)


def test_run_query(athena_utils, glue_data):
    sql = "select * from redpandatestgluedb.redpandatestgluetable"
    assert all(athena_utils.run_query(sql, as_df=True) == glue_data)
