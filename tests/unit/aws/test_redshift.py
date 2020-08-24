import pytest
import psycopg2

from red_panda.aws.redshift import RedshiftUtils

import logging


LOGGER = logging.getLogger(__name__)


@pytest.fixture
def redshift_utils():
    return RedshiftUtils({})


def test_redshift_utils_run_query(mocker, redshift_utils):
    mock_connect = mocker.patch("psycopg2.connect").return_value
    mock_cursor = mock_connect.cursor.return_value
    mock_cursor.description = [["column_name"]]
    mock_cursor.fetchall.return_value = "value"
    res = redshift_utils.run_query("select * from table", fetch=True)
    assert res == ("value", ["column_name"])
