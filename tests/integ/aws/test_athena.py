import pytest
from red_panda.aws.athena import AthenaUtils

import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def athena_utils(aws_config, athena_result_location, aws_region):
    return AthenaUtils(
        aws_config, athena_result_location, region_name=aws_region, work_group="primary"
    )


def test_athena_run_query_return_df(athena_utils, glue_data, glue_db, glue_table_name):
    sql = f"select * from {glue_db}.{glue_table_name}"
    assert athena_utils.run_query(sql, as_df=True).equals(glue_data)


def test_athena_run_query_return_list(
    athena_utils, glue_data, glue_db, glue_table_name
):
    sql = f"select * from {glue_db}.{glue_table_name}"
    assert all(
        [
            x == y
            for x, y in zip(
                athena_utils.run_query(sql, as_df=False), glue_data.to_dict("records")
            )
        ]
    )


def test_athena_run_query_use_cache(athena_utils, glue_db, glue_table_name):
    # TODO: more robust test
    import random
    import string

    sql = f"""select col0 as {''.join(random.choices(string.ascii_lowercase, k=10))} 
    from {glue_db}.{glue_table_name}"""
    athena_utils.run_query(sql)
    query_id_1 = athena_utils.cursor.query_id
    athena_utils.run_query(sql, use_cache=True)
    query_id_2 = athena_utils.cursor.query_id
    assert query_id_1 == query_id_2
