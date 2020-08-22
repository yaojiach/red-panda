import pandas as pd

from red_panda.pandas.utils import merge_dfs, row_number, groupby_distinct

import logging

LOGGER = logging.getLogger(__name__)


def test_merge_dfs():
    df1 = pd.DataFrame(data={"col0": [0, 1, 2], "col1": [1, 2, 3]})
    df2 = pd.DataFrame(data={"col0": [0, 1, 2], "col2": [1, 2, 3]})
    merged = pd.DataFrame(
        data={"col0": [0, 1, 2], "col1": [1, 2, 3], "col2": [1, 2, 3]}
    )
    assert merge_dfs([df1, df2]).equals(merged)


def test_row_number_return_series():
    df = pd.DataFrame({"group": [0, 0, 1], "sort": [1, 2, 3]})
    series = pd.Series([0, 1, 0])
    assert all(row_number(df, ["group"], ["sort"]) == series)


def test_groupby_distinct():
    df = pd.DataFrame(data={"col0": [0, 1, 1, 2, 2], "col1": [1, 2, 3, 4, 4]})
    computed = pd.DataFrame(data={"col0": [0, 1, 2], "col1": [1, 2, 1]})
    assert groupby_distinct(df, "col0", "col1").equals(computed)
