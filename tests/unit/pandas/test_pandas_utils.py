import pandas as pd

from red_panda.pandas.utils import (
    merge_dfs,
    row_number,
    groupby_mutate
)

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


def test_groupby_mutate():
    df = pd.DataFrame({"group": [0, 0, 1, 1], "x": [1, 1, 1, 3]})
    result = pd.DataFrame(
        {"group": [0, 0, 1, 1], "x": [1, 1, 1, 3], "new": [0.5, 0.5, 0.25, 0.75]}
    )
    mutated = groupby_mutate(df, "group", {"new": lambda x: x["x"] / sum(x["x"])})
    assert mutated.equals(result)


def test_groupby_mutate_inplace():
    df = pd.DataFrame({"group": [0, 0, 1, 1], "x": [1, 1, 1, 3]})
    result = pd.DataFrame(
        {"group": [0, 0, 1, 1], "x": [1, 1, 1, 3], "new": [0.5, 0.5, 0.25, 0.75]}
    )
    groupby_mutate(df, "group", {"new": lambda x: x["x"] / sum(x["x"])}, inplace=True)
    assert df.equals(result)
