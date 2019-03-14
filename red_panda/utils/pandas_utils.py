from functools import reduce

import pandas as pd


def merge_dfs(dfs, **kwargs):
    """Merge a list of DataFrames on common columns
    """
    return reduce(lambda df1, df2: pd.merge(df1, df2, **kwargs), dfs)
