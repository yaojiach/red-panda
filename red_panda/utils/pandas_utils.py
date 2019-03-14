from functools import reduce

import pandas as pd


def merge_dfs(dfs, **kwargs):
    """Merge a list of DataFrames on common columns
    """
    return reduce(lambda df1, df2: pd.merge(df1, df2, **kwargs), dfs)

def row_number(df, group_by, sort_by, ascending=True):
    """Create a row number series given a DataFrame lists of columns for group by and sort by
    """
    return df.sort_values(sort_by, ascending=ascending).groupby(group_by).cumcount()
