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

def groupby_mutate(df, group_by, mutate):
    """Similar to R's dplyr::mutate

    # Example
        ```python
        def def func(g):
            return g['b'] / g['b'].sum()
        
        df['pct'] = groupby_mutate(df, ['a'], func)
        ```
    """
    # return df.groupby(group_by, group_keys=False).apply(lambda g: eval(mutate))
    return df.groupby(group_by, group_keys=False).apply(mutate)
