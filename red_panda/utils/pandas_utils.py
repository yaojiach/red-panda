from functools import reduce

import pandas as pd


def merge_dfs(dfs, **kwargs):
    """Merge a list of DataFrames on common columns
    """
    return reduce(lambda df1, df2: pd.merge(df1, df2, **kwargs), dfs)


def row_number(df, group_by, sort_by, col_name='row_number', ascending=True, as_series=False):
    """Create a row number series given a DataFrame lists of columns for group by and sort by

    # Example:
        ```python
        df = row_number(df, ['group'], ['sort', 'order'])
        df['rn] = row_number(df, ['group'], ['sort', 'order'], as_series=True)
        ```
    """
    if as_series:
        return df.sort_values(sort_by, ascending=ascending).groupby(group_by).cumcount()
    else:
        if col_name in list(df.columns):
            raise ValueError(f'Column  {col_name} already exists.')
        df[col_name] = df.sort_values(
            sort_by, ascending=ascending
        ).groupby(group_by).cumcount()
        return df


def _groupby_mutate(df, group_by, mutate):
    """Internal base function similar to R's dplyr::mutate

    # Example
        ```python
        def def func(g):
            return g['b'] / g['b'].sum()
        
        df['pct'] = groupby_mutate(df, ['a'], func)
        ```
    """
    # return df.groupby(group_by, group_keys=False).apply(lambda g: eval(mutate))
    return df.groupby(group_by, group_keys=False).apply(mutate)


def groupby_mutate(df, group_by, func_dict):
    """Similar to R's dplyr::mutate

    # Example
        ```python
        def func(x):
            return x['a'].nunique() / x['b'].nunique()
        func_dict = {
            'a_u': lambda x: x['a'].nunique(),
            'c': func
        }
        # achieve the same:
        def f(x):
            d = {}
            d['a_u'] = x['a'].nunique()
            d['c'] = x['a'].nunique() / x['b'].nunique()
            return pd.Series(d, index=['a_u', 'c'])
        _groupby_mutate(df, ['group'], f).reset_index()
        ```
    """
    return _groupby_mutate(
        df,
        group_by,
        lambda x: pd.Series(
            { col: func(x) for col, func in func_dict.items() },
            index=list(func_dict.keys())
        )
    ).reset_index()


def groupby_distinct(df, group_by, distinct):
    func_dict = {}
    if isinstance(distinct, list):
        for d in distinct:
            func_dict[d] = lambda x: x[d].nunique()
    else:
        func_dict[distinct] = lambda x: x[distinct].nunique()
    return groupby_mutate(df, group_by, func_dict)
