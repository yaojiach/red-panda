from functools import reduce
from typing import Union, List, Dict, Callable
import pandas as pd


def merge_dfs(dfs: List[pd.DataFrame], **kwargs) -> pd.DataFrame:
    """Merge a list of DataFrames on common columns.
    
    Args:
        dfs: A list of `pandas.DataFrame`s.
        **kwargs: Keyword arguments for `pandas.merge`.
    
    Returns:
        Merged DataFrame.
    """
    return reduce(lambda df1, df2: pd.merge(df1, df2, **kwargs), dfs)


def row_number(
    df: pd.DataFrame, group_by: List[str], sort_by: List[str], ascending: bool = True,
) -> pd.Series:
    """Create a row number series given a DataFrame lists of columns for group by and sort by.
    
    Args:
        df: Input DataFrame.
        group_by: List of group by columns.
        sort_by: List of sort by columns.
        col_name (optional): The output column name.
        ascending (optional): Whether sort in ascending order.
        as_series (optional): Whether to return a Series instead of a DataFrame.

    Returns:
        A DataFrame with row number or the row number Series.

    Example:
        >>> df = row_number(df, ['group'], ['sort'], as_series=False)
        >>> df['rn'] = row_number(df, ['group'], ['sort'])
    """
    return df.sort_values(sort_by, ascending=ascending).groupby(group_by).cumcount()


def groupby_mutate(
    df: pd.DataFrame, group_by: Union[List[str], str], func_dict: Dict[str, Callable]
) -> pd.DataFrame:
    """Similar to R's dplyr::mutate.

    Example:
        >>> def func(x):
                return x['a'].nunique() / x['b'].nunique()
        >>> func_dict = {
                'a_u': lambda x: x['a'].nunique(),
                'c': func
            }
        >>> groupby_mutate(df, 'b', func_dict)
    """
    return (
        df.groupby(group_by, group_keys=False)
        .apply(
            lambda x: pd.Series(
                {col: func(x) for col, func in func_dict.items()},
                index=list(func_dict.keys()),
            )
        )
        .reset_index()
    )


def groupby_distinct(
    df: pd.DataFrame, group_by: Union[List[str], str], distinct: str
) -> pd.DataFrame:
    """Unique count per group."""
    func_dict = {}
    func_dict[distinct] = lambda x: x[distinct].nunique()
    return groupby_mutate(df, group_by, func_dict)
