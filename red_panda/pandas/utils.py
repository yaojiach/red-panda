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
    df: pd.DataFrame,
    group_by: Union[List[str], str],
    func_dict: Dict[str, Callable],
    inplace: bool = False,
) -> pd.DataFrame:
    """Similar to R's dplyr::mutate.

    Example:
        >>> def func(x):
                return x["x"] / sum(x["x"])
        >>> func_dict = {
                'ratio': x["x"] / sum(x["x"])
            }
        >>> groupby_mutate(df, "b", func_dict)
    """
    out = df if inplace else df.copy()
    for col, func in func_dict.items():
        out[col] = out.groupby(group_by, group_keys=False).apply(func)
    return out
