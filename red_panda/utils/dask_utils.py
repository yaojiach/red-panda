import dask
import dask.dataframe as dd


def clean_column_name(column_names, out_dict=True):
    """Clean raw column names and output a dict for DataFrame.rename to consume
    """
    renamed = [c.replace('.', '_').lower() for c in column_names]
    if out_dict:
        return dict(zip(column_names, renamed))
    else:
        return renamed


def make_cat_feat_name(n):
    import re
    return re.sub(r'[^a-zA-Z_]+', '_', n).lower()


def make_cat_feat(dd, feature, groups, prefix=None, pandas=False):
    """When using dask.DataFrame, execution is delayed
    """
    if prefix is None:
        prefix = feature
    featnames = []
    for f in groups:
        featname = prefix + '_' + make_cat_feat_name(f)
        featnames.append(featname)
        if pandas:
            dd[featname] = 0
            dd.loc[dd[feature] == f, featname] = 1
        else: # dask
            dd[featname] = 1
            dd[featname] = dd[featname].where(dd[feature] == f, 0)
    return {'dd': dd, 'features': featnames}


# Dask custom function for group by count distinct
def _group_by_count_distinct_chunk(s):
    """The function applied to the individual partition (map)
    """   
    return s.apply(lambda x: list(set(x)))

def _group_by_count_distinct_agg(s):
    """The function which will aggregate the result from all the partitions(reduce)
    """
    s = s._selected_obj    
    return s.groupby(level=list(range(s.index.nlevels))).sum()

def _group_by_count_distinct_finalize(s):
    """The optional functional that will be applied to the result of the agg functions
    """
    return s.apply(lambda x: len(set(x)))

group_by_count_distinct = dd.Aggregation(
    'group_by_count_distinct',
    _group_by_count_distinct_chunk,
    _group_by_count_distinct_agg,
    _group_by_count_distinct_finalize
)
