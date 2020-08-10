import os
import re
from copy import deepcopy
from typing import Iterable


def filter_kwargs(full, ref):
    return {k: v for k, v in full.items() if k in ref}


def prettify_sql(sql):
    return re.sub(r"\n\s*\n*", "\n", sql.lstrip())


def make_valid_uri(*args):
    if len(args) >= 2:
        l = deepcopy(list(args))
        i = l[1]
        if i[0] == "/":
            l[1] = i[1:]
    return os.path.join(*l)


def flatten(items):
    """Yield items from any nested iterable; see Reference.
    """
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flatten(x):
                yield sub_x
        else:
            yield x


def index_of_dict_in_list(l, value, key=None):
    """Get index of item (a dict) in list

    # Example:
    ```python
    l = [
        {'a': 1},
        {'a': 2},
        {'a': 3}
    ]
    index_of_dict_in_list(l, 2) # returns 1
    ```
    """
    if key is None:
        for i, d in enumerate(l):
            (v,) = d.values()
            if v == value:
                return i
    else:
        for i, d in enumerate(l):
            if d[key] == value:
                return i
    raise ValueError("No value matched")
