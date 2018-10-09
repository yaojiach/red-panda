# -*- coding: utf-8 -*-
import os
import re
from copy import deepcopy


def filter_kwargs(full, ref):
    return {k: v for k, v in full.items() if k in ref}


def prettify_sql(sql):
    return re.sub(r'\n\s*\n*', '\n', sql.lstrip())

def make_valid_uri(*args):
    if len(args) >= 2:
        l = deepcopy(list(args))
        i = l[1]
        if i[0] == '/':
            l[1] = i[1:]
    return os.path.join(*l)
