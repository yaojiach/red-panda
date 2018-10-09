# -*- coding: utf-8 -*-
import re


def filter_kwargs(full, ref):
    return {k: v for k, v in full.items() if k in ref}


def prettify_sql(sql):
    return re.sub(r'\n\s*\n*', '\n', sql.lstrip())
