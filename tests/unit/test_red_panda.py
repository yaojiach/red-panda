import pandas as pd
import numpy as np

from red_panda.red_panda import map_types


def test_map_types():
    PANDAS_TYPES = {"a": np.dtype("int64")}
    REDSHIFT_TYPES = {"a": {"data_type": "bigint"}}
    assert map_types(PANDAS_TYPES) == REDSHIFT_TYPES
