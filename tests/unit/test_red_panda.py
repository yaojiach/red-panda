import pytest
import numpy as np

from red_panda.red_panda import map_types, check_invalid_columns


def test_map_types():
    PANDAS_TYPES = {"a": np.dtype("int64")}
    REDSHIFT_TYPES = {"a": {"data_type": "bigint"}}
    assert map_types(PANDAS_TYPES) == REDSHIFT_TYPES


def test_check_invalid_columns_raises():
    WITH_RESERVED_WORD = ["column"]
    with pytest.raises(ValueError):
        check_invalid_columns(WITH_RESERVED_WORD)
