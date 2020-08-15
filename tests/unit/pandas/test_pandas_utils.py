import pandas as pd

from red_panda.pandas.utils import groupby_distinct

import logging

LOGGER = logging.getLogger(__name__)


def test_groupby_distinct():
    df = pd.DataFrame(data={"col0": [0, 1, 1, 2, 2], "col1": [1, 2, 3, 4, 4]})
    computed = pd.DataFrame(data={"col0": [0, 1, 2], "col1": [1, 2, 1]})
    assert all(groupby_distinct(df, "col0", "col1") == computed)
