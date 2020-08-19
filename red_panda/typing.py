from typing import Tuple, Union
import pandas as pd


QueryResult = Tuple[Union[dict, None], Union[list, None]]
TemplateQueryResult = Union[QueryResult, pd.DataFrame]
AthenaQueryResult = Union[list, pd.DataFrame]
