from multiprocessing import Value
from pyathena import connect
from pyathena.util import as_pandas

from red_panda.typing import AthenaQueryResult
from red_panda.aws import AWSUtils

import logging


LOGGER = logging.getLogger(__name__)


class AthenaUtils(AWSUtils):
    """AWS Athena operations.

    Args:
        aws_config: AWS configuration.
        s3_staging_dir: Full S3 folder uri, i.e. s3://athena-query/results.
        region_name: AWS region name.

    Attributes:
        aws_config: AWS configuration.
        s3_staging_dir: Full S3 folder uri, i.e. s3://athena-query/results.
        region_name: AWS region name.

    TODO:
        * Complete Support for other cursor types.
        * Full parameters on `connect`.
    """

    def __init__(
        self,
        aws_config: dict,
        s3_staging_dir: dict,
        work_group: str = None,
        region_name: str = None,
    ):
        super().__init__(aws_config=aws_config)
        self.work_group = work_group
        self.cursor = connect(
            aws_access_key_id=self.aws_config.get("aws_access_key_id"),
            aws_secret_access_key=self.aws_config.get("aws_secret_access_key"),
            s3_staging_dir=s3_staging_dir,
            work_group=work_group,
            region_name=region_name or aws_config.get("region_name"),
        ).cursor()

    def run_query(
        self, sql: str, as_df: bool = False, use_cache: bool = False
    ) -> AthenaQueryResult:
        """Run query on Athena.

        Args:
            sql: SQL query.
            as_df (optional): Whether to return the result as DataFrame.

        Returns:
            Query result as DataFrame or List[Tuple].
        """
        cache_size = 0
        if use_cache:
            if self.work_group is None:
                raise ValueError("Workgroup must be specified to use cache.")
            cache_size = 50
        self.cursor.execute(sql, cache_size=cache_size)

        if as_df:
            return as_pandas(self.cursor)

        res = []
        desc = self.cursor.description
        for row in self.cursor:
            r = {}
            for i, c in enumerate(desc):
                r[c[0]] = row[i]
            res.append(r)
        return res
