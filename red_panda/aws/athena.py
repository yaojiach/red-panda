from pyathena import connect
from pyathena.util import as_pandas

from red_panda.aws import AWSUtils

class AthenaUtils(AWSUtils):
    """AWS Athena operations

    # Arguments:
        s3_staging_dir: str, full S3 folder uri, i.e. s3://athena-query/results

    # TODO: 
        - Complete Support for other cursor types.
        - Full parameters on `connect`
    """

    def __init__(self, aws_config, s3_staging_dir, region_name):
        super().__init__(aws_config=aws_config)
        self.cursor = connect(
            aws_access_key_id=self.aws_config.get("aws_access_key_id"),
            aws_secret_access_key=self.aws_config.get("aws_secret_access_key"),
            s3_staging_dir=s3_staging_dir,
            region_name=region_name,
        ).cursor()

    def run_sql(self, sql, as_df=False):
        self.cursor.execute(sql)
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
