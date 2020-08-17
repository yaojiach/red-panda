import warnings
from io import StringIO, BytesIO
import logging

import pandas as pd
import boto3

from red_panda.pandas import PANDAS_TOCSV_KWARGS, PANDAS_READ_TABLE_KWARGS
from red_panda.aws import (
    S3_PUT_KWARGS,
    S3_GET_KWARGS,
    S3_CREATE_BUCKET_KWARGS,
)
from red_panda.utils import filter_kwargs, make_valid_uri
from red_panda.aws import AWSUtils, run_awscli


LOGGER = logging.getLogger(__name__)


class S3Utils(AWSUtils):
    """AWS S3 operations
    """

    def __init__(self, aws_config):
        super().__init__(aws_config=aws_config)

    def _connect_s3(self):
        """Get S3 session

        If key/secret are not provided, boto3's default behavior is falling back to awscli configs
        and environment variables.
        """
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=self.aws_config.get("aws_access_key_id"),
            aws_secret_access_key=self.aws_config.get("aws_secret_access_key"),
            aws_session_token=self.aws_config.get("aws_session_token"),
        )
        return s3

    def _check_s3_bucket_existence(self, bucket):
        s3 = self.get_s3_client()
        try:
            s3.head_bucket(Bucket=bucket)
        except s3.exceptions.ClientError:
            LOGGER.warning(f"{bucket} does not exist or you do not have access to it.")
            return False
        else:
            return True

    def _check_s3_key_existence(self, bucket, key):
        s3 = self.get_s3_client()
        try:
            s3.head_object(Bucket=bucket, Key=key)
        except s3.exceptions.ClientError:
            LOGGER.warning(
                f"{bucket}/{key} does not exist or you do not have access to it."
            )
            return False
        else:
            return True

    def _get_s3_pattern_existence(self, bucket, pattern):
        s3 = self.get_s3_resource()
        all_keys = [
            o.key for o in s3.Bucket(bucket).objects.all() if o.key.startswith(pattern)
        ]
        return all_keys

    def get_s3_resource(self):
        """Return a boto3 S3 resource"""
        return self._connect_s3()

    def get_s3_client(self):
        """Return a boto3 S3 client"""
        return self._connect_s3().meta.client

    def list_buckets(self):
        s3 = self.get_s3_client()
        response = s3.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        return buckets

    def list_object_keys(self, bucket, prefix=""):
        s3 = self.get_s3_client()
        response = s3.list_objects(Bucket=bucket, Prefix=prefix)
        return [o["Key"] for o in response["Contents"]]

    def create_bucket(self, bucket, error="warn", response=False, **kwargs):
        """Check and create bucket

        # Argument
            bucket: str, s3 bucket name
            error: str, 'warn' or 'raise' or 'silent', how to handle if bucket already exists
        """
        s3 = self.get_s3_client()
        if self._check_s3_bucket_existence(bucket):
            if error == "raise":
                raise ValueError(f"{bucket} already exists")
            elif error == "warn":
                warnings.warn(f"{bucket} already exists")
        extra_kwargs = filter_kwargs(kwargs, S3_CREATE_BUCKET_KWARGS)
        res = s3.create_bucket(Bucket=bucket, **extra_kwargs)
        if response:
            return res

    def file_to_s3(self, file_name, bucket, key, **kwargs):
        """Put a file to S3

        # Arguments
            file_name: str, path to file.

            bucket: str, S3 bucket name.

            key: str, S3 key.

            kwargs: ExtraArgs for boto3.client.upload_file().
        """
        s3 = self._connect_s3()
        self._check_s3_bucket_existence(bucket)
        s3_put_kwargs = filter_kwargs(kwargs, S3_PUT_KWARGS)
        s3.meta.client.upload_file(
            file_name, Bucket=bucket, Key=key, ExtraArgs=s3_put_kwargs
        )

    def df_to_s3(self, df, bucket, key, **kwargs):
        """Put DataFrame to S3

        # Arguments
            df: pandas.DataFrame, source dataframe.

            bucket: str, S3 bucket name.

            key: str, S3 key.

            kwargs: kwargs for boto3.Bucket.put_object(); kwargs to pandas.DataFrame.to_csv().
        """
        s3 = self._connect_s3()
        buffer = StringIO()
        to_csv_kwargs = filter_kwargs(kwargs, PANDAS_TOCSV_KWARGS)
        df.to_csv(buffer, **to_csv_kwargs)
        self._check_s3_bucket_existence(bucket)
        s3_put_kwargs = filter_kwargs(kwargs, S3_PUT_KWARGS)
        s3.Bucket(bucket).put_object(Key=key, Body=buffer.getvalue(), **s3_put_kwargs)

    def delete_from_s3(self, bucket, key):
        """Delete object from S3

        # Arguments
            bucket: str, S3 bucket name.

            key: str, S3 key.
        """
        s3 = self._connect_s3()
        if self._check_s3_key_existence(bucket, key):
            s3.meta.client.delete_object(Bucket=bucket, Key=key)
        else:
            LOGGER.warning(f"{bucket}: {key} does not exist.")

    def delete_bucket(self, bucket):
        """Delete bucket and all objects

        # TODO:
            Handle when there is bucket versioning
        """
        s3_bucket = self.get_s3_resource().Bucket(bucket)
        s3_bucket.objects.all().delete()
        s3_bucket.delete()

    def s3_to_obj(self, bucket, key, **kwargs):
        """Read S3 object into memory as BytesIO

        # Arguments:
            bucket: str, S3 bucket name.

            key: str, S3 key.

            kwargs: Defined kwargs for boto3.client.get_object().
        """
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        s3 = self.get_s3_client()
        obj = s3.get_object(Bucket=bucket, Key=key, **s3_get_kwargs)
        return BytesIO(obj["Body"].read())

    def s3_to_file(self, bucket, key, file_name, **kwargs):
        """
        # Arguments:
            bucket: str, S3 bucket name.

            key: str, S3 key.

            file_name: str, local file name.

            kwargs: Defined kwargs for boto3.client.download_file().
        """
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        s3 = self.get_s3_resource()
        s3.Bucket(bucket).download_file(
            Key=key, Filename=file_name, ExtraArgs=s3_get_kwargs
        )

    def s3_to_df(self, bucket, key, **kwargs):
        """Read S3 object into memory as DataFrame

        Only supporting delimited files. Default is tab delimited files.

        # Arguments:
            bucket: str, S3 bucket name.

            key: str, S3 key.

            kwargs: Defined kwargs for pandas.read_table() and boto3.client.get_object().
        """
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        read_table_kwargs = filter_kwargs(kwargs, PANDAS_READ_TABLE_KWARGS)
        buffer = self.s3_to_obj(bucket, key, **s3_get_kwargs)
        return pd.read_csv(buffer, **read_table_kwargs)

    def s3_folder_to_df(self, bucket, folder, prefix=None, **kwargs):
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        read_table_kwargs = filter_kwargs(kwargs, PANDAS_READ_TABLE_KWARGS)
        if folder[-1] != "/":
            folder = folder + "/"
        if prefix is None:
            prefix = "/"
        pattern = make_valid_uri(folder, prefix)
        allfiles = self.list_object_keys(bucket, pattern)
        allfiles = [f for f in allfiles if f != folder]
        dfs = []
        for f in allfiles:
            LOGGER.info(f"Reading file {f}")
            dfs.append(self.s3_to_df(bucket, f, **s3_get_kwargs, **read_table_kwargs))
        return pd.concat(dfs)

    def sync(self, source, destination, *args):
        """Sync two buckets or directories
        """
        run_awscli("s3", "sync", source, destination, *args, config=self.aws_config)
