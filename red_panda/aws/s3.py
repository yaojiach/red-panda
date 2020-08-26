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
from red_panda.aws import AWSUtils


LOGGER = logging.getLogger(__name__)


class S3Utils(AWSUtils):
    """AWS S3 operations.

    Args:
        aws_config: AWS configuration.

    Attributes:
        aws_config (dict): AWS configuration.
    """

    def __init__(self, aws_config: dict):
        super().__init__(aws_config=aws_config)

    def _connect_s3(self):
        """Get S3 session.

        If key/secret are not provided, boto3's default behavior is falling back to awscli configs
        and environment variables.
        """
        return boto3.resource(
            "s3",
            aws_access_key_id=self.aws_config.get("aws_access_key_id"),
            aws_secret_access_key=self.aws_config.get("aws_secret_access_key"),
            aws_session_token=self.aws_config.get("aws_session_token"),
        )

    def _check_s3_bucket_existence(self, bucket: str) -> bool:
        s3 = self.get_s3_client()
        try:
            s3.head_bucket(Bucket=bucket)
        except s3.exceptions.ClientError:
            LOGGER.warning(f"{bucket} does not exist or you do not have access to it.")
            return False
        else:
            return True

    def _check_s3_key_existence(self, bucket: str, key: str) -> bool:
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

    def _get_s3_pattern_existence(self, bucket: str, pattern: str) -> list:
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

    def list_buckets(self) -> list:
        """List all buckets.
        
        Returns:
            All S3 buckets for the account.
        """
        s3 = self.get_s3_client()
        response = s3.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        return buckets

    def list_object_keys(self, bucket: str, prefix: str = "") -> list:
        """List all object keys.

        Args:
            bucket: Bucket name.
            prefix: Any prefix for the object.

        Returns:
            A list of all objects in a bucket given certain prefix.
        """
        s3 = self.get_s3_client()
        response = s3.list_objects(Bucket=bucket, Prefix=prefix)
        return [o["Key"] for o in response["Contents"]]

    def create_bucket(self, bucket: str, error: str = "warn", **kwargs):
        """Check and create bucket.

        Args:
            bucket: S3 bucket name.
            error (optional): Specify `warn` or `raise` or `silent`. How to handle if bucket already
                exists. Default is `warn`.
            **kwargs: Additional keyword arguments for creating bucket.

        Returns:
            The response from `boto3.create_bucket`.
        """
        s3 = self.get_s3_client()
        if self._check_s3_bucket_existence(bucket):
            if error == "raise":
                raise ValueError(f"{bucket} already exists")
            elif error == "warn":
                warnings.warn(f"{bucket} already exists")
        extra_kwargs = filter_kwargs(kwargs, S3_CREATE_BUCKET_KWARGS)
        return s3.create_bucket(Bucket=bucket, **extra_kwargs)

    def file_to_s3(self, file_name: str, bucket: str, key: str, **kwargs):
        """Put a file to S3.

        Args:
            file_name: Local file name.
            bucket: S3 bucket name.
            key: S3 key.
            **kwargs: ExtraArgs for `boto3.client.upload_file`.
        """
        s3 = self._connect_s3()
        self._check_s3_bucket_existence(bucket)
        s3_put_kwargs = filter_kwargs(kwargs, S3_PUT_KWARGS)
        s3.meta.client.upload_file(
            file_name, Bucket=bucket, Key=key, ExtraArgs=s3_put_kwargs
        )

    def df_to_s3(self, df: pd.DataFrame, bucket: str, key: str, **kwargs):
        """Put DataFrame to S3.

        Args:
            df: Source dataframe.
            bucket: S3 bucket name.
            key: S3 key.
            **kwargs: kwargs for `boto3.Bucket.put_object` and `pandas.DataFrame.to_csv`.
        """
        s3 = self._connect_s3()
        buffer = StringIO()
        to_csv_kwargs = filter_kwargs(kwargs, PANDAS_TOCSV_KWARGS)
        df.to_csv(buffer, **to_csv_kwargs)
        self._check_s3_bucket_existence(bucket)
        s3_put_kwargs = filter_kwargs(kwargs, S3_PUT_KWARGS)
        s3.Bucket(bucket).put_object(Key=key, Body=buffer.getvalue(), **s3_put_kwargs)

    def delete_from_s3(self, bucket: str, key: str):
        """Delete object from S3.

        Args:
            bucket: S3 bucket name.
            key: S3 key.
        """
        s3 = self._connect_s3()
        if self._check_s3_key_existence(bucket, key):
            s3.meta.client.delete_object(Bucket=bucket, Key=key)
        else:
            LOGGER.warning(f"{bucket}: {key} does not exist.")

    def delete_bucket(self, bucket: str):
        """Empty and delete bucket.

        Args:
            bucket: S3 bucket name.

        TODO:
            * Handle when there is bucket versioning.
        """
        s3_bucket = self.get_s3_resource().Bucket(bucket)
        s3_bucket.objects.all().delete()
        s3_bucket.delete()

    def s3_to_obj(self, bucket: str, key: str, **kwargs) -> BytesIO:
        """Read S3 object into memory as BytesIO.

        Args:
            bucket: S3 bucket name.
            key: S3 key.
            **kwargs: kwargs for `boto3.client.get_object`.
        """
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        s3 = self.get_s3_client()
        obj = s3.get_object(Bucket=bucket, Key=key, **s3_get_kwargs)
        return BytesIO(obj["Body"].read())

    def s3_to_file(self, bucket: str, key: str, file_name: str, **kwargs):
        """Download S3 object as local file.

        Args:
            bucket: S3 bucket name.
            key: S3 key.
            file_name: Local file name.
            **kwargs: kwargs for `boto3.client.download_file`.
        """
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        s3 = self.get_s3_resource()
        s3.Bucket(bucket).download_file(
            Key=key, Filename=file_name, ExtraArgs=s3_get_kwargs
        )

    def s3_to_df(self, bucket: str, key: str, **kwargs):
        """Read S3 object into memory as DataFrame

        Only supporting delimited files. Default is tab delimited files.

        Args:
            bucket: S3 bucket name.
            key: S3 key.
            **kwargs: kwargs for `pandas.read_table` and `boto3.client.get_object`.

        Returns:
            A DataFrame.
        """
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        read_table_kwargs = filter_kwargs(kwargs, PANDAS_READ_TABLE_KWARGS)
        buffer = self.s3_to_obj(bucket, key, **s3_get_kwargs)
        return pd.read_csv(buffer, **read_table_kwargs)

    def s3_folder_to_df(self, bucket: str, folder: str, prefix: str = None, **kwargs):
        """Read all files in folder with prefix to a df.

        Args:
            bucket: S3 bucket name.
            folder: S3 folder.
            prefix: File prefix.

        Returns:
            A DataFrame.
        """
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        read_table_kwargs = filter_kwargs(kwargs, PANDAS_READ_TABLE_KWARGS)
        if folder[-1] != "/":
            folder = folder + "/"
        pattern = make_valid_uri(folder, prefix or "/")
        allfiles = [f for f in self.list_object_keys(bucket, pattern) if f != folder]
        dfs = []
        for f in allfiles:
            LOGGER.info(f"Reading file {f}")
            dfs.append(self.s3_to_df(bucket, f, **s3_get_kwargs, **read_table_kwargs))
        return pd.concat(dfs)
