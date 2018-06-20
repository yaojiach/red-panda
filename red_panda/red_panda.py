# -*- coding: utf-8 -*-
import os
import warnings
from collections import OrderedDict
from io import StringIO
from textwrap import dedent

import pandas as pd
import psycopg2
import boto3
import botocore

from red_panda.constants import RESERVED_WORDS, TOCSV_KWARGS, COPY_KWARGS, TYPES_MAP
from red_panda.errors import ReservedWordError


def map_types(columns_types):
    """Convert Pandas dtypes to Redshift data types

    # Arguments
        cols_types: dict, return value of dict(df.dtypes)

    # Returns
        A dict of {original column name: mapped redshift data type}
    """
    return {c: TYPES_MAP.get(t.name) if TYPES_MAP.get(t.name) is not None else 'varchar(256)' \
            for c, t in columns_types.items()}


def check_invalid_columns(columns):
    """Check column names against Redshift reserved words

    # Arguments
        columns: list, list of column names to check

    # Raises
        `ReservedWordError`
    """
    invalid_df_col_names = []
    invalid_df_col_names = [c for c in columns if c in RESERVED_WORDS]
    if len(invalid_df_col_names) > 0:
        raise ReservedWordError('Redshift reserved words: f{invalid_df_col_names}')


class RedPanda:
    """Class for operations between Pandas and Redshift

    Solves interoperability between Pandas and Redshift through csv ingestion via S3.

    # Arguments
    
        redshift_conf: dict, Redshift configuration.
    
        s3_conf: dict, S3 configuration.

        debug: bool, if True, queries will be printed instead of executed.

    # References
        - https://github.com/getredash/redash for handling connections
        - https://github.com/agawronski/pandas_redshift for `df_to_redshift`
    """
    
    def __init__(self, redshift_config, s3_config=None, debug=False):
        self.redshift_config = redshift_config
        self.s3_config = s3_config
        self._debug = debug
    
    def _connect_redshift(self):
        connection = psycopg2.connect(
            user=self.redshift_config.get('user'),
            password=self.redshift_config.get('password'),
            host=self.redshift_config.get('host'),
            port=self.redshift_config.get('port'),
            dbname=self.redshift_config.get('dbname')
        )
        return connection

    def _connect_s3(self):
        s3 = boto3.resource(
            's3',
            aws_access_key_id=self.s3_config.get('aws_access_key_id'),
            aws_secret_access_key=self.s3_config.get('aws_secret_access_key'),
            aws_session_token=self.s3_config.get('aws_session_token')
        )
        return s3

    def _check_s3_key_existence(self, bucket, key):
        s3 = self._connect_s3()
        try:
            s3.meta.client.head_object(Bucket=bucket, Key=key)
        except botocore.errorfactory.ClientError:
            return False
        else:
            return True

    def _warn_s3_key_existence(self, bucket, key):
        if self._check_s3_key_existence(bucket, key):
            warnings.warn(f'{key} exists in {bucket}. May cause data consistency issues.')

    def _get_redshift_n_slices(self):
        """Get number of slices of a Redshift cluster

        # TODO

            This line `n_slices = data[0][0]` is a little sketchy

        """
        data, _ = self.run_query('select count(1) from stv_slices', fetch=True)
        try:
            n_slices = data[0][0]
        except IndexError:
            print('No information returned from: select count(1) from stv_slices')
        return n_slices

    def run_query(self, sql, fetch=False):
        """Run generic SQL

        # Argument
        
            sql: str
        
            fetch: bool, if or not to return data from the query.

        # Returns
            (data, columns) where data is a json/dict representation of the data and columns is a
            list of column names.
        """
        if self._debug:
            print(dedent(sql))
            return (None, None)

        conn = self._connect_redshift()
        cursor = conn.cursor()
        columns = None
        data = None
        try:
            cursor.execute(sql)
            if fetch:
                if cursor.description is not None:
                    columns = [desc[0] for desc in cursor.description]
                    data = cursor.fetchall()
                else:
                    print('Query completed but it returned no data.')
            else:
                conn.commit()
        except KeyboardInterrupt:
            conn.cancel()
            print('User canceled query.')
        finally:
            conn.close()
        return (data, columns)

    def df_to_s3(self, df, bucket, key, **kwargs):
        """Put DataFrame to S3
        
        # Arguments
        
            df: pandas.DataFrame, source dataframe.
        
            bucket: str, S3 bucket name.
        
            key: str, S3 key.
        """
        s3 = self._connect_s3()
        buffer = StringIO()
        to_csv_kwargs = {k: v for k, v in kwargs.items() if k in TOCSV_KWARGS}
        df.to_csv(buffer, **to_csv_kwargs)
        self._warn_s3_key_existence(bucket, key)
        s3.Bucket(bucket).put_object(Key=key, Body=buffer.getvalue())

    def file_to_s3(self, file_name, bucket, key):
        """Put a file to S3

        # Arguments

            file_name: str, path to file.
        
            bucket: str, S3 bucket name.
        
            key: str, S3 key.
        """
        s3 = self._connect_s3()
        self._warn_s3_key_existence(bucket, key)
        s3.meta.client.upload_file(file_name, Bucket=bucket, Key=key)       

    def delete_from_s3(self, bucket, key, silent=True):
        """Delete object from S3

        # Arguments

            bucket: str, S3 bucket name.
        
            key: str, S3 key.
        """
        s3 = self._connect_s3()
        if self._check_s3_key_existence(bucket, key):
            s3.meta.client.delete_object(Bucket=bucket, Key=key)
        else:
            if not silent:
                print(f'{bucket}: {key} does not exist.')

    def s3_to_redshift(
        self, 
        bucket, 
        key, 
        table_name, 
        column_definition=None, 
        append=False,
        delimiter=',',
        ignoreheader=1,
        quote_character='"',
        dateformat='auto',
        timeformat='auto',
        acceptinvchars=True,
        escape=False,
        null=None,
        region=None
    ):
        if not append:
            if column_definition is None:
                raise ValueError('column_definition cannot be None if append is False')
            else:
                drop_template = f'drop table if exists {table_name}'
                self.run_query(drop_template)
                column_definition_template = ','.join(f'{c} {t}' \
                                                      for c, t in column_definition.items())
                create_template = f'create table {table_name} ({column_definition_template})'
                self.run_query(create_template)

        s3_source = f's3://{bucket}/{key}'
        quote_option = f"csv quote as '{quote_character}'" if delimiter == ',' else ''
        region_option = f"region '{region}'" if region is not None else ''
        escape_option = 'escape' if escape else ''
        acceptinvchars_option = 'acceptinvchars' if acceptinvchars else ''
        null_option = f"null as '{null}'" if null is not None else ''
        aws_token = self.s3_config.get("aws_session_token")
        aws_token_option = f"session_token '{aws_token}'" if aws_token is not None else ''
        copy_template = f"""\
        copy {table_name}
        from '{s3_source}' 
        delimiter '{delimiter}'
        {quote_option}
        {escape_option}
        {acceptinvchars_option}
        {null_option}
        ignoreheader {ignoreheader}
        dateformat '{dateformat}'
        timeformat '{timeformat}'
        access_key_id '{self.s3_config.get("aws_access_key_id")}'
        secret_access_key '{self.s3_config.get("aws_secret_access_key")}'
        {aws_token_option}
        {region_option}
        """
        self.run_query(copy_template)

    def df_to_redshift(
        self, 
        df, 
        table_name, 
        bucket,
        column_definition=None, 
        append=False, 
        path=None, 
        file_name=None,
        cleanup=True,
        **kwargs
    ):
        """Pandas DataFrame to Redshift table

        # Arguments

            df: pandas.DataFrame, source dataframe.
        
            table_name: str, Redshift table name (optional include schema name).
        
            bucket: str, S3 bucket name.
        
            column_definition: dict, (optional) specify the column definition if for CREATE TABLE.
        
            If not given and append is False, data type will be inferred.
        
            append: bool, if true, df will be appended to Redshift table, otherwise table will be
        
            dropped and recreated.
        
            path: str, S3 key excluding file name.
        
            file_name: str, if `fname` is None, filename will be randomly generated.
        
            cleanup: bool, default true, S3 file will be deleted after COPY.
        
            kwargs: keyword arguments to pass to Pandas `to_csv` and Redshift COPY. 
            See red_panda.constants for all implemented arguments.
        """
        to_csv_kwargs = {k: v for k, v in kwargs.items() if k in TOCSV_KWARGS}
        copy_kwargs = {k: v for k, v in kwargs.items() if k in COPY_KWARGS}
        
        if column_definition is None:
            column_definition = map_types(OrderedDict(df.dtypes))

        if to_csv_kwargs.get('index'):
            if df.index.name:
                full_column_definition = OrderedDict({df.index.name: df.index.dtype})
            else:
                full_column_definition = OrderedDict({'index': df.index.dtype})
            full_column_definition = map_types(full_column_definition)
            full_column_definition.update(column_definition)
            column_definition = full_column_definition

        check_invalid_columns(list(column_definition))
        if file_name is None:
            import time
            file_name = f'redpanda-{int(time.time())}'
        s3_key = os.path.join(path if path is not None else '', file_name)
        self.df_to_s3(df, bucket=bucket, key=s3_key, **to_csv_kwargs)
        self.s3_to_redshift(
            bucket,
            s3_key,
            table_name,
            column_definition=column_definition,
            append=append,
            **copy_kwargs
        )
        if cleanup:
            self.delete_from_s3(bucket, s3_key)


    def redshift_to_df(self, sql):
        """Redshift results to Pandas DataFrame

        # Arguments
            sql: String, SQL query

        # Returns
            DataFrame of query result
        """
        data, columns = self.run_query(sql, fetch=True)
        data = pd.DataFrame(data, columns=columns)
        return data
