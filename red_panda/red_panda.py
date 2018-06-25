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

from red_panda.constants import RESERVED_WORDS, TOCSV_KWARGS, COPY_KWARGS, S3_PUT_KWARGS, TYPES_MAP
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


def filter_kwargs(full, ref):
    return {k: v for k, v in full.items() if k in ref}


class RedPanda:
    """Class for operations between Pandas and Redshift

    Solves interoperability between Pandas and Redshift through csv ingestion via S3.

    # Arguments
        redshift_conf: dict, Redshift configuration.
    
        s3_conf: dict, S3 configuration.

        debug: bool, if True, queries will be printed instead of executed.

    # References
        - https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html for COPY
        - https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html for UNLOAD
        - https://github.com/getredash/redash for handling connections
        - https://github.com/agawronski/pandas_redshift for inspiration
    """
    
    def __init__(self, redshift_config, s3_config=None, debug=False):
        if s3_config is None:
            s3_config = {
                'aws_access_key_id': None,
                'aws_secret_access_key': None,
                'aws_session_token': None,
            }
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
        """Get S3 session

        If key/secret are not provided, boto3's default behavior is falling back to awscli configs
        and environment variables.
        """
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

    def _get_s3_pattern_existence(self, bucket, pattern):
        s3 = self._connect_s3()
        all_keys = [o.key for o in s3.Bucket(bucket).objects.all() if o.key.startswith(pattern)]
        return all_keys

    def _get_redshift_n_slices(self):
        """Get number of slices of a Redshift cluster"""
        data, _ = self.run_query('select count(1) from stv_slices', fetch=True)
        try:
            n_slices = data[0][0]
        except IndexError:
            print('Could not derive number of slices of Redshift cluster.')
        return n_slices

    def get_s3_resource(self):
        """Return a boto3 S3 resource"""
        return self._connect_s3()
    
    def get_s3_client(self):
        """Return a boto3 S3 client"""
        return self._connect_s3().meta.client

    def run_query(self, sql, fetch=False):
        """Run generic SQL

        # Arguments
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

            kwargs: kwargs for boto3.Bucket.put_object(); kwargs to pandas.DataFrame.to_csv();
        """
        s3 = self._connect_s3()
        buffer = StringIO()
        to_csv_kwargs = filter_kwargs(kwargs, TOCSV_KWARGS)
        df.to_csv(buffer, **to_csv_kwargs)
        self._warn_s3_key_existence(bucket, key)
        s3_put_kwargs = filter_kwargs(kwargs, S3_PUT_KWARGS)
        s3.Bucket(bucket).put_object(Key=key, Body=buffer.getvalue(), **s3_put_kwargs)

    def file_to_s3(self, file_name, bucket, key, **kwargs):
        """Put a file to S3

        # Arguments
            file_name: str, path to file.
        
            bucket: str, S3 bucket name.
        
            key: str, S3 key.

            kwargs: ExtraArgs for boto3.client.upload_file();
        """
        s3 = self._connect_s3()
        self._warn_s3_key_existence(bucket, key)
        s3_put_kwargs = filter_kwargs(kwargs, S3_PUT_KWARGS)
        s3.meta.client.upload_file(file_name, Bucket=bucket, Key=key, ExtraArgs=s3_put_kwargs)       

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
        region=None,
        iam_role=None
    ):
        """Load S3 file into Redshift

        # Arguments
            bucket: str, S3 bucket name.
            
            key: str, S3 key.

            table_name: str, Redshift table name (optional include schema name).

            column_definition: dict, (optional) specify the column definition if for CREATE TABLE.

            append: bool, if True, df will be appended to Redshift table, otherwise table will be
            dropped and recreated.

            delimiter: str, delimiter of file. Default is ",".
            
            ignoreheader: int, number of header lines to skip when COPY.
            
            quote_character: str, QUOTE_CHARACTER for COPY. Only used when delimiter is ",".
            Default to '"'.
            
            dateformat: str, TIMEFORMAT argument for COPY. Default is "auto". 
            
            timeformat: str, TIMEFORMAT argument for COPY. Default is "auto".
            
            acceptinvchars: bool, whether to include the ACCEPTINVCHAR argument in COPY.
            
            escape: bool, whether to include the ESCAPE argument in COPY.
            
            null: str, specify the NULL AS string.
            
            region: str, S3 region.

            iam_role: str, use IAM Role for access control. This feature is untested.
        """
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
        aws_access_key_id = self.s3_config.get("aws_access_key_id")
        aws_secret_access_key = self.s3_config.get("aws_secret_access_key")
        if aws_access_key_id is None and aws_secret_access_key is None and iam_role is None:
            raise ValueError(
                'Must provide at least one of [iam_role, aws_access_key_id/aws_secret_access_key]'
            )
        aws_token = self.s3_config.get("aws_session_token")
        aws_token_option = f"session_token '{aws_token}'" if aws_token is not None else ''
        if iam_role is not None:
            iam_role_option = f"iam_role '{iam_role}'"
            access_key_id_option = ''
            secret_access_key_option = ''
        else:
            iam_role_option = ''
            access_key_id_option = f"access_key_id '{aws_access_key_id}'"
            secret_access_key_option = f"secret_access_key '{aws_secret_access_key}'"
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
        {access_key_id_option}
        {secret_access_key_option}
        {aws_token_option}
        {iam_role_option}
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
        
            kwargs: keyword arguments to pass to Pandas `to_csv` and Redshift COPY. See 
            red_panda.constants for all implemented arguments.
        """
        to_csv_kwargs = filter_kwargs(kwargs, TOCSV_KWARGS)
        copy_kwargs = filter_kwargs(kwargs, COPY_KWARGS)
        
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
        try:
            self.s3_to_redshift(
                bucket,
                s3_key,
                table_name,
                column_definition=column_definition,
                append=append,
                **copy_kwargs
            )
        finally:
            if cleanup:
                self.delete_from_s3(bucket, s3_key)

    def redshift_to_df(self, sql):
        """Redshift results to Pandas DataFrame

        # Arguments
            sql: str, SQL query

        # Returns
            DataFrame of query result
        """
        data, columns = self.run_query(sql, fetch=True)
        data = pd.DataFrame(data, columns=columns)
        return data

    def redshift_to_s3(
        self, 
        sql, 
        bucket, 
        path=None, 
        prefix=None,
        iam_role=None,
        manifest=False,
        delimiter='|',
        fixedwidth=None,
        encrypted=False,
        bzip2=False,
        gzip=False,
        addquotes=True,
        null=None,
        escape=False,
        allowoverwrite=False,
        parallel='ON',
        maxfilesize=None
    ):
        """Run sql and unload result to S3

        # Arguments
            sql: str, SQL query

            bucket: str, S3 bucket name.
            
            key: str, S3 key. Create if does not exist.

            prefix: str, prefix of the set of files.

            iam_role: str, IAM Role string. If provided, this will be used as authorization instead
            of access_key_id/secret_access_key. This feature is untested.

            manifest: bool, whether or not to create the manifest file.

            delimiter: str, delimiter charater if the output file is delimited.

            fixedwidth: str, if not None, it will overwrite delimiter and use fixedwidth format
            instead.
            
            encrypted: bool, whether or not the files should be encrypted.
            
            bzip2: bool, whether or not the files should be compressed with bzip2.
            
            gzip: bool, whether or not the files should be compressed with gzip.
            
            addquotes: bool, whether or not values with delimiter characters should be quoted.
            
            null: str, specify the NULL AS string.
            
            escape: bool, whether to include the ESCAPE argument in UNLOAD.
            
            allowoverwrite: bool, whether or not existing files should be overwritten. Redshift will
            fail with error message if this is False and there are existing files.
            
            parallel: str, ON or OFF. Whether or not to use parallel and unload into multiple files.
            
            maxfilesize: str, maxfilesize argument for UNLOAD.
        """
        destination_option = ''
        if path is not None:
            destination_option = os.path.join(destination_option, f'{path}')
        if prefix is not None:
            destination_option = os.path.join(destination_option, f'{prefix}')
        existing_keys = self._get_s3_pattern_existence(bucket, destination_option)
        warn_message = f"""\
        These keys already exist. May cause data consistency issues.
        {existing_keys}
        """
        warnings.warn(dedent(warn_message))
        destination_option = os.path.join(f's3://{bucket}', destination_option)
        if bzip2 and gzip:
            raise ValueError('Only one of [bzip2, gzip] should be True')
        manifest_option = 'manifest' if manifest else ''
        delimiter_option = f"delimiter '{delimiter}'"
        if fixedwidth is not None:
            fixedwidth_option = f"fixedwidth '{fixedwidth}'"
            delimiter_option = ''
        else:
            fixedwidth_option = ''
        encrypted_option = 'encrypted' if encrypted else ''
        bzip2_option = 'bzip2' if bzip2 else ''
        gzip_option = 'gzip' if gzip else ''
        addquotes_option = 'addquotes' if addquotes else ''
        null_option = f"null as '{null}'" if null is not None else ''
        escape_option = 'escape' if escape else ''
        allowoverwrite_option = 'allowoverwrite' if allowoverwrite else ''
        parallel_option = f"parallel {parallel}"
        maxfilesize_option = f"maxfilesize '{maxfilesize}'" if maxfilesize is not None else ''
        aws_access_key_id = self.s3_config.get("aws_access_key_id")
        aws_secret_access_key = self.s3_config.get("aws_secret_access_key")
        if aws_access_key_id is None and aws_secret_access_key is None and iam_role is None:
            raise ValueError(
                'Must provide at least one of [iam_role, aws_access_key_id/aws_secret_access_key]'
            )
        aws_token = self.s3_config.get("aws_session_token")
        aws_token_option = f"session_token '{aws_token}'" if aws_token is not None else ''
        if iam_role is not None:
            iam_role_option = f"iam_role '{iam_role}'"
            access_key_id_option = ''
            secret_access_key_option = ''
        else:
            iam_role_option = ''
            access_key_id_option = f"access_key_id '{aws_access_key_id}'"
            secret_access_key_option = f"secret_access_key '{aws_secret_access_key}'"
        sql = sql.replace('\n', ' ')
        unload_template = f"""\
        unload ('{sql}')
        to '{destination_option}'
        {manifest_option}
        {delimiter_option}
        {fixedwidth_option}
        {encrypted_option}
        {bzip2_option}
        {gzip_option}
        {addquotes_option}
        {null_option}
        {escape_option}
        {allowoverwrite_option}
        {parallel_option}
        {maxfilesize_option}
        {access_key_id_option}
        {secret_access_key_option}
        {aws_token_option}
        {iam_role_option}
        """
        self.run_query(unload_template)
