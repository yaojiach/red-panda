# -*- coding: utf-8 -*-
import os
import re
import warnings
from collections import OrderedDict
from io import StringIO, BytesIO
from textwrap import dedent

import pandas as pd
import psycopg2
import boto3
import botocore
from awscli.clidriver import create_clidriver

from red_panda.constants import (
    RESERVED_WORDS, 
    TOCSV_KWARGS, 
    READ_TABLE_KWARGS, 
    COPY_KWARGS, 
    S3_PUT_KWARGS, 
    S3_GET_KWARGS, 
    TYPES_MAP, 
    S3_CREATE_BUCKET_KWARGS, 
    AWSCLI_CREATE_CLUSTER_KWARGS, 
    AWSCLI_CREATE_CLUSTER_ARGS
)
from red_panda.templates.aws.redshift_admin_templates import (
    SQL_NUM_SLICES, 
    SQL_TABLE_INFO, 
    SQL_TABLE_INFO_SIMPLIFIED, 
    SQL_LOAD_ERRORS, 
    SQL_RUNNING_INFO, 
    SQL_LOCK_INFO,
    SQL_TRANSACT_INFO
)
from red_panda.errors import ReservedWordError, S3BucketExists, S3BucketNotExist, S3KeyNotExist
from red_panda.utils import filter_kwargs, prettify_sql, make_valid_uri


def map_types(columns_types):
    """Convert Pandas dtypes to Redshift data types

    # Arguments
        cols_types: dict, return value of dict(df.dtypes)

    # Returns
        A dict of {original column name: mapped redshift data type}
    """
    return {c: {'data_type': TYPES_MAP.get(t.name)} \
            if TYPES_MAP.get(t.name) is not None else {'data_type': 'varchar(256)'} \
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


def create_column_definition_single(d):
    """

    # Arguments
        d: dict, a dict of values to compose a single column definition, defaults:
        {
            'data_type': 'varchar(256)', # str
            'default': None, # Any
            'identity': None, # tuple
            'encode': None, # str
            'distkey': False,
            'sortkey': False,
            'nullable': True,
            'unique': False,
            'primary_key': False,
            'foreign_key': False,
            'references': None, # str
            'like': None, # str
        }
    
    # TODO
        - Check validity of arguments before running, i.e. only one distkey is set. etc
    """
    data_type = d.get('data_type')
    data_type_option = data_type if data_type is not None else 'varchar(256)'
    default = d.get('default')
    quote = "'" if not isinstance(default, (int, float, complex)) else ''
    default_option = f"default {quote}{default}{quote}" if default is not None else ''
    identity = d.get('identity')
    if identity is not None:
        seed, step = identity
        identity_option = f'identity({seed}, {step})'
    else:
        identity_option = ''
    encode = d.get('encode')
    encode_option = f'encode {encode}' if encode is not None else ''
    distkey = d.get('distkey')
    distkey_option = 'distkey' if distkey is not None and distkey else ''
    sortkey = d.get('sortkey')
    sortkey_option = 'sortkey' if sortkey is not None and sortkey else ''
    nullable = d.get('nullable')
    nullable_option = 'not null' if nullable is not None and not nullable else ''
    unique = d.get('unique')
    unique_option = 'unique' if unique is not None and unique else ''
    primary_key = d.get('primary_key')
    primary_key_option = 'primary key' if primary_key is not None and primary_key else ''
    references = d.get('references')
    references_option = f'references {references}' if references is not None else ''
    like = d.get('like')
    like_option = f'like {like}' if like is not None else ''
    template = ' '.join([
        data_type_option,
        default_option,
        identity_option,
        encode_option,
        distkey_option,
        sortkey_option,
        nullable_option,
        unique_option,
        primary_key_option,
        references_option,
        like_option
    ])
    return ' '.join(template.split())


def create_column_definition(d):
    return ',\n'.join(f'{c} {create_column_definition_single(o)}' for c, o in d.items())


def set_aws_env_from_config(env, config):
    if config.get('aws_access_key_id') is not None:
        env['AWS_ACCESS_KEY_ID'] = config.get('aws_access_key_id')
    if config.get('aws_secret_access_key') is not None:
        env['AWS_SECRET_ACCESS_KEY'] = config.get('aws_secret_access_key')
    if config.get('aws_session_token') is not None:
        env['AWS_SESSION_TOKEN'] = config.get('aws_session_token')
    if config.get('metadata_service_timeout') is not None:
        env['AWS_METADATA_SERVICE_TIMEOUT'] = config.get('metadata_service_timeout')
    if config.get('metadata_service_num_attempts') is not None:
        env['AWS_METADATA_SERVICE_NUM_ATTEMPTS'] = config.get('metadata_service_num_attempts')
    return env


def run_awscli(*cmd, config=None):
    """Work around to run awscli commands for features not included in boto3

    # Example
        `run_awscli('s3', 'sync', 's3://bucket/source', 's3://bucket/destination', '--delete')`
    """
    old_env = os.environ.copy()
    try:
        env = os.environ.copy()
        env['LC_CTYPE'] = 'en_US.UTF'
        if config is not None:
            env = set_aws_env_from_config(env, config)
        os.environ.update(env)
        exit_code = create_clidriver().main([*cmd])
        if exit_code > 0:
            raise RuntimeError(f'awscli exited with code {exit_code}')
    finally:
        os.environ.clear()
        os.environ.update(old_env)


class RedshiftUtils:
    """ Base class for Redshift operations
    """
    def __init__(self, redshift_config, debug=False):
        self.redshift_config = redshift_config
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

    def run_sql_from_file(self, fpath):
        """Run a .sql file
        
        # TODO:
            - Add error handling
            - Add support for transactions
        """
        with open(fpath, 'r') as f:
            sql = f.read()
        self.run_query(sql)

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
            print(prettify_sql(sql))
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

    def cancel_query(self, pid, transaction=False):
        self.run_query(f"cancel {pid}")
        if transaction:
            self.run_query('abort')

    def kill_session(self, pid):
        self.run_query(f'select pg_terminate_backend({pid})')

    def get_num_slices(self):
        """Get number of slices of a Redshift cluster"""
        data, _ = self.run_query(SQL_NUM_SLICES, fetch=True)
        try:
            n_slices = data[0][0]
        except IndexError:
            print('Could not derive number of slices of Redshift cluster.')
        return n_slices

    def run_template(self, sql, as_df=True):
        data, columns = self.run_query(sql, fetch=True)
        if as_df:
            return pd.DataFrame(data, columns=columns)
        else:
            return (data, columns)

    def get_table_info(self, as_df=True, simple=False):
        sql = SQL_TABLE_INFO_SIMPLIFIED if simple else SQL_TABLE_INFO
        return self.run_template(sql, as_df)

    def get_load_error(self, as_df=True):
        return self.run_template(SQL_LOAD_ERRORS, as_df)

    def get_running_info(self, as_df=True):
        return self.run_template(SQL_RUNNING_INFO, as_df)

    def get_lock_info(self, as_df=True):
        return self.run_template(SQL_LOCK_INFO, as_df)

    def get_transaction_info(self, as_df=True):
        return self.run_template(SQL_TRANSACT_INFO, as_df)

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

    def redshift_to_file(self, sql, filename, **kwargs):
        """Redshift results to Pandas DataFrame

        # Arguments
            sql: str, SQL query
            filename: str, file name to save as
        # Returns
            None
        """
        data = self.redshift_to_df(sql)
        to_csv_kwargs = filter_kwargs(kwargs, TOCSV_KWARGS)
        data.to_csv(filename, **to_csv_kwargs)

    def create_table(
        self,
        table_name,
        column_definition,
        temp=False,
        if_not_exists=False,
        backup='YES',
        unique=None,
        primary_key=None,
        foreign_key=None,
        references=None,
        diststyle='EVEN',
        distkey=None,
        sortstyle='COMPOUND',
        sortkey=None,
        drop_first=False
    ):
        """Utility for creating table in Redshift

        # Arguments
            column_definition: dict, default:
            {
                'col1': {
                    'data_type': 'varchar(256)', # str
                    'default': None, # Any
                    'identity': None, # tuple(int, int)
                    'encode': None, # str
                    'distkey': False,
                    'sortkey': False,
                    'nullable': True,
                    'unique': False,
                    'primary_key': False,
                    'foreign_key': False,
                    'references': None, # str
                    'like': None, # str
                },
                'col2': {
                    ...
                },
                ...
            }

            unique: list[str]

            primary_key: str

            foreign_key: list[str], must match references.

            references: list[str], must match foreign_key.

            sortkey: list[str]

        # TODO
            - Complete doctring
            - Check consistency between column_constraints and table_contraints
            - More rigorous testing
        """
        if drop_first:
            self.run_query(f'drop table if exists {table_name}')
        temp_option = 'temp' if temp else ''
        exist_option = 'if not exists' if if_not_exists else ''
        unique_option = f'unique ({", ".join(unique)})' if unique is not None else ''
        primary_key_option = f'primary key ({primary_key})' if primary_key is not None else ''
        foreign_key_option = f'foreign key ({", ".join(foreign_key)})' \
                             if foreign_key is not None else ''
        references_option = f'references ({", ".join(references)})' \
                            if references is not None else ''
        distkey_option = f'distkey({distkey})' if distkey is not None else ''
        sortkey_option = f'{sortstyle} sortkey({" ".join(sortkey)})' if sortkey is not None else ''
        create_template = f"""\
        create table {temp_option} {table_name} {exist_option} (
        {create_column_definition(column_definition)}
        )
        backup {backup}
        diststyle {diststyle}
        {unique_option}
        {primary_key_option}
        {foreign_key_option}
        {references_option}
        {distkey_option}
        {sortkey_option}
        """
        self.run_query(create_template)


class AWSUtils:
    """ Base class for AWS operations
    """
    def __init__(self, aws_config):
        if aws_config is None:
            aws_config = {
                'aws_access_key_id': None,
                'aws_secret_access_key': None,
                'aws_session_token': None,
                'region_name': None
            }
        self.aws_config = aws_config 


class EMRUtils(AWSUtils):
    """ Base class for EMR operations
    """
    def __init__(self, aws_config=None):
        super().__init__(aws_config=aws_config)

    @classmethod
    def cli_to_config(cls, cli_cmd_str):
        """Translate awscli's create-cluster command to boto3.emr.run_job_flow parameters

        This is helpful when you manually configured EMR in AWS console and then export cli string
        for repeated use.
        """
        raise NotImplementedError

    @classmethod
    def validate_emr_create_cluster_config(cls, config):
        raise NotImplementedError

    def get_emr_client(self):
        """Get EMR client

        If key/secret are not provided, boto3's default behavior is falling back to awscli configs
        and environment variables.
        """
        emr = boto3.client(
            'emr',
            aws_access_key_id=self.aws_config.get('aws_access_key_id'),
            aws_secret_access_key=self.aws_config.get('aws_secret_access_key'),
            aws_session_token=self.aws_config.get('aws_session_token'),
            region_name=self.aws_config.get('region_name'),
        )
        return emr

    def cli_create_cluster(self, *args):
        """TODO: Use `run_job_flow` instead

        # Example
        ```
        emr.cli_create_cluster(
            '--auto-scaling-role', 'EMR_AutoScaling_DefaultRole',
            '--applications', 
            'Name=Hadoop', 
            'Name=Hive',
            'Name=Pig', 
            'Name=Hue', 
            'Name=Spark', 
            'Name=Tez', 
            'Name=Zeppelin',
            '--ebs-root-volume-size', '32',
            '--ec2-attributes', '''
            {
                "KeyName":"TODO-CHANGE-TO-ACTUAL",
                "InstanceProfile":"EMR_EC2_DefaultRole",
                "SubnetId":"TODO-CHANGE-TO-ACTUAL",
                "EmrManagedSlaveSecurityGroup":"TODO-CHANGE-TO-ACTUAL",
                "EmrManagedMasterSecurityGroup":"TODO-CHANGE-TO-ACTUAL",
                "AdditionalMasterSecurityGroups":["TODO-CHANGE-TO-ACTUAL","TODO-CHANGE-TO-ACTUAL"]}
            ''',
            '--service-role', 'EMR_DefaultRole',
            '--enable-debugging',
            '--release-label', 'emr-5.16.0',
            '--log-uri', 's3n://TODO-CHANGE-TO-ACTUAL/',
            '--name', "TODO-CHANGE-TO-ACTUAL",
            '--instance-groups', '''
            [
                {
                    "InstanceCount":4,
                    "EbsConfiguration":{
                        "EbsBlockDeviceConfigs":[
                            {
                                "VolumeSpecification":{
                                    "SizeInGB":32,"VolumeType":"gp2"
                                },
                                "VolumesPerInstance":1
                            }
                            ],
                        "EbsOptimized":true
                    },
                    "InstanceGroupType":"CORE",
                    "InstanceType":"m4.xlarge",
                    "Name":"Core - 2"
                },
                {
                    "InstanceCount":1,
                    "EbsConfiguration":{
                        "EbsBlockDeviceConfigs":[
                            {
                                "VolumeSpecification":{
                                    "SizeInGB":32,
                                    "VolumeType":"gp2"
                                },
                                "VolumesPerInstance":1
                            }
                            ]
                    },
                    "InstanceGroupType":"MASTER",
                    "InstanceType":"m4.2xlarge",
                    "Name":"Master - 1"}]
            ''',
            '--configurations', '''
            [
                {
                    "Classification":"hive-site",
                    "Properties":{
                        "hive.metastore.client.factory.class":\
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    },
                    "Configurations":[]
                },
                {
                    "Classification":"spark-hive-site",
                    "Properties":{
                        "hive.metastore.client.factory.class":\
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    },
                    "Configurations":[]
                }
            ]
            ''',
            '--scale-down-behavior', 'TERMINATE_AT_TASK_COMPLETION',
            '--region', 'us-east-1'
        )
        ```
        """
        run_awscli('emr', 'create-cluster', *args, config=self.aws_config)

    def create_cluster(self, config):
        """create cluster from config file

        # Returns
            Cluster ID: string
        """
        emr_client = self.get_emr_client()
        config = EMRUtils.validate_emr_create_cluster_config(config)
        return emr_client.run_job_flow(**config)

    def get_master_publicdns(self, cluster_id=None):
        emr_client = self.get_emr_client()
        desc = emr_client.describe_cluster(ClusterId=cluster_id)
        return desc['Cluster']['MasterPublicDnsName']


class S3Utils(AWSUtils):
    """ Base class for S3 operations
    """
    def __init__(self, aws_config):
        super().__init__(aws_config=aws_config)

    def _connect_s3(self):
        """Get S3 session

        If key/secret are not provided, boto3's default behavior is falling back to awscli configs
        and environment variables.
        """
        s3 = boto3.resource(
            's3',
            aws_access_key_id=self.aws_config.get('aws_access_key_id'),
            aws_secret_access_key=self.aws_config.get('aws_secret_access_key'),
            aws_session_token=self.aws_config.get('aws_session_token')
        )
        return s3
    
    def _check_s3_bucket_existence(self, bucket):
        s3 = self.get_s3_client()
        try:
            s3.head_bucket(Bucket=bucket)
        except botocore.errorfactory.ClientError:
            return False
        else:
            return True

    def _check_s3_key_existence(self, bucket, key):
        s3 = self.get_s3_client()
        try:
            s3.head_object(Bucket=bucket, Key=key)
        except botocore.errorfactory.ClientError:
            return False
        else:
            return True

    def _warn_s3_key_existence(self, bucket, key):
        if self._check_s3_key_existence(bucket, key):
            warnings.warn(f'{key} exists in {bucket}. May cause data consistency issues.')

    def _get_s3_pattern_existence(self, bucket, pattern):
        s3 = self.get_s3_resource()
        all_keys = [o.key for o in s3.Bucket(bucket).objects.all() if o.key.startswith(pattern)]
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
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        return buckets

    def list_object_keys(self, bucket, prefix=''):
        s3 = self.get_s3_client()
        response = s3.list_objects(Bucket=bucket, Prefix=prefix)
        return [o['Key'] for o in response['Contents']]

    def create_bucket(self, bucket, error='warn', response=False, **kwargs):
        """Check and create bucket

        # Argument
            bucket: str, s3 bucket name
            error: str, 'warn' or 'raise' or 'silent', how to handle if bucket already exists
        """
        s3 = self.get_s3_client()
        if self._check_s3_bucket_existence(bucket):
            if error == 'raise':
                raise S3BucketExists(f'{bucket} already exists')
            elif error == 'warn':
                warnings.warn(f'{bucket} already exists')
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
        self._warn_s3_key_existence(bucket, key)
        s3_put_kwargs = filter_kwargs(kwargs, S3_PUT_KWARGS)
        s3.meta.client.upload_file(file_name, Bucket=bucket, Key=key, ExtraArgs=s3_put_kwargs)       

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
        to_csv_kwargs = filter_kwargs(kwargs, TOCSV_KWARGS)
        df.to_csv(buffer, **to_csv_kwargs)
        self._warn_s3_key_existence(bucket, key)
        s3_put_kwargs = filter_kwargs(kwargs, S3_PUT_KWARGS)
        s3.Bucket(bucket).put_object(Key=key, Body=buffer.getvalue(), **s3_put_kwargs)

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

    def delete_bucket(self, bucket):
        """Delete bucket and all objects

        # TODO
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
        return BytesIO(obj['Body'].read())

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
        s3.Bucket(bucket).download_file(Key=key, Filename=file_name, ExtraArgs=s3_get_kwargs)

    def s3_to_df(self, bucket, key, **kwargs):
        """Read S3 object into memory as DataFrame

        Only supporting delimited files. Default is tab delimited files.

        # Arguments:
            bucket: str, S3 bucket name.
        
            key: str, S3 key.

            kwargs: Defined kwargs for pandas.read_table() and boto3.client.get_object().
        """
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        read_table_kwargs = filter_kwargs(kwargs, READ_TABLE_KWARGS)
        buffer = self.s3_to_obj(bucket, key, **s3_get_kwargs)
        return pd.read_csv(buffer, **read_table_kwargs)

    def s3_folder_to_df(self, bucket, folder, prefix=None, silent=True, **kwargs):
        s3_get_kwargs = filter_kwargs(kwargs, S3_GET_KWARGS)
        read_table_kwargs = filter_kwargs(kwargs, READ_TABLE_KWARGS)
        if folder[-1] != '/':
            folder = folder + '/'
        if prefix is None:
            prefix = '/'
        pattern = make_valid_uri(folder, prefix)
        allfiles = self.list_object_keys(bucket, pattern)
        allfiles = [f for f in allfiles if f != folder]
        dfs = []
        for f in allfiles:
            if not silent:
                print(f'Reading file {f}')
            dfs.append(
                self.s3_to_df(
                    bucket, 
                    f, 
                    **s3_get_kwargs,
                    **read_table_kwargs
                )
            )
        return pd.concat(dfs)

    def sync(self, source, destination, *args):
        """Sync two buckets or directories
        """
        run_awscli('s3', 'sync', source, destination, *args, config=self.aws_config)


class RedPanda(RedshiftUtils, S3Utils):
    """Class for operations between Pandas and Redshift/S3

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
    
    def __init__(self, redshift_config, aws_config=None, debug=False):
        RedshiftUtils.__init__(self, redshift_config, debug)
        S3Utils.__init__(self, aws_config)

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
        acceptinvchars='?',
        acceptanydate=False,
        blanksasnull=False,
        emptyasnull=False,
        escape=False,
        null=None,
        encoding=None,
        explicit_ids=False,
        fillrecord=False,
        ignoreblanklines=False,
        removequotes=False,
        roundec=False,
        trimblanks=False,
        truncatecolumns=False,
        column_list=None,
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
                drop_first = False if append else True
                self.create_table(table_name, column_definition, drop_first=drop_first)
                # drop_template = f'drop table if exists {table_name}'
                # self.run_query(drop_template)
                # column_definition_template = ','.join(f'{c} {t}' \
                #                                       for c, t in column_definition.items())
                # create_template = f'create table {table_name} ({column_definition_template})'
                # self.run_query(create_template)

        s3_source = f's3://{bucket}/{key}'
        quote_option = f"csv quote as '{quote_character}'" if delimiter == ',' and not escape else ''
        region_option = f"region '{region}'" if region is not None else ''
        escape_option = 'escape' if escape else ''
        acceptinvchars_option = f"acceptinvchars as '{acceptinvchars}'" \
                                if acceptinvchars is not None else ''
        acceptanydate_option = 'acceptanydate' if acceptanydate else ''
        blanksasnull_option = 'blanksasnull' if blanksasnull else ''
        emptyasnull_option = 'emptyasnull' if emptyasnull else ''
        explicit_ids_option = 'explicit_ids' if explicit_ids else ''
        fillrecord_option = 'fillrecord' if fillrecord else ''
        ignoreblanklines_option = 'ignoreblanklines' if ignoreblanklines else ''
        removequotes_option = 'removequotes' if removequotes else ''
        roundec_option = 'roundec' if roundec else ''
        trimblanks_option = 'trimblanks' if trimblanks else ''
        truncatecolumns_option = 'truncatecolumns' if truncatecolumns else ''
        encoding_option = f'encoding as {encoding}' if encoding is not None else ''
        null_option = f"null as '{null}'" if null is not None else ''
        aws_access_key_id = self.aws_config.get("aws_access_key_id")
        aws_secret_access_key = self.aws_config.get("aws_secret_access_key")
        if aws_access_key_id is None and aws_secret_access_key is None and iam_role is None:
            raise ValueError(
                'Must provide at least one of [iam_role, aws_access_key_id/aws_secret_access_key]'
            )
        aws_token = self.aws_config.get("aws_session_token")
        aws_token_option = f"session_token '{aws_token}'" if aws_token is not None else ''
        if iam_role is not None:
            iam_role_option = f"iam_role '{iam_role}'"
            access_key_id_option = ''
            secret_access_key_option = ''
        else:
            iam_role_option = ''
            access_key_id_option = f"access_key_id '{aws_access_key_id}'"
            secret_access_key_option = f"secret_access_key '{aws_secret_access_key}'"
        column_list_option = ''
        if column_list is not None:
            column_list_option = f"({','.join(column_list)})"
        copy_template = f"""\
        copy {table_name} {column_list_option}
        from '{s3_source}' 
        delimiter '{delimiter}'
        {quote_option}
        {escape_option}
        {acceptinvchars_option}
        {acceptanydate_option}
        {blanksasnull_option}
        {emptyasnull_option}
        {null_option}
        {encoding_option}
        {explicit_ids_option}
        {fillrecord_option}
        {removequotes_option}
        {roundec_option}
        {trimblanks_option}
        {truncatecolumns_option}
        {ignoreblanklines_option}
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
        destination_option = make_valid_uri(f's3://{bucket}', destination_option)
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
        aws_access_key_id = self.aws_config.get("aws_access_key_id")
        aws_secret_access_key = self.aws_config.get("aws_secret_access_key")
        if aws_access_key_id is None and aws_secret_access_key is None and iam_role is None:
            raise ValueError(
                'Must provide at least one of [iam_role, aws_access_key_id/aws_secret_access_key]'
            )
        aws_token = self.aws_config.get("aws_session_token")
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
