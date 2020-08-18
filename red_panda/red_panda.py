import warnings
from collections import OrderedDict
from textwrap import dedent
import logging

from red_panda.pandas import PANDAS_TOCSV_KWARGS
from red_panda.aws import (
    REDSHIFT_RESERVED_WORDS,
    REDSHIFT_COPY_KWARGS,
)
from red_panda.utils import filter_kwargs, make_valid_uri
from red_panda.aws.s3 import S3Utils
from red_panda.aws.redshift import RedshiftUtils


LOGGER = logging.getLogger(__name__)


def map_types(columns_types: dict):
    """Convert Pandas dtypes to Redshift data types.

    Args:
        cols_types: The return value of `dict(df.dtypes)`, where `df` is a Pandas Dataframe.

    Returns:
        dict: A `dict` of `{original column name: mapped redshift data type}`
    """
    PANDAS_REDSHIFT_TYPES_MAP = {
        "O": "varchar(256)",
        "object": "varchar(256)",
        "int64": "bigint",
        "float64": "double precision",
        "boolean": "boolean",
    }
    return {
        c: {"data_type": PANDAS_REDSHIFT_TYPES_MAP.get(t.name)}
        if PANDAS_REDSHIFT_TYPES_MAP.get(t.name) is not None
        else {"data_type": "varchar(256)"}
        for c, t in columns_types.items()
    }


def check_invalid_columns(columns: list):
    """Check column names against Redshift reserved words.

    Args:
        columns: A list of column names to check.

    Raises:
        ValueError: If the column name is invalid.
    """
    invalid_df_col_names = []
    invalid_df_col_names = [c for c in columns if c in REDSHIFT_RESERVED_WORDS]
    if len(invalid_df_col_names) > 0:
        raise ValueError("Redshift reserved words: f{invalid_df_col_names}")


class RedPanda(RedshiftUtils, S3Utils):
    """Class for operations between Pandas and Redshift/S3.

    Args:
        redshift_conf (dict): Redshift configuration.
        aws_conf (dict, optional): AWS configuration.
        default_bucket (str, optional): Default bucket to store files.
        dryrun (bool, optional): If True, queries will be printed instead of executed.
    
    Attributes:
        redshift_conf (dict): Redshift configuration.
        aws_conf (dict): AWS configuration.
        default_bucket (str): Default bucket to store files.
    """

    def __init__(
        self, redshift_config, aws_config=None, default_bucket=None, dryrun=False
    ):
        RedshiftUtils.__init__(self, redshift_config, dryrun)
        S3Utils.__init__(self, aws_config)
        self.default_bucket = default_bucket

    def s3_to_redshift(
        self,
        bucket,
        key,
        table_name,
        column_definition=None,
        append=False,
        delimiter=",",
        ignoreheader=1,
        quote_character='"',
        dateformat="auto",
        timeformat="auto",
        acceptinvchars="?",
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
        iam_role=None,
    ):
        """Load S3 file into Redshift.

        Args:
            bucket (str): S3 bucket name.
            key (str): S3 key.
            table_name (str): Redshift table name (optional include schema name).
            column_definition (dict, optional): Specify the column definition if for CREATE TABLE.
            append (bool, optional): Ff True, df will be appended to Redshift table, otherwise table 
                will be dropped and recreated.
            delimiter (str, optional): Delimiter of file. Default is ",".
            ignoreheader (int, optional): number of header lines to skip when COPY.
            quote_character (str, optional): QUOTE_CHARACTER for COPY. Only used when delimiter is 
                ",". Default to '"'.
            dateformat (str, optional): TIMEFORMAT argument for COPY. Default is "auto". 
            timeformat (str, optional): TIMEFORMAT argument for COPY. Default is "auto".
            acceptinvchars (bool, optional): Whether to include the ACCEPTINVCHAR argument in COPY.
            escape (bool, optional): Whether to include the ESCAPE argument in COPY.
            null (str, optional): Specify the NULL AS string.
            region (str, optional): S3 region.
            iam_role (str, optional): Use IAM Role for access control. This feature is untested.
        """
        if not append:
            if column_definition is None:
                raise ValueError("column_definition cannot be None if append is False")
            else:
                drop_first = False if append else True
                self.create_table(table_name, column_definition, drop_first=drop_first)

        s3_source = f"s3://{bucket}/{key}"
        quote_option = (
            f"csv quote as '{quote_character}'"
            if delimiter == "," and not escape
            else ""
        )
        region_option = f"region '{region}'" if region is not None else ""
        escape_option = "escape" if escape else ""
        acceptinvchars_option = (
            f"acceptinvchars as '{acceptinvchars}'"
            if acceptinvchars is not None
            else ""
        )
        acceptanydate_option = "acceptanydate" if acceptanydate else ""
        blanksasnull_option = "blanksasnull" if blanksasnull else ""
        emptyasnull_option = "emptyasnull" if emptyasnull else ""
        explicit_ids_option = "explicit_ids" if explicit_ids else ""
        fillrecord_option = "fillrecord" if fillrecord else ""
        ignoreblanklines_option = "ignoreblanklines" if ignoreblanklines else ""
        removequotes_option = "removequotes" if removequotes else ""
        roundec_option = "roundec" if roundec else ""
        trimblanks_option = "trimblanks" if trimblanks else ""
        truncatecolumns_option = "truncatecolumns" if truncatecolumns else ""
        encoding_option = f"encoding as {encoding}" if encoding is not None else ""
        null_option = f"null as '{null}'" if null is not None else ""
        aws_access_key_id = self.aws_config.get("aws_access_key_id")
        aws_secret_access_key = self.aws_config.get("aws_secret_access_key")
        if (
            aws_access_key_id is None
            and aws_secret_access_key is None
            and iam_role is None
        ):
            raise ValueError(
                "Must provide at least one of [iam_role, aws_access_key_id/aws_secret_access_key]"
            )
        aws_token = self.aws_config.get("aws_session_token")
        aws_token_option = (
            f"session_token '{aws_token}'" if aws_token is not None else ""
        )
        if iam_role is not None:
            iam_role_option = f"iam_role '{iam_role}'"
            access_key_id_option = ""
            secret_access_key_option = ""
        else:
            iam_role_option = ""
            access_key_id_option = f"access_key_id '{aws_access_key_id}'"
            secret_access_key_option = f"secret_access_key '{aws_secret_access_key}'"
        column_list_option = ""
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
        bucket=None,
        column_definition=None,
        append=False,
        path=None,
        file_name=None,
        cleanup=True,
        **kwargs,
    ):
        """Pandas DataFrame to Redshift table.

        Args:
            df (pandas.DataFrame): Source dataframe.
            table_name (str): Redshift table name (optionally include schema name).
            bucket (str, optional): S3 bucket name, fallback to `default_bucket` if not present.
            column_definition (dict, optional): Specify the column definition for CREATE TABLE. If 
                not given and append is False, data type will be inferred.
            append (bool, optional): If true, df will be appended to Redshift table, otherwise table 
                will be dropped and recreated.
            path (str, optional): S3 key excluding file name.
            file_name (str, optional): If None, filename will be randomly generated.
            cleanup: (bool, optional): Default True, S3 file will be deleted after COPY.
            **kwargs: keyword arguments to pass to Pandas `to_csv` and Redshift COPY.
        """
        bridge_bucket = bucket or self.default_bucket
        if not bridge_bucket:
            raise ValueError("Either bucket or default_bucket must be provided.")

        to_csv_kwargs = filter_kwargs(kwargs, PANDAS_TOCSV_KWARGS)
        copy_kwargs = filter_kwargs(kwargs, REDSHIFT_COPY_KWARGS)

        if column_definition is None:
            column_definition = map_types(OrderedDict(df.dtypes))

        # default pandas behavior is true when index is not specified
        if to_csv_kwargs.get("index") is None or to_csv_kwargs.get("index"):
            if df.index.name:
                full_column_definition = OrderedDict({df.index.name: df.index.dtype})
            else:
                full_column_definition = OrderedDict({"index": df.index.dtype})
            full_column_definition = map_types(full_column_definition)
            full_column_definition.update(column_definition)
            column_definition = full_column_definition

        check_invalid_columns(list(column_definition))

        if file_name is None:
            import uuid

            file_name = f"redpanda-{uuid.uuid4()}"

        s3_key = make_valid_uri(path if path is not None else "", file_name)
        self.df_to_s3(df, bucket=bridge_bucket, key=s3_key, **to_csv_kwargs)
        try:
            self.s3_to_redshift(
                bridge_bucket,
                s3_key,
                table_name,
                column_definition=column_definition,
                append=append,
                **copy_kwargs,
            )
        finally:
            if cleanup:
                self.delete_from_s3(bridge_bucket, s3_key)

    def redshift_to_s3(
        self,
        sql,
        bucket,
        path=None,
        prefix=None,
        iam_role=None,
        manifest=False,
        delimiter="|",
        fixedwidth=None,
        encrypted=False,
        bzip2=False,
        gzip=False,
        addquotes=True,
        null=None,
        escape=False,
        allowoverwrite=False,
        parallel="ON",
        maxfilesize=None,
    ):
        """Run sql and unload result to S3.

        Args:
            sql (str): SQL query.
            bucket (str): S3 bucket name.
            key (str, optional): S3 key. Create if does not exist.
            prefix (str, optional): Prefix of the set of files.
            iam_role (str, optional): IAM Role string. If provided, this will be used as 
                authorization instead of access_key_id/secret_access_key. This feature is untested.
            manifest (bool, optional): Whether or not to create the manifest file.
            delimiter (str, optional): Delimiter charater if the output file is delimited.
            fixedwidth (str, optional): If not None, it will overwrite delimiter and use fixedwidth 
                format instead.
            encrypted (bool, optional): Whether or not the files should be encrypted.
            bzip2 (bool, optional): Whether or not the files should be compressed with bzip2.
            gzip (bool, optional): Whether or not the files should be compressed with gzip.
            addquotes (bool, optional): Whether or not values with delimiter characters should be 
                quoted.
            null (str, optional): Specify the NULL AS string.
            escape (bool, optional): Whether to include the ESCAPE argument in UNLOAD.
            allowoverwrite (bool, optional): Whether or not existing files should be overwritten. 
                Redshift will fail with error message if this is False and there are existing files.
            parallel (str, optional): ON or OFF. Whether or not to use parallel and unload into 
                multiple files.
            maxfilesize (str, optional): Maxfilesize argument for UNLOAD.
        """
        destination_option = ""
        if path is not None:
            destination_option = make_valid_uri(destination_option, f"{path}")
            if destination_option[-1] != "/":
                destination_option = destination_option + "/"
        if prefix is not None:
            destination_option = make_valid_uri(destination_option, f"{prefix}")
        existing_keys = self._get_s3_pattern_existence(bucket, destination_option)
        warn_message = f"""\
        These keys already exist. It may cause data consistency issues.
        {existing_keys}
        """
        warnings.warn(dedent(warn_message))
        destination_option = make_valid_uri(f"s3://{bucket}", destination_option)
        if bzip2 and gzip:
            raise ValueError("Only one of [bzip2, gzip] should be True")
        manifest_option = "manifest" if manifest else ""
        delimiter_option = f"delimiter '{delimiter}'"
        if fixedwidth is not None:
            fixedwidth_option = f"fixedwidth '{fixedwidth}'"
            delimiter_option = ""
        else:
            fixedwidth_option = ""
        encrypted_option = "encrypted" if encrypted else ""
        bzip2_option = "bzip2" if bzip2 else ""
        gzip_option = "gzip" if gzip else ""
        addquotes_option = "addquotes" if addquotes else ""
        null_option = f"null as '{null}'" if null is not None else ""
        escape_option = "escape" if escape else ""
        allowoverwrite_option = "allowoverwrite" if allowoverwrite else ""
        parallel_option = f"parallel {parallel}"
        maxfilesize_option = (
            f"maxfilesize '{maxfilesize}'" if maxfilesize is not None else ""
        )
        aws_access_key_id = self.aws_config.get("aws_access_key_id")
        aws_secret_access_key = self.aws_config.get("aws_secret_access_key")
        if (
            aws_access_key_id is None
            and aws_secret_access_key is None
            and iam_role is None
        ):
            raise ValueError(
                "Must provide at least one of [iam_role, aws_access_key_id/aws_secret_access_key]"
            )
        aws_token = self.aws_config.get("aws_session_token")
        aws_token_option = (
            f"session_token '{aws_token}'" if aws_token is not None else ""
        )
        if iam_role is not None:
            iam_role_option = f"iam_role '{iam_role}'"
            access_key_id_option = ""
            secret_access_key_option = ""
        else:
            iam_role_option = ""
            access_key_id_option = f"access_key_id '{aws_access_key_id}'"
            secret_access_key_option = f"secret_access_key '{aws_secret_access_key}'"
        sql = sql.replace("\n", " ")
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
