import warnings
from collections import OrderedDict
from textwrap import dedent
from typing import Union, Optional, List
import logging

import pandas as pd

from red_panda.pandas import PANDAS_TOCSV_KWARGS
from red_panda.aws import (
    REDSHIFT_RESERVED_WORDS,
    REDSHIFT_COPY_KWARGS,
)
from red_panda.utils import filter_kwargs, make_valid_uri
from red_panda.aws.s3 import S3Utils
from red_panda.aws.redshift import RedshiftUtils


LOGGER = logging.getLogger(__name__)


def map_types(columns_types: dict) -> dict:
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
    invalid_df_col_names = [c for c in columns if c in REDSHIFT_RESERVED_WORDS]
    if len(invalid_df_col_names) > 0:
        raise ValueError(f"Redshift reserved words: {invalid_df_col_names}")


class RedPanda(RedshiftUtils, S3Utils):
    """Class for operations between Pandas and Redshift/S3.

    Args:
        redshift_config: Redshift configuration.
        aws_config (optional): AWS configuration.
        default_bucket (optional): Default bucket to store files.
        dryrun (optional): If True, queries will be printed instead of executed.
    
    Attributes:
        redshift_config (dict): Redshift configuration.
        aws_config (dict): AWS configuration.
        default_bucket (str): Default bucket to store files.
    """

    def __init__(
        self,
        redshift_config: dict,
        aws_config: dict,
        default_bucket: str = None,
        dryrun: bool = False,
    ):
        RedshiftUtils.__init__(self, redshift_config, dryrun)
        S3Utils.__init__(self, aws_config)
        self.default_bucket = default_bucket

    def s3_to_redshift(
        self,
        bucket: str,
        key: str,
        table_name: str,
        column_definition: dict = None,
        append: bool = False,
        delimiter: str = ",",
        ignoreheader: int = 1,
        quote_character: str = '"',
        dateformat: str = "auto",
        timeformat: str = "auto",
        acceptinvchars: str = "?",
        acceptanydate: bool = False,
        blanksasnull: bool = False,
        emptyasnull: bool = False,
        escape: bool = False,
        null: str = None,
        encoding: str = None,
        explicit_ids: bool = False,
        fillrecord: bool = False,
        ignoreblanklines: bool = False,
        removequotes: bool = False,
        roundec: bool = False,
        trimblanks: bool = False,
        truncatecolumns: bool = False,
        region: str = None,
        iam_role: str = None,
        column_list: list = None,
    ):
        """Load S3 file into Redshift.

        Args:
            bucket: S3 bucket name.
            key: S3 key.
            table_name: Redshift table name (optional include schema name).
            column_definition (optional): Specify the column definition if for CREATE TABLE.
            append (optional): Ff True, df will be appended to Redshift table, otherwise table will 
                be dropped and recreated.
            delimiter (optional): Delimiter of file. Default is ",".
            ignoreheader (optional): number of header lines to skip when COPY.
            quote_character (optional): QUOTE_CHARACTER for COPY. Only used when delimiter is ",". 
                Default to '"'.
            dateformat (optional): TIMEFORMAT argument for COPY. Default is "auto". 
            timeformat (optional): TIMEFORMAT argument for COPY. Default is "auto".
            acceptinvchars (optional): Whether to include the ACCEPTINVCHAR argument in COPY.
            acceptanydate (optional): Allows any date format, including invalid formats.
            blanksasnull (optional): Loads blank fields, which consist of only white space 
                characters, as NULL.
            emptyasnull (optional): Indicates that Amazon Redshift should load empty CHAR and 
                VARCHAR fields as NULL.
            escape (optional): Whether to include the ESCAPE argument in COPY.
            null (optional): Specify the NULL AS string.
            encoding (optional): Specifies the encoding type of the load data.
            explicit_ids (optional): Use EXPLICIT_IDS with tables that have IDENTITY column.
            fillrecord (optional): Allows data files to be loaded when contiguous columns are 
                missing at the end of some of the records.
            ignoreblanklines (optional): Ignores blank lines that only contain a line feed in a data
                file and does not try to load them.
            removequotes (optional): Removes surrounding quotation marks from strings in the 
                incoming data.
            roundec (optional): Rounds up numeric values when the scale of the input value is 
                greater than the scale of the column. 
            trimblanks (optional): Removes the trailing white space characters from a VARCHAR 
                string.
            truncatecolumns (optional): Truncates data in columns to the appropriate number of 
                characters so that it fits the column specification.
            region (optional): S3 region.
            iam_role (optional): Use IAM Role for access control.
            column_list (optional): List of columns to COPY.
        
        TODO:
            * Handle S3 client side encryption.
            * Handle COPY using manifest file.
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
        df: pd.DataFrame,
        table_name: str,
        bucket: str = None,
        column_definition: dict = None,
        append: bool = False,
        path: str = None,
        file_name: str = None,
        cleanup: bool = True,
        **kwargs,
    ):
        """Pandas DataFrame to Redshift table.

        Args:
            df: Source dataframe.
            table_name: Redshift table name (optionally include schema name).
            bucket (optional): S3 bucket name, fallback to `default_bucket` if not present.
            column_definition (optional): Specify the column definition for CREATE TABLE. If 
                not given and append is False, data type will be inferred.
            append (optional): If true, df will be appended to Redshift table, otherwise table 
                will be dropped and recreated.
            path (optional): S3 key excluding file name.
            file_name (optional): If None, file_name will be randomly generated.
            cleanup: (optional): Default True, S3 file will be deleted after COPY.
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
        sql: str,
        bucket: str = None,
        path: str = None,
        prefix: str = None,
        iam_role: str = None,
        file_format: str = None,
        partition_by: List[str] = None,
        include_partition_column: bool = False,
        manifest: bool = False,
        header: bool = False,
        delimiter: str = None,
        fixedwidth: Union[str, int] = None,
        encrypted: bool = False,
        bzip2: bool = False,
        gzip: bool = False,
        zstd: bool = False,
        addquotes: bool = False,
        null: str = None,
        escape: bool = False,
        allowoverwrite: bool = False,
        parallel: str = "ON",
        maxfilesize: Union[str, int, float] = None,
        region: str = None,
    ):
        """Run sql and unload result to S3.

        Args:
            sql: SQL query.
            bucket: S3 bucket name.
            key (optional): S3 key. Create if does not exist.
            prefix (optional): Prefix of the set of files.
            iam_role (optional): IAM Role string. If provided, this will be used as authorization 
                instead of access_key_id/secret_access_key. This feature is untested.
            file_format (optional): CSV or PARQUET.
            manifest (optional): Whether or not to create the manifest file.
            header (optional): Whether or not to include header.
            delimiter (optional): Delimiter charater if the output file is delimited.
            fixedwidth (optional): If not None, it will overwrite delimiter and use fixedwidth 
                format instead.
            encrypted (optional): Whether or not the files should be encrypted.
            bzip2 (optional): Whether or not the files should be compressed with bzip2.
            gzip (optional): Whether or not the files should be compressed with gzip.
            zstd (optional): Whether or not the files should be compressed with zstd.
            addquotes (optional): Whether or not values with delimiter characters should be quoted.
            null (optional): Specify the NULL AS string.
            escape (optional): Whether to include the ESCAPE argument in UNLOAD.
            allowoverwrite (optional): Whether or not existing files should be overwritten. Redshift
                will fail with error message if this is False and there are existing files.
            parallel (optional): ON or OFF. Whether or not to use parallel and unload into multiple 
                files.
            maxfilesize (optional): Maxfilesize argument for UNLOAD.
            region (optional): AWS region if S3 region is different from Redshift region.
        """
        destination_option = ""
        if path is not None:
            destination_option = make_valid_uri(destination_option, f"{path}")
            if destination_option[-1] != "/":
                destination_option = destination_option + "/"
        if prefix is not None:
            destination_option = make_valid_uri(destination_option, f"{prefix}")
        dest_bucket: Optional[str] = bucket or self.default_bucket
        if dest_bucket is None:
            raise ValueError("bucket cannot be None.")
        existing_keys = self._get_s3_pattern_existence(dest_bucket, destination_option)
        if existing_keys:
            warn_message = f"""\
            These keys already exist. It may cause data consistency issues.
            {existing_keys}
            """
            warnings.warn(dedent(warn_message))
        destination_option = make_valid_uri(f"s3://{dest_bucket}", destination_option)

        if sum([bzip2, gzip, zstd]) > 1:
            raise ValueError("Only one of [bzip2, gzip, zstd] should be True.")

        file_format_option = ""
        if file_format is not None:
            if file_format == "CSV":
                if fixedwidth:
                    raise ValueError(
                        "fixedwidth should not be specified for CSV file_format."
                    )
                delimiter = ","
                file_format_option = "format CSV"
            elif file_format == "PARQUET":
                file_format_option = "format PARQUET"
                if delimiter:
                    raise ValueError(
                        "delimiter should not be specified for PARQUET file_format."
                    )
                if fixedwidth:
                    raise ValueError(
                        "fixedwidth should not be specified for PARQUET file_format."
                    )
                if addquotes:
                    raise ValueError(
                        "addquotes should not be specified for PARQUET file_format."
                    )
                if escape:
                    raise ValueError(
                        "escape should not be specified for PARQUET file_format."
                    )
                if null:
                    raise ValueError(
                        "null should not be specified for PARQUET file_format."
                    )
                if header:
                    raise ValueError(
                        "header should not be specified for PARQUET file_format."
                    )
                if gzip:
                    raise ValueError(
                        "gzip should not be specified for PARQUET file_format."
                    )
                if bzip2:
                    raise ValueError(
                        "bzip2 should not be specified for PARQUET file_format."
                    )
                if zstd:
                    raise ValueError(
                        "zstd should not be specified for PARQUET file_format."
                    )
            else:
                raise ValueError("File format can only be CSV or PARQUET if specified.")

        partition_include_option = " INCLUDE" if include_partition_column else ""
        partition_option = (
            f"{','.join(partition_by)}{partition_include_option}"
            if partition_by
            else ""
        )

        manifest_option = "manifest" if manifest else ""
        header_option = "header" if header else ""
        delimiter_option = f"delimiter '{delimiter}'" if delimiter else "delimiter '|'"
        if fixedwidth is not None:
            fixedwidth_option = f"fixedwidth '{fixedwidth}'"
            delimiter_option = ""
        else:
            fixedwidth_option = ""
        encrypted_option = "encrypted" if encrypted else ""
        bzip2_option = "bzip2" if bzip2 else ""
        gzip_option = "gzip" if gzip else ""
        zstd_option = "zstd" if zstd else ""
        addquotes_option = "addquotes" if addquotes else ""
        null_option = f"null as '{null}'" if null is not None else ""
        escape_option = "escape" if escape else ""
        allowoverwrite_option = "allowoverwrite" if allowoverwrite else ""
        parallel_option = f"parallel {parallel}"
        maxfilesize_option = (
            f"maxfilesize '{maxfilesize}'" if maxfilesize is not None else ""
        )
        region_option = f"region {region}" if region else ""
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
        {file_format_option}
        {partition_option}
        {manifest_option}
        {header_option}
        {delimiter_option}
        {fixedwidth_option}
        {encrypted_option}
        {bzip2_option}
        {gzip_option}
        {zstd_option}
        {addquotes_option}
        {null_option}
        {escape_option}
        {allowoverwrite_option}
        {parallel_option}
        {maxfilesize_option}
        {region_option}
        {access_key_id_option}
        {secret_access_key_option}
        {aws_token_option}
        {iam_role_option}
        """
        self.run_query(unload_template)
