import logging
import pandas as pd
import psycopg2

from red_panda.pandas import PANDAS_TOCSV_KWARGS
from red_panda.aws.templates.redshift import (
    SQL_NUM_SLICES,
    SQL_TABLE_INFO,
    SQL_TABLE_INFO_SIMPLIFIED,
    SQL_LOAD_ERRORS,
    SQL_RUNNING_INFO,
    SQL_LOCK_INFO,
    SQL_TRANSACT_INFO,
)
from red_panda.utils import filter_kwargs, prettify_sql


LOGGER = logging.getLogger(__name__)


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
    """
    data_type = d.get("data_type")
    data_type_option = data_type if data_type is not None else "varchar(256)"
    default = d.get("default")
    quote = "'" if not isinstance(default, (int, float, complex)) else ""
    default_option = f"default {quote}{default}{quote}" if default is not None else ""
    identity = d.get("identity")
    if identity is not None:
        seed, step = identity
        identity_option = f"identity({seed}, {step})"
    else:
        identity_option = ""
    encode = d.get("encode")
    encode_option = f"encode {encode}" if encode is not None else ""
    distkey = d.get("distkey")
    distkey_option = "distkey" if distkey is not None and distkey else ""
    sortkey = d.get("sortkey")
    sortkey_option = "sortkey" if sortkey is not None and sortkey else ""
    nullable = d.get("nullable")
    nullable_option = "not null" if nullable is not None and not nullable else ""
    unique = d.get("unique")
    unique_option = "unique" if unique is not None and unique else ""
    primary_key = d.get("primary_key")
    primary_key_option = (
        "primary key" if primary_key is not None and primary_key else ""
    )
    references = d.get("references")
    references_option = f"references {references}" if references is not None else ""
    like = d.get("like")
    like_option = f"like {like}" if like is not None else ""
    template = " ".join(
        [
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
            like_option,
        ]
    )
    return " ".join(template.split())


def create_column_definition(d):
    return ",\n".join(f"{c} {create_column_definition_single(o)}" for c, o in d.items())


class RedshiftUtils:
    """ Base class for Redshift operations
    """

    def __init__(self, redshift_config, dryrun=False):
        self.redshift_config = redshift_config
        self._dryrun = dryrun

    def _connect_redshift(self):
        connection = psycopg2.connect(
            user=self.redshift_config.get("user"),
            password=self.redshift_config.get("password"),
            host=self.redshift_config.get("host"),
            port=self.redshift_config.get("port"),
            dbname=self.redshift_config.get("dbname"),
        )
        return connection

    def run_sql_from_file(self, fpath):
        """Run a .sql file

        # TODO:
            - Add support for transactions
        """
        with open(fpath, "r") as f:
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
        LOGGER.info(prettify_sql(sql))
        if self._dryrun:
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
                    LOGGER.warning("Query completed but it returned no data.")
            else:
                conn.commit()
        except KeyboardInterrupt:
            conn.cancel()
            LOGGER.warning("User canceled query.")
        finally:
            conn.close()
        return (data, columns)

    def cancel_query(self, pid, transaction=False):
        self.run_query(f"cancel {pid}")
        if transaction:
            self.run_query("abort")

    def kill_session(self, pid):
        self.run_query(f"select pg_terminate_backend({pid})")

    def get_num_slices(self):
        """Get number of slices of a Redshift cluster"""
        data, _ = self.run_query(SQL_NUM_SLICES, fetch=True)
        n_slices = None
        try:
            n_slices = data[0][0]
        except IndexError:
            LOGGER.error("Could not derive number of slices of Redshift cluster.")
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
        to_csv_kwargs = filter_kwargs(kwargs, PANDAS_TOCSV_KWARGS)
        data.to_csv(filename, **to_csv_kwargs)

    def create_table(
        self,
        table_name,
        column_definition,
        temp=False,
        if_not_exists=False,
        backup="YES",
        unique=None,
        primary_key=None,
        foreign_key=None,
        references=None,
        diststyle="EVEN",
        distkey=None,
        sortstyle="COMPOUND",
        sortkey=None,
        drop_first=False,
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

        # TODO:
            - Check consistency between column_constraints and table_constraints
        """
        if drop_first:
            self.run_query(f"drop table if exists {table_name}")
        temp_option = "temp" if temp else ""
        exist_option = "if not exists" if if_not_exists else ""
        unique_option = f'unique ({", ".join(unique)})' if unique is not None else ""
        primary_key_option = (
            f"primary key ({primary_key})" if primary_key is not None else ""
        )
        foreign_key_option = (
            f'foreign key ({", ".join(foreign_key)})' if foreign_key is not None else ""
        )
        references_option = (
            f'references ({", ".join(references)})' if references is not None else ""
        )
        distkey_option = f"distkey({distkey})" if distkey is not None else ""
        sortkey_option = (
            f'{sortstyle} sortkey({", ".join(sortkey)})' if sortkey is not None else ""
        )
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
