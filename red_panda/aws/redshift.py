import logging
from typing import Union, Optional, List

import pandas as pd
import psycopg2

from red_panda.typing import QueryResult, TemplateQueryResult
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


def create_column_definition_single(d: dict) -> str:
    """Create the column definition for a single column.

    Args:
        d: A `dict` of values to compose a single column definition, defaults::

                {
                    "data_type": "varchar(256)", # str
                    "default": None, # Any
                    "identity": None, # tuple
                    "encode": None, # str
                    "distkey": False,
                    "sortkey": False,
                    "nullable": True,
                    "unique": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "references": None, # str
                    "like": None, # str
                }
    
    Returns:
        str: Single column definition for Redshift.
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


def create_column_definition(d: dict) -> str:
    """Create full column definition string for Redshift.

    Args:
        d: A `dict` if single column definitions, where the keys are column names.

    Returns:
        str: Full column definition for Redshift.
    """
    return ",\n".join(f"{c} {create_column_definition_single(o)}" for c, o in d.items())


class RedshiftUtils:
    """ Base class for Redshift operations.

    Args:
        redshift_conf: Redshift configuration.
        dryrun (optional): If True, queries will be printed instead of executed.
    
    Attributes:
        redshift_conf (dict): Redshift configuration.
    """

    def __init__(self, redshift_config: dict, dryrun: bool = False):
        self.redshift_config = redshift_config
        self._dryrun = dryrun

    def _connect_redshift(self):
        return psycopg2.connect(
            user=self.redshift_config.get("user"),
            password=self.redshift_config.get("password"),
            host=self.redshift_config.get("host"),
            port=self.redshift_config.get("port"),
            dbname=self.redshift_config.get("dbname"),
        )

    def run_sql_from_file(self, file_name: str):
        """Run a `.sql` file.

        Args:
            file_name: SQL file to be run.

        TODO:
            * Add support for transactions.
        """
        with open(file_name, "r") as f:
            sql = f.read()
        self.run_query(sql)

    def run_query(self, sql: str, fetch: bool = False) -> QueryResult:
        """Run a SQL query.

        Args:
            sql: SQL string.
            fetch (optional): Whether to return data from the query.

        Returns:
            Tuple[dict, list]: (data, columns) where data is a json/dict representation of the data 
            and columns is a list of column names.
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

    def cancel_query(self, pid: Union[str, int], transaction: bool = False):
        """Cancels a running query given pid.

        Args:
            pid: PID of a running query in Redshift.
            transaction (optional): Whether the running query is a transaction.
        """
        self.run_query(f"cancel {pid}")
        if transaction:
            self.run_query("abort")

    def kill_session(self, pid: Union[str, int]):
        """ Kill a session given pid.

        Args:
            pid: PID of a running query in Redshift.
        """
        self.run_query(f"select pg_terminate_backend({pid})")

    def get_num_slices(self) -> Optional[int]:
        """Get number of slices of a Redshift cluster.
        
        Returns:
            int: Number of slices of the connected cluster.

        Raises:
            IndexError: When Redshift returns invalid number of slices.
        """
        data, _ = self.run_query(SQL_NUM_SLICES, fetch=True)
        n_slices = None
        try:
            n_slices = data[0][0]
        except IndexError:
            LOGGER.error("Could not derive number of slices of Redshift cluster.")
        return n_slices

    def run_template(self, sql: str, as_df: bool = True) -> TemplateQueryResult:
        """Utility method to run a pre-defined sql template.

        Args:
            sql: SQL string.
            as_df (optional): Whether or not to return the result as a `pandas.DataFrame`.

        Returns:
            Union[pandas.DataFrame, Tuple[dict, list]]: Either return a DataFrame/table of the 
            template query result or the raw form as specified in `run_query`.
        """
        data, columns = self.run_query(sql, fetch=True)
        if as_df:
            return pd.DataFrame(data, columns=columns)
        else:
            return (data, columns)

    def get_table_info(
        self, as_df: bool = True, simple: bool = False
    ) -> TemplateQueryResult:
        """Utility to get table information in the cluster.

        Args:
            as_df (optional): Whether or not to return the result as a `pandas.DataFrame`.
            simple (optional): Whether to get the basic table information only.

        Returns:
            Table information.
        """
        sql = SQL_TABLE_INFO_SIMPLIFIED if simple else SQL_TABLE_INFO
        return self.run_template(sql, as_df)

    def get_load_error(self, as_df: bool = True) -> TemplateQueryResult:
        """Utility to get load errors in the cluster.

        Args:
            as_df (optional): Whether or not to return the result as a `pandas.DataFrame`.

        Returns:
            Load error information.
        """
        return self.run_template(SQL_LOAD_ERRORS, as_df)

    def get_running_info(self, as_df: bool = True) -> TemplateQueryResult:
        """Utility to get information on running queries in the cluster.

        Args:
            as_df (optional): Whether or not to return the result as a `pandas.DataFrame`.

        Returns:
            Running query information.
        """
        return self.run_template(SQL_RUNNING_INFO, as_df)

    def get_lock_info(self, as_df: bool = True) -> TemplateQueryResult:
        """Utility to get lock information in the cluster.

        Args:
            as_df (optional): Whether or not to return the result as a `pandas.DataFrame`.

        Returns:
            Lock information.
        """
        return self.run_template(SQL_LOCK_INFO, as_df)

    def get_transaction_info(self, as_df: bool = True) -> TemplateQueryResult:
        """Utility to get transaction information in the cluster.

        Args:
            as_df (optional): Whether or not to return the result as a `pandas.DataFrame`.

        Returns:
            Transaction information.
        """
        return self.run_template(SQL_TRANSACT_INFO, as_df)

    def redshift_to_df(self, sql: str) -> pd.DataFrame:
        """Redshift query result to a Pandas DataFrame.

        Args:
            sql: SQL query.

        Returns:
            pandas.DataFrame: A DataFrame of query result.
        """
        data, columns = self.run_query(sql, fetch=True)
        return pd.DataFrame(data, columns=columns)

    def redshift_to_file(self, sql: str, file_name: str, **kwargs):
        """Redshift query result to a file.

        Args:
            sql: SQL query.
            file_name: File name of the saved file.
            **kwargs: `to_csv` keyword arguments.
        """
        data = self.redshift_to_df(sql)
        to_csv_kwargs = filter_kwargs(kwargs, PANDAS_TOCSV_KWARGS)
        data.to_csv(file_name, **to_csv_kwargs)

    def create_table(
        self,
        table_name: str,
        column_definition: dict,
        temp: bool = False,
        if_not_exists: bool = False,
        backup: str = "YES",
        unique: List[str] = None,
        primary_key: str = None,
        foreign_key: List[str] = None,
        references: List[str] = None,
        diststyle: str = "EVEN",
        distkey: str = None,
        sortstyle: str = "COMPOUND",
        sortkey: List[str] = None,
        drop_first: bool = False,
    ):
        """Utility for creating a table in Redshift.

        Args:
            table_name: Name of table to be created.
            column_definition: default::

                {
                    "col1": {
                        "data_type": "varchar(256)", # str
                        "default": None, # Any
                        "identity": None, # tuple(int, int)
                        "encode": None, # str
                        "distkey": False,
                        "sortkey": False,
                        "nullable": True,
                        "unique": False,
                        "primary_key": False,
                        "foreign_key": False,
                        "references": None, # str
                        "like": None, # str
                    },
                    "col2": {
                        ...
                    },
                    ...
                }
            temp (optional): Corresponding argument of `create table` in Redshift.  
            if_not_exists (optional): Corresponding argument of `create table` in Redshift.  
            backup (optional): Corresponding argument of `create table` in Redshift.  
            unique (optional): Corresponding argument of `create table` in Redshift.  
            primary_key (optional): Corresponding argument of `create table` in Redshift.  
            foreign_key (optional): Corresponding argument of `create table` in Redshift.  
                Must match references.
            references (optional): Corresponding argument of `create table` in Redshift.  
                Must match foreign_key.
            diststyle (optional): Corresponding argument of `create table` in Redshift.  
            distkey (optional): Corresponding argument of `create table` in Redshift.  
            sortstyle (optional): Corresponding argument of `create table` in Redshift.  
            sortkey (optional): Corresponding argument of `create table` in Redshift.  
            drop_first (optional): Corresponding argument of `create table` in Redshift.  

        TODO:
            * Check consistency between column_constraints and table_constraints
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
