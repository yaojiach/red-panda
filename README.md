# Red Panda

![image](https://raw.githubusercontent.com/jucyai/red-panda/dev/red-panda.jpg)

[![docs](https://readthedocs.org/projects/red-panda/badge/?version=dev)](https://red-panda.readthedocs.io/en/dev/?badge=dev)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://github.com/jucyao/red-panda/blob/master/LICENSE)

Easily interact with cloud (AWS) in your Data Science workflow.

## Features

- DataFrame/files to and from S3 and Redshift.
- Run queries on Redshift in Python.
- Use built-in Redshift admin queries, such as checking running queries and errors.
- Use Redshift utility functions to easily accomplish common tasks such as creating a table.
- Manage files on S3.
- Query data on S3 directly with Athena.
- Pandas DataFrame utility functions.

## Installation

```sh
pip install red-panda
```

## Using red-panda

Import `red-panda` and create an instance of `RedPanda`. If you create the instance with `dryrun=True` (i.e. `rp = RedPanda(redshift_conf, s3_conf, dryrun=True)`), `red-panda` will print the planned queries instead of executing them.

```python
from red_panda import RedPanda

redshift_conf = {
    "user": "awesome-developer",
    "password": "strong-password",
    "host": "awesome-domain.us-east-1.redshift.amazonaws.com",
    "port": 5432,
    "dbname": "awesome-db",
}

aws_conf = {
    "aws_access_key_id": "your-aws-access-key-id",
    "aws_secret_access_key": "your-aws-secret-access-key",
    # "aws_session_token": "temporary-token-if-you-have-one",
}

rp = RedPanda(redshift_conf, aws_conf)
```

Load your Pandas DataFrame into Redshift as a new table.

```python
import pandas as pd

df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

s3_bucket = "s3-bucket-name"
s3_path = "parent-folder/child-folder" # optional, if you don't have any sub folders
s3_file_name = "test.csv" # optional, randomly generated if not provided
rp.df_to_redshift(df, "test_table", bucket=s3_bucket, path=s3_path, append=False)
```

It is also possible to:

- Upload a DataFrame or flat file to S3.
- Delete files from S3.
- Load S3 data into Redshift.
- Unload a Redshift query result to S3.
- Obtain a Redshift query result as a DataFrame.
- Run any query on Redshift.
- Download S3 file to local.
- Read S3 file in memory as DataFrame.
- Run built-in Redshift admin queries, such as getting running query information.
- Use utility functions such as `create_table` to quickly create tables in Redshift.
- Run queries against S3 data directly with Athena using `AthenaUtils`.
- Use features separately with `RedshiftUtils`, `S3Utils`, `AthenaUtils`.

```python
s3_key = s3_path + "/" + s3_file_name

# DataFrame uploaded to S3
rp.df_to_s3(df, s3_bucket, s3_key)

# Delete a file on S3
rp.delete_from_s3(s3_bucket, s3_key)

# Upload a local file to S3
pd.to_csv(df, "test_data.csv", index=False)
rp.file_to_s3("test_data.csv", s3_bucket, s3_key)

# Populate a Redshift table from S3 files
# Use a dictionary for column definition, here we minimally define only data_type
redshift_column_definition = {
    "col1": {data_type: "int"},
    "col2": {data_type: "int"},
}
rp.s3_to_redshift(
    s3_bucket, s3_key, "test_table", column_definition=redshift_column_definition
)

# Unload Redshift query result to S3
sql = "select * from test_table"
rp.redshift_to_s3(sql, s3_bucket, s3_path+"/unload", prefix="unloadtest_")

# Obtain Redshift query result as a DataFrame
df = rp.redshift_to_df("select * from test_table")

# Run queries on Redshift
rp.run_query("create table test_table_copy as select * from test_table")

# Download S3 file to local
rp.s3_to_file(s3_bucket, s3_key, "local_file_name.csv")

# Read S3 file in memory as DataFrame
df = rp.s3_to_df(s3_bucket, s3_key, delimiter=",") # csv file in this example

# Since we are only going to use Redshift functionalities, we can just use RedshiftUtils
from red_panda.red_panda import RedshiftUtils
ru = RedshiftUtils(redshift_conf)

# Run built-in Redshift admin queries, such as getting running query information
load_errors = ru.get_load_error(as_df=True)

# Use utility functions such as create_table to quickly create tables in Redshift
ru.create_table("test_table", redshift_column_definition, sortkey=["col2"], drop_first=True)
```

For full API documentation, visit <https://red-panda.readthedocs.io/en/latest/>.

## TODO

In no particular order:

- Support more data formats for copy. Currently only support delimited files.
- Support more data formats for s3 to df. Currently only support delimited files.
- Improve tests and docs.
- Better ways of inferring data types from dataframe to Redshift.
- Explore using `S3 Transfer Manager`'s `upload_fileobj` for `df_to_s3` to take advantage of automatic multipart upload.
- Add COPY from S3 manifest file, in addition to COPY from S3 source path.
- Support multi-cloud.
- Take advantage of Redshift slices for parallel processing. Split files for COPY.
