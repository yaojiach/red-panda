Red Panda 🐼😊
================

Data science on AWS without frustration.

Features
--------

- DataFrame/files to and from S3 and Redshift.
- Run queries on Redshift in Python.
- Manage files on S3.


Installation
------------

    .. code-block:: console
       
        $ pip install red-panda


Using red-panda
---------------

Import `red-panda` and create an instance of `RedPanda`. If you create the instance with `debug` on (i.e. `rp = RedPanda(redshift_conf, s3_conf, debug=True)`), `red-panda` will print the planned queries instead of executing them.

    .. code-block:: python

        from red_panda import RedPanda

        redshift_conf = {
            'user': 'awesome-developer',
            'password': 'strong-password',
            'host': 'awesome-domain.us-east-1.redshift.amazonaws.com',
            'port': 5432,
            'dbname': 'awesome-db',
        }

        s3_conf = {
            'aws_access_key_id': 'your-aws-access-key-id',
            'aws_secret_access_key': 'your-aws-secret-access-key',
            # 'aws_session_token': 'temporary-token-if-you-have-one',
        }

        rp = RedPanda(redshift_conf, s3_conf)


Load your Pandas DataFrame into Redshift as a new table.

    .. code-block:: python

        import pandas as pd

        df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})

        your_bucket = 's3-bucket-name'
        your_path = 'parent-folder/child-folder' # optional
        file_name = 'test.csv' # optional
        rp.df_to_redshift(df, 'test_table', bucket=your_bucket, path=your_path, append=False)


TODO
----

In no particular order:

- Improve tests and docs.
- Handle when user does have implicit column that is the index in a DataFrame. Currently index is automatically dropped.
- Better ways of inferring data types from dataframe to Redshift.
- Take advantage of Redshift slices for parallel processing. Split files for COPY.
- Explore using `S3 Transfer Manager`'s upload_fileobj for `df_to_s3` to take advantage of automatic multipart upload.
- More options for data consistency management. Currently deleting file from S3 after COPY.
- Add encryption options for files uploaded to S3.
- Add COPY from S3 manifest file, in addition to COPY from S3 source path.
- Support more data formats.
- Build cli to manage data outside of Python.
