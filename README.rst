Red Panda 🐼😊
=============

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


Examples
--------


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
