# -*- coding: utf-8 -*-
"""Tests

# ``.env`` template

```sh
REDSHIFT_USER=
REDSHIFT_PASSWORD=
REDSHIFT_HOST=
REDSHIFT_PORT=
REDSHIFT_DBNAME=
REDSHIFT_TEST_SCHEMA=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
S3_TEST_BUCKET=
s3_TEST_FOLDER=
```
"""
import os

import pytest
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

redshift_conf = {
    'user': os.getenv('REDSHIFT_USER'),
    'password': os.getenv('REDSHIFT_PASSWORD'),
    'host': os.getenv('REDSHIFT_PORT'),
    'port': int(os.getenv('REDSHIFT_PORT')),
    'dbname': os.getenv('REDSHIFT_DBNAME'),
}

s3_conf = {
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
}

S3_TEST_BUCKET = os.getenv('S3_TEST_BUCKET')

@pytest.fixture
def rp():
    from red_panda import RedPanda
    return RedPanda(redshift_conf, s3_conf)

def test_redshift_config():
    assert redshift_conf['port'] == 8192

def test_s3_test_bucket_exists(rp):
    assert rp._check_s3_bucket_existence(S3_TEST_BUCKET)