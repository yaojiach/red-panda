import pytest
import os
import logging
import boto3
import pandas as pd
from subprocess import Popen, PIPE
from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger(__name__)

STACK_NAME = os.getenv("STACK_NAME")
BASE_BUCKET_ID = os.getenv("BASE_BUCKET_ID")
GLUE_BUCKET_ID = os.getenv("GLUE_BUCKET_ID")
ATHENA_BUCKET_ID = os.getenv("ATHENA_BUCKET_ID")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
DEFAULT_REGION = os.getenv("DEFAULT_REGION")

GLUE_DATA = pd.DataFrame({"col0": ["a", "b"], "col1": ["x", "y"]})


def cdk_run(args, info, msg):
    LOGGER.info(info)
    p = Popen(args, cwd="cdk", stdin=PIPE, stdout=PIPE, stderr=PIPE,)
    out, err = p.communicate()
    LOGGER.info(out)
    if err != b"":
        LOGGER.error(err)
        raise RuntimeError(msg)


def pytest_addoption(parser):
    parser.addoption(
        "--skip-cdk",
        action="store_true",
        help="Skip creating AWS stack. Default to skip.",
    )


def prepare_glue_date():
    from io import StringIO

    buffer = StringIO()
    GLUE_DATA.to_csv(buffer, header=False)
    boto3.resource("s3").Object(get_s3_bucket(GLUE_BUCKET_ID), "sample.csv").put(
        Body=buffer.getvalue()
    )


@pytest.fixture(scope="module")
def glue_data():
    return GLUE_DATA


@pytest.fixture(scope="module", autouse=True)
def aws(pytestconfig):
    if pytestconfig.getoption("--skip-cdk"):
        LOGGER.info("Skipping CDK Setup")
        prepare_glue_date()
        LOGGER.info("Prepared Glue data")

        yield

        LOGGER.info("Empty all buckets")
        empty_s3_buckets()
        LOGGER.info("No teardown needed")
    else:
        # Setup
        LOGGER.info("Create CDK stack")
        cdk_run(["npm", "run", "build"], "Building cdk", "npm build failed.")
        cdk_run(
            ["cdk", "deploy", "--require-approval", "never"],
            "Setup CDK stack",
            "CDK stack failed to create.",
        )
        prepare_glue_date()
        LOGGER.info("Prepared Glue data")

        yield

        # Teardown
        LOGGER.info("Empty all buckets")
        empty_s3_buckets()
        LOGGER.info("Destroy stack")
        cdk_run(
            ["cdk", "destroy", "--force"],
            "Teardown CDK stack",
            "CDK stack failed to delete.",
        )


@pytest.fixture(scope="module")
def aws_config():
    return {
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    }


def empty_s3_buckets():
    boto3.resource("s3").Bucket(get_s3_bucket(BASE_BUCKET_ID)).objects.all().delete()
    boto3.resource("s3").Bucket(get_s3_bucket(GLUE_BUCKET_ID)).objects.all().delete()
    boto3.resource("s3").Bucket(get_s3_bucket(ATHENA_BUCKET_ID)).objects.all().delete()


def get_s3_bucket(id):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    buckets = [
        b["Name"]
        for b in s3_client.list_buckets()["Buckets"]
        if b["Name"].startswith(f"{STACK_NAME}-{id}")
    ]
    bucket = buckets[0] if len(buckets) > 0 else ""
    return bucket


@pytest.fixture(scope="module")
def aws_region():
    return DEFAULT_REGION


@pytest.fixture(scope="module")
def s3_bucket():
    return get_s3_bucket(BASE_BUCKET_ID)


@pytest.fixture(scope="module")
def glue_bucket():
    return get_s3_bucket(GLUE_BUCKET_ID)


@pytest.fixture(scope="module")
def athena_result_location():
    return f"s3://{get_s3_bucket(ATHENA_BUCKET_ID)}/result"


@pytest.fixture(scope="module")
def iam_role_arn():
    iam_client = boto3.client(
        "iam",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    roles = [
        b["Arn"]
        for b in iam_client.list_roles()["Roles"]
        if b["RoleName"].lower().startswith(STACK_NAME)
    ]
    return roles[0] if len(roles) > 0 else ""


@pytest.fixture(scope="module")
def redshift_config():
    redshift_client = boto3.client(
        "redshift",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    clusters = [
        c["Endpoint"]["Address"]
        for c in redshift_client.describe_clusters()["Clusters"]
        if c["ClusterIdentifier"].startswith(STACK_NAME)
    ]
    cluster = clusters[0] if len(clusters) > 0 else ""
    return {
        "user": os.getenv("REDSHIFT_USERNAME"),
        "password": os.getenv("REDSHIFT_PASSWORD"),
        "host": cluster,
        "port": os.getenv("REDSHIFT_PORT"),
        "dbname": os.getenv("REDSHIFT_DB"),
    }
