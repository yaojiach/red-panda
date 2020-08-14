import pytest
import os
import logging
import boto3
from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger(__name__)

STACK_NAME = os.getenv("STACK_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def pytest_addoption(parser):
    parser.addoption(
        "--skip-cdk",
        action="store_true",
        help="Skip creating AWS stack. Default to skip.",
    )


@pytest.fixture(scope="module", autouse=True)
def aws(pytestconfig):
    if pytestconfig.getoption("--skip-cdk"):
        LOGGER.info("Skipping CDK Setup")
        yield
    else:
        from subprocess import Popen, PIPE

        LOGGER.info("Setup CDK stack")
        p = Popen(["cdk", "ls"], cwd="cdk", stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        LOGGER.info(out)
        if err != b"":
            LOGGER.error(err)
            raise RuntimeError("CDK stack failed to create.")
        yield
        LOGGER.info("Teardown CDK stack")
        empty_s3_bucket()
        p = Popen(["cdk", "ls"], cwd="cdk", stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        LOGGER.info(out)
        if err != b"":
            LOGGER.error(err)
            raise RuntimeError("CDK stack failed to delete.")


@pytest.fixture(scope="module")
def aws_config():
    return {
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    }


def empty_s3_bucket():
    boto3.resource("s3").Bucket(get_s3_bucket()).objects.all().delete()


def get_s3_bucket():
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    buckets = [
        b["Name"]
        for b in s3_client.list_buckets()["Buckets"]
        if b["Name"].startswith(STACK_NAME)
    ]
    bucket = buckets[0] if len(buckets) > 0 else ""
    return bucket


@pytest.fixture(scope="module")
def s3_bucket():
    return get_s3_bucket()


@pytest.fixture(scope="module")
def redshift_config():
    redshift_client = boto3.client(
        "redshift",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    clusters = [
        c["Address"]
        for c in redshift_client.describe_clusters()["Clusters"]
        if c["ClusterIdentifier"].startswith(STACK_NAME)
    ]
    cluster = clusters[0] if len(clusters) > 0 else ""
    return {
        "user": os.environ("REDSHIFT_USERNAME"),
        "password": os.environ("REDSHIFT_PASSWORD"),
        "host": cluster,
        "port": os.environ("REDSHIFT_PORT"),
        "dbname": os.environ("REDSHIFT_DB"),
    }
