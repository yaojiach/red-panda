import pytest
import os
import logging
import boto3
from subprocess import Popen, PIPE
from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger(__name__)

STACK_NAME = os.getenv("STACK_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


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


@pytest.fixture(scope="module", autouse=True)
def aws(pytestconfig):
    if pytestconfig.getoption("--skip-cdk"):
        LOGGER.info("Skipping CDK Setup")
        yield
        LOGGER.info("No teardown needed")
    else:
        # Setup
        cdk_run(["npm", "run", "build"], "Building cdk", "npm build failed.")
        cdk_run(
            ["cdk", "deploy", "--require-approval", "never"],
            "Setup CDK stack",
            "CDK stack failed to create.",
        )

        yield

        # Teardown
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
