# -*- coding: utf-8 -*-
# pylint: disable=no-value-for-parameter
import os
import json

import click

from red_panda.red_panda import S3Utils


def get_aws_config(filepath):
    with open(filepath, 'r') as f:
        config = json.load(f)
    return config.get('AWS_CONFIG')


def get_s3u(config):
    config = get_aws_config(config)
    s3u = S3Utils(config)
    return s3u


@click.group()
def cli():
    pass


@click.group()
def s3():
    pass


@click.command()
@click.option(
    '-c', 
    '--config',
    required=False,
    type=str,
    help='Config file path.'
)
@click.option(
    '-b', 
    '--bucket',
    required=True,
    type=str,
    help='S3 bucket name.'
)
@click.option(
    '-k', 
    '--key',
    required=True,
    type=str,
    help='S3 key.'
)
@click.option(
    '-s', 
    '--source',
    required=False,
    type=str,
    help='Destination location'
)
def upload(config, bucket, key, source):
    s3u = get_s3u(config)
    s3u.file_to_s3(source, bucket, key)


@click.command()
@click.option(
    '-c', 
    '--config',
    required=False,
    type=str,
    help='Config file path.'
)
@click.option(
    '-b', 
    '--bucket',
    required=True,
    type=str,
    help='S3 bucket name.'
)
@click.option(
    '-k', 
    '--key',
    required=True,
    type=str,
    help='S3 key.'
)
@click.option(
    '-d', 
    '--destination',
    required=False,
    type=str,
    help='Destination location path.'
)
def download(config, bucket, key, destination):
    s3u = get_s3u(config)
    s3u.s3_to_file(bucket, key, destination)


s3.add_command(upload)
s3.add_command(download)
cli.add_command(s3)


if __name__ == '__main__':
    cli()
