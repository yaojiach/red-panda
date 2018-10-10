# -*- coding: utf-8 -*-
import io
import re

from collections import OrderedDict
from setuptools import setup, find_packages


with io.open('README.rst', 'rt', encoding='utf8') as f:
    readme = f.read()

with io.open('red_panda/__init__.py', 'rt', encoding='utf8') as f:
    version = re.search(r'__version__ = \'(.*?)\'', f.read()).group(1)

setup(
    name='red-panda',
    version=version,
    url='https://github.com/yaojiach/red-panda',
    project_urls=OrderedDict((
        ('Code', 'https://github.com/yaojiach/red-panda'),
        ('Issue tracker', 'https://github.com/yaojiach/red-panda/issues'),
    )),
    license='MIT',
    author='Jiachen Yao',
    maintainer='Jiachen Yao',
    description='Data science on the cloud',
    long_description=readme,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    python_requires='>=3.6',
    install_requires=[
        'pandas',
        'psycopg2-binary',
        'boto3',
        'awscli',
        'oss2',
        'click',
        'python-dotenv',
    ],
    extras_require={
        'dev': [
            'pytest',
            'tox',
        ],
    },
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
    ],
    entry_points='''
        [console_scripts]
        redcli=red_panda.scripts.redcli:cli
    ''',
)
