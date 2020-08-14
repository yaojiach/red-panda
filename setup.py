import io
import re

from collections import OrderedDict
from setuptools import setup, find_packages


with open("README.md") as f:
    readme = f.read()

with io.open("red_panda/__init__.py", "rt") as f:
    version = re.search(r"__version__ = \"(.*?)\"", f.read()).group(1)

setup(
    name="red-panda",
    version=version,
    url="https://github.com/jucyai/red-panda",
    project_urls=OrderedDict(
        (
            ("Code", "https://github.com/jucyai/red-panda"),
            ("Issue tracker", "https://github.com/jucyai/red-panda/issues"),
        )
    ),
    license="MIT",
    author="Jiachen Yao",
    maintainer="Jiachen Yao",
    description="Data science on the cloud",
    long_description_content_type="text/markdown",
    long_description=readme,
    packages=find_packages(exclude=["tests", "cdk"]),
    include_package_data=True,
    python_requires=">=3.6",
    install_requires=[
        "pandas>=1.1.0",
        "psycopg2-binary>=2.8.5",
        "boto3>=1.14.38",
        "awscli>=1.18.115",
        "PyAthena>=1.11.0"
    ],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
