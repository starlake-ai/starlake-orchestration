#!/usr/bin/env python

from distutils.core import setup

from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

import os

version = os.environ.get("PROJECT_VERSION", "0.5.6")

setup(name='starlake-orchestration',
      version=version,
      description='Starlake Python Distribution For orchestration',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Stéphane Manciot',
      author_email='stephane.manciot@gmail.com',
      license='Apache 2.0',
#      url='https://github.com/starlake-ai/starlake/tree/master/src/main/python/starlake-orchestration',
      packages=find_packages(include=['ai', 'ai.*']),
      install_requires=['croniter', 'python-dateutil', 'pytz'],
      extras_require={
        "airflow": ["starlake-airflow~=0.6"],
        "dagster": ["starlake-dagster~=0.5"],
        "snowflake": ["starlake-snowflake~=0.4"],
        "shell": [],
        "gcp": ["google-cloud-storage"],
        "aws": ["boto3"],
        "azure": [],
      },
#      python_requires='>=3.8',
)
