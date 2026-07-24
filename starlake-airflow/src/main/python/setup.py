#!/usr/bin/env python

from distutils.core import setup

from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

import os

version = os.environ.get("PROJECT_VERSION", "0.6.11")

setup(name='starlake-airflow',
      version=version,
      description='Starlake Python Distribution For Airflow',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Stéphane Manciot',
      author_email='stephane.manciot@gmail.com',
      license='Apache 2.0',
#      url='https://github.com/starlake-ai/starlake/tree/master/src/main/python/starlake-airflow',
      packages=find_packages(include=['ai', 'ai.*']),
      install_requires=['starlake-orchestration~=0.5'],
      extras_require={
        # Floor of the full data-aware round-trip: datasets landed in 2.4,
        # triggering_dataset_events (consumer side) in 2.5. The producer side
        # is version-portable via install_dataset_extra_forwarding (issue #125);
        # advanced opt-in features (deferrable cloud pre-load, dataset aliases)
        # still require 2.10+ and are out of scope of this floor.
        "airflow": ["airflow>=2.5.0"],
        "shell": [],
        "gcp": [], #["apache-airflow-providers-google>=10.0.7"]
        "aws": [],
        "azure": [],
      },
      python_requires='>=3.8'
)
