#!/usr/bin/env python
from setuptools import find_namespace_packages, setup
import os
import re

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


package_name = "dbt-trino"


# get this from a separate file
def _dbt_trino_version():
    _version_path = os.path.join(
        this_directory, "dbt", "adapters", "trino", "__version__.py"
    )
    _version_pattern = r"""version\s*=\s*["'](.+)["']"""
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f"invalid version at {_version_path}")
        return match.group(1)


package_version = _dbt_trino_version()
description = """The trino adapter plugin for dbt (data build tool)"""

dbt_version = "1.0.0"
# the package version should be the dbt version, with maybe some things on the
# ends of it. (0.19.1 vs 0.19.1a1, 0.19.1.1, ...)
if not package_version.startswith(dbt_version):
    raise ValueError(
        f"Invalid setup.py: package_version={package_version} must start with "
        f"dbt_version={dbt_version}"
    )

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    platforms="any",
    license="Apache License 2.0",
    license_files=("LICENSE.txt",),
    author="Starburst Data",
    author_email="info@starburstdata.com",
    url="https://github.com/starburstdata/dbt-trino",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    package_data={
        "dbt": [
            "include/trino/dbt_project.yml",
            "include/trino/sample_profiles.yml",
            "include/trino/macros/*.sql",
            "include/trino/macros/*/*.sql",
        ]
    },
    install_requires=[
        "dbt-core=={}".format(dbt_version),
        "trino==0.309.0",
    ],
)
