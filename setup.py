#!/usr/bin/env python
from setuptools import find_namespace_packages, setup
import os
import re

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


package_name = "dbt-trino"


# get this package's version from dbt/adapters/<name>/__version__.py
def _get_plugin_version_dict():
    _version_path = os.path.join(this_directory, "dbt", "adapters", "trino", "__version__.py")
    _semver = r"""(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"""
    _pre = r"""((?P<prekind>a|b|rc)(?P<pre>\d+))?"""
    _version_pattern = rf"""version\s*=\s*["']{_semver}{_pre}["']"""
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f"invalid version at {_version_path}")
        return match.groupdict()


def _dbt_trino_version():
    parts = _get_plugin_version_dict()
    trino_version = "{major}.{minor}.{patch}".format(**parts)
    if parts["prekind"] and parts["pre"]:
        trino_version += parts["prekind"] + parts["pre"]
    return trino_version


# require a compatible minor version (~=), prerelease if this is a prerelease
def _get_dbt_core_version():
    parts = _get_plugin_version_dict()
    minor = "{major}.{minor}.0".format(**parts)
    pre = parts["prekind"] + "1" if parts["prekind"] else ""
    return f"{minor}{pre}"


package_version = _dbt_trino_version()
description = """The trino adapter plugin for dbt (data build tool)"""
dbt_version = _get_dbt_core_version()

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
            "include/trino/macros/*/*/*.sql",
        ]
    },
    install_requires=[
        "dbt-core~={}".format(dbt_version),
        "trino~=0.313.0",
    ],
)
