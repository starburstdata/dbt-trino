"""Load local test settings from test.env.

Sourcing the file through the shell mangles values with backslashes, carriage
returns or shell metacharacters - which service account passwords are full of -
so both pytest and the setup script read it here instead.
"""
import os
import pathlib

ENV_FILE = pathlib.Path(__file__).resolve().parent.parent / "test.env"


def load_test_env(env_file=ENV_FILE):
    """Set anything in test.env that is not already in the environment."""
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip().lstrip("﻿")
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        # Accept the shell forms people paste in: 'export NAME=value', quoted
        # values, and trailing carriage returns from a CRLF file.
        name = name.strip().removeprefix("export ").strip()
        if not name.isidentifier():
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(name, value)
