import warnings

import pytest
from dbt.tests.util import run_dbt
from urllib3.exceptions import InsecureRequestWarning


@pytest.mark.skip_profile("trino_starburst")
class TestInsecureWarnings:
    def test_table_properties(self, project):
        with warnings.catch_warnings(record=True) as w:
            dbt_args = ["show", "--inline", "select 1"]
            run_dbt(dbt_args)

            # Check if not any InsecureRequestWarning was raised
            assert not any(
                issubclass(warning.category, InsecureRequestWarning) for warning in w
            ), "InsecureRequestWarning was not raised"
