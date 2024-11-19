import warnings

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture
from urllib3.exceptions import InsecureRequestWarning


class TestRequireCertificateValidationDefault:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {}}

    def test_cert_default_value(self, project):
        assert project.adapter.connections.profile.credentials.cert is None

    def test_require_certificate_validation_logs(self, project):
        dbt_args = ["show", "--inline", "select 1"]
        _, logs = run_dbt_and_capture(dbt_args)
        assert "It is strongly advised to enable `require_certificate_validation` flag" in logs

    @pytest.mark.skip_profile("trino_starburst")
    def test_require_certificate_validation_insecure_request_warning(self, project):
        with warnings.catch_warnings(record=True) as w:
            dbt_args = ["show", "--inline", "select 1"]
            run_dbt(dbt_args)

            # Check if any InsecureRequestWarning was raised
            assert any(
                issubclass(warning.category, InsecureRequestWarning) for warning in w
            ), "InsecureRequestWarning was not raised"


class TestRequireCertificateValidationFalse:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"require_certificate_validation": False}}

    def test_cert_default_value(self, project):
        assert project.adapter.connections.profile.credentials.cert is None

    def test_require_certificate_validation_logs(self, project):
        dbt_args = ["show", "--inline", "select 1"]
        _, logs = run_dbt_and_capture(dbt_args)
        assert "It is strongly advised to enable `require_certificate_validation` flag" in logs

    @pytest.mark.skip_profile("trino_starburst")
    def test_require_certificate_validation_insecure_request_warning(self, project):
        with warnings.catch_warnings(record=True) as w:
            dbt_args = ["show", "--inline", "select 1"]
            run_dbt(dbt_args)

            # Check if any InsecureRequestWarning was raised
            assert any(
                issubclass(warning.category, InsecureRequestWarning) for warning in w
            ), "InsecureRequestWarning was not raised"


class TestRequireCertificateValidationTrue:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"require_certificate_validation": True}}

    def test_cert_default_value(self, project):
        assert project.adapter.connections.profile.credentials.cert is True

    def test_require_certificate_validation_logs(self, project):
        dbt_args = ["show", "--inline", "select 1"]
        _, logs = run_dbt_and_capture(dbt_args)
        assert "It is strongly advised to enable `require_certificate_validation` flag" not in logs

    @pytest.mark.skip_profile("trino_starburst")
    def test_require_certificate_validation_insecure_request_warning(self, project):
        with warnings.catch_warnings(record=True) as w:
            dbt_args = ["show", "--inline", "select 1"]
            run_dbt(dbt_args)

            # Check if not any InsecureRequestWarning was raised
            assert not any(
                issubclass(warning.category, InsecureRequestWarning) for warning in w
            ), "InsecureRequestWarning was not raised"
