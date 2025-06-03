import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
    models__delete_insert_incremental_predicates_sql,
    seeds__expected_delete_insert_incremental_predicates_csv,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
    models__duplicated_unary_unique_key_list_sql,
    models__empty_str_unique_key_sql,
    models__empty_unique_key_list_sql,
    models__no_unique_key_sql,
    models__nontyped_trinary_unique_key_list_sql,
    models__not_found_unique_key_list_sql,
    models__not_found_unique_key_sql,
    models__str_unique_key_sql,
    models__trinary_unique_key_list_sql,
    models__unary_unique_key_list_sql,
    seeds__seed_csv,
)
from dbt.tests.util import run_dbt_and_capture

seeds__duplicate_insert_sql = """
-- Insert statement which when applied to seed.csv triggers the inplace
--   overwrite strategy of incremental models. Seed and incremental model
--   diverge.

-- insert new row, which should not be in incremental model
--  with primary or first three columns unique
insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('CT','Hartford','Hartford',DATE '2022-02-14');

"""

seeds__add_new_rows_sql = """
-- Insert statement which when applied to seed.csv sees incremental model
--   grow in size while not (necessarily) diverging from the seed itself.

-- insert two new rows, both of which should be in incremental model
--   with any unique columns
insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('WA','King','Seattle',DATE '2022-02-01');

insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('CA','Los Angeles','Los Angeles',DATE '2022-02-01');

"""

models__expected__one_str__overwrite_sql = """
{{
    config(
        materialized='table'
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    cast('2022-02-14' as date) as last_visit_date
union all
select 'MA','Suffolk','Boston',DATE '2020-02-12'
union all
select 'NJ','Mercer','Trenton',DATE '2022-01-01'
union all
select 'NY','Kings','Brooklyn',DATE '2021-04-02'
union all
select 'NY','New York','Manhattan',DATE '2021-04-01'
union all
select 'PA','Philadelphia','Philadelphia',DATE '2021-05-21'
union all
select 'CO','Denver',null,DATE '2021-06-18'

"""

models__expected__unique_key_list__inplace_overwrite_sql = """
{{
    config(
        materialized='table'
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    cast('2022-02-14' as date) as last_visit_date
union all
select 'MA','Suffolk','Boston',DATE '2020-02-12'
union all
select 'NJ','Mercer','Trenton',DATE '2022-01-01'
union all
select 'NY','Kings','Brooklyn',DATE '2021-04-02'
union all
select 'NY','New York','Manhattan',DATE '2021-04-01'
union all
select 'PA','Philadelphia','Philadelphia',DATE '2021-05-21'
union all
select 'CO','Denver',null,DATE '2021-06-18'

"""

models__location_specified = """
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['state', 'county', 'city'],
        properties= {
            "location": "'s3a://datalake/model'"
        }
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    cast('2022-02-14' as date) as last_visit_date
union all
select 'MA','Suffolk','Boston',DATE '2020-02-12'
union all
select 'NJ','Mercer','Trenton',DATE '2022-01-01'
union all
select 'NY','Kings','Brooklyn',DATE '2021-04-02'
union all
select 'NY','New York','Manhattan',DATE '2021-04-01'
union all
select 'PA','Philadelphia','Philadelphia',DATE '2021-05-21'

"""

models__delete_insert_composite_keys_sql = """
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['id', 'col']
    )
}}
select 1 as id, 1 as col
union all
select 1 as id, 3 as col
union all
select 3 as id, 1 as col
union all
select 3 as id, 3 as col

{% if is_incremental() %}

except
(select 1 as id, 1 as col
union all
select 3 as id, 3 as col)

{% endif %}
"""

seeds__expected_delete_insert_composite_keys_csv = """id,col
1,1
1,3
3,1
3,3
"""


class TrinoIncrementalUniqueKey(BaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "duplicate_insert.sql": seeds__duplicate_insert_sql,
            "seed.csv": seeds__seed_csv,
            "add_new_rows.sql": seeds__add_new_rows_sql,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "trinary_unique_key_list.sql": models__trinary_unique_key_list_sql,
            "nontyped_trinary_unique_key_list.sql": models__nontyped_trinary_unique_key_list_sql,
            "unary_unique_key_list.sql": models__unary_unique_key_list_sql,
            "not_found_unique_key.sql": models__not_found_unique_key_sql,
            "empty_unique_key_list.sql": models__empty_unique_key_list_sql,
            "no_unique_key.sql": models__no_unique_key_sql,
            "empty_str_unique_key.sql": models__empty_str_unique_key_sql,
            "str_unique_key.sql": models__str_unique_key_sql,
            "duplicated_unary_unique_key_list.sql": models__duplicated_unary_unique_key_list_sql,
            "not_found_unique_key_list.sql": models__not_found_unique_key_list_sql,
            "expected": {
                "one_str__overwrite.sql": models__expected__one_str__overwrite_sql,
                "unique_key_list__inplace_overwrite.sql": models__expected__unique_key_list__inplace_overwrite_sql,
            },
        }


@pytest.mark.iceberg
class TestIcebergIncrementalDeleteInsert(TrinoIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental",
            "models": {"+incremental_strategy": "delete+insert"},
            "seeds": {"incremental": {"seed": {"+column_types": {"some_date": "date"}}}},
        }


@pytest.mark.delta
class TestDeltaIncrementalDeleteInsert(TrinoIncrementalUniqueKey):
    def test__no_unique_keys(self, project):
        super().test__no_unique_keys(project)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental",
            "models": {"+on_table_exists": "drop", "+incremental_strategy": "delete+insert"},
            "seeds": {"incremental": {"seed": {"+column_types": {"some_date": "date"}}}},
        }


@pytest.mark.iceberg
@pytest.mark.skip_profile("starburst_galaxy")
class TestIcebergIncrementalDeleteInsertWithLocation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models__location_specified,
        }

    def test_temporary_table_location(self, project):
        # Create model with properties
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert f'create table "{project.database}"."{project.test_schema}"."model"' in logs
        assert "location = 's3a://datalake/model'" in logs

        # Temporary table is created on the second run
        # So, now we check if the second run is successful and location
        # is patched correctly
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert (
            f'create table "{project.database}"."{project.test_schema}"."model__dbt_tmp"' in logs
        )
        assert "location = 's3a://datalake/model__dbt_tmp'" in logs


@pytest.mark.iceberg
class TestIcebergCompositeUniqueKeys(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_delete_insert_incremental_predicates.csv": seeds__expected_delete_insert_incremental_predicates_csv,
            "expected_delete_insert_composite_keys.csv": seeds__expected_delete_insert_composite_keys_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_incremental_predicates_sql,
            "delete_insert_composite_keys.sql": models__delete_insert_composite_keys_sql,
        }

    def test__incremental_predicates_composite_keys(self, project):
        """seed should match model after two incremental runs"""

        expected_fields = self.get_expected_fields(
            relation="expected_delete_insert_composite_keys", seed_rows=4
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="expected_delete_insert_composite_keys",
            incremental_model="delete_insert_composite_keys",
            update_sql_file=None,
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)
