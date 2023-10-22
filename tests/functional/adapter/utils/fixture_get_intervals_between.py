models__trino_test_get_intervals_between_sql = """
SELECT
  {{ get_intervals_between("'2023-09-01'", "'2023-09-12'", "day") }} as intervals,
  11 as expected

"""
