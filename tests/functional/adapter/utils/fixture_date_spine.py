# If date_spine works properly, there should be no `null` values in the resulting model

models__trino_test_date_spine_sql = """
with generated_dates as (
    {{ date_spine("day", "'2023-09-01'", "'2023-09-10'") }}
), expected_dates as (
    select cast('2023-09-01' as date) as expected
    union all
    select cast('2023-09-02' as date) as expected
    union all
    select cast('2023-09-03' as date) as expected
    union all
    select cast('2023-09-04' as date) as expected
    union all
    select cast('2023-09-05' as date) as expected
    union all
    select cast('2023-09-06' as date) as expected
    union all
    select cast('2023-09-07' as date) as expected
    union all
    select cast('2023-09-08' as date) as expected
    union all
    select cast('2023-09-09' as date) as expected
), joined as (
    select
        generated_dates.date_day,
        expected_dates.expected
    from generated_dates
    left join expected_dates on generated_dates.date_day = expected_dates.expected
)

SELECT * from joined
"""
