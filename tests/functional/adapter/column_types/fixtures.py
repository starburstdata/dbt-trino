model_sql = """
select
    cast(0 as tinyint) as tinyint_col,
    cast(1 as smallint) as smallint_col,
    cast(2 as integer) as integer_col,
    cast(2 as int) as int_col,
    cast(3 as bigint) as bigint_col,
    cast(4.0 as real) as real_col,
    cast(5.0 as double) as double_col,
    cast(5.5 as double precision) as double_precision_col,
    cast(6.0 as decimal) as decimal_col,
    cast('7' as char) as char_col,
    cast('8' as varchar(20)) as varchar_col
"""

schema_yml = """
version: 2
models:
  - name: model
    tests:
      - is_type:
          column_map:
            tinyint_col: ['integer', 'number']
            smallint_col: ['integer', 'number']
            integer_col: ['integer', 'number']
            int_col: ['integer', 'number']
            bigint_col: ['integer', 'number']
            real_col: ['float', 'number']
            double_col: ['float', 'number']
            double_precision_col: ['float', 'number']
            decimal_col: ['numeric', 'number']
            char_col: ['string', 'not number']
            varchar_col: ['string', 'not number']
"""
