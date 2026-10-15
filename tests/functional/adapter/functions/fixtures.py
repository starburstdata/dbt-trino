MY_UDF_SQL = """
price * 2
""".strip()

MY_UDF_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: double
        description: The price of the standard item
    returns:
      data_type: double
      description: The resulting xlarge price
"""

MY_UDF_WITH_DEFAULT_ARG_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: double
        description: The price of the standard item
        default_value: 100
    returns:
      data_type: double
      description: The resulting xlarge price
"""

MY_UDF_QUOTED_DESCRIPTION_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the xlarge price for a customer's item
    arguments:
      - name: price
        data_type: double
    returns:
      data_type: double
"""

# Trino rejects NOT DETERMINISTIC on a body that is deterministic (and vice versa), so a
# non-deterministic function needs a body that calls a non-deterministic function.
NON_DETERMINISTIC_UDF_SQL = """
price * 2 + 0 * random()
""".strip()

MY_UDF_WITH_SELECT_SQL = """
SELECT price * 2
""".strip()

MY_UDF_WITH_SUBQUERY_SQL = """
(SELECT price * 2)
""".strip()

MY_UDF_FLOAT_YML = """
functions:
  - name: price_for_xlarge
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MULTI_ARG_UDF_SQL = """
CASE WHEN flag THEN concat(label, ':', cast(amount as varchar)) ELSE label END
""".strip()

MULTI_ARG_UDF_YML = """
functions:
  - name: describe_amount
    arguments:
      - name: label
        data_type: varchar
      - name: amount
        data_type: decimal(10,2)
      - name: flag
        data_type: boolean
    returns:
      data_type: varchar
"""

NO_ARG_UDF_SQL = """
42
""".strip()

NO_ARG_UDF_YML = """
functions:
  - name: meaning_of_life
    returns:
      data_type: integer
"""

# `price_for_xlarge` with a different argument type, which Trino treats as a separate overload
MY_UDF_BIGINT_YML = """
functions:
  - name: price_for_xlarge
    arguments:
      - name: price
        data_type: bigint
    returns:
      data_type: bigint
"""

MODEL_USING_UDF_SQL = """
select {{ function('price_for_xlarge') }}(cast(21 as double)) as doubled
"""

MY_UDF_WITH_GRANTS_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      grants:
        select: ["some_user"]
    arguments:
      - name: price
        data_type: double
    returns:
      data_type: double
"""
