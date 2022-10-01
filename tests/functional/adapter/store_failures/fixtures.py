seed_csv = """
id,value
1,1
2,2
3,3
4,4
""".lstrip()

table_model = """
select * from {{ ref('seed') }}
"""

table_profile_yml = """
version: 2
models:
  - name: table_model
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: value
        quote: true
        tests:
          - not_null
          - accepted_values:
              values:
                - 1
                - 2
                - 3
                - 4
              quote: false

seeds:
  - name: seed
    columns:
      - name: id
      - name: value
        tests:
          - not_null
"""
