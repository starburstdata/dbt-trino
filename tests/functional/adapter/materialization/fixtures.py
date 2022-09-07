seed_csv = """
id,name,some_date
1,Easton,1981-05-20 06:46:51
2,Lillian,1978-09-03 18:10:33
3,Jeremiah,1982-03-11 03:59:51
4,Nolan,1976-05-06 20:21:35
""".lstrip()

model_sql = """
select * from {{ ref('seed') }}
"""

profile_yml = """
version: 2
models:
  - name: materialization
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: name
        tests:
          - not_null
"""
