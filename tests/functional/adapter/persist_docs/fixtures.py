seed_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
""".lstrip()

table_model = """
{{config(materialized = "table")}}
select * from {{ ref('seed') }}
"""

view_model = """
{{config(materialized = "view")}}
select * from {{ ref('seed') }}
"""

incremental_model = """
{{config(materialized = "incremental")}}
select * from {{ ref('seed') }}
"""

profile_yml = """
version: 2
models:
  - name: table_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
        tests:
          - unique
          - not_null
      - name: name
        tests:
          - not_null
  - name: view_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
        tests:
          - unique
          - not_null
      - name: name
        tests:
          - not_null
  - name: incremental_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
        tests:
          - unique
          - not_null
      - name: name
        tests:
          - not_null
"""
