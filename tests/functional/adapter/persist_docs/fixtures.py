seed_csv = """
id,name,date
1,Easton,1981-05-20 06:46:51
2,Lillian,1978-09-03 18:10:33
3,Jeremiah,1982-03-11 03:59:51
4,Nolan,1976-05-06 20:21:35
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

table_profile_yml = """
version: 2
models:
  - name: table_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as well as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as well as other;
          '''abc123'''
          reserved -- characters
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
        tests:
          - unique
          - not_null
      - name: name
        description: |
          Fancy column description
        tests:
          - not_null
seeds:
  - name: seed
    description: |
      Seed model description "with double quotes"
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
      - name: name
        description: |
          Fancy column description
        tests:
          - not_null
"""


view_profile_yml = """
version: 2
models:
  - name: view_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as well as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: name
        tests:
          - not_null
seeds:
  - name: seed
    description: |
      Seed model description "with double quotes"
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
      - name: name
        description: |
          Fancy column description
        tests:
          - not_null
"""

incremental_profile_yml = """
version: 2
models:
  - name: incremental_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as well as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as well as other;
          '''abc123'''
          reserved -- characters
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
        tests:
          - unique
          - not_null
      - name: name
        description: |
          Fancy column description
        tests:
          - not_null
seeds:
  - name: seed
    description: |
      Seed model description "with double quotes"
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
      - name: name
        description: |
          Fancy column description
        tests:
          - not_null
"""
