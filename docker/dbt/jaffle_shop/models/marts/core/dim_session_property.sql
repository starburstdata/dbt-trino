{{
  config(
    pre_hook="set session query_max_run_time='20s'"
  )
}}

select 'OK' as status
