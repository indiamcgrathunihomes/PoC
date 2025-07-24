
select
  current_role() as role,
  current_user() as user,
  current_warehouse() as warehouse,
  current_database() as database,
  current_schema() as schema