drop index :GRAPHILE_WORKER_SCHEMA.jobs_priority_run_at_id_locked_at_without_failures_idx;

create index jobs_priority_run_at_id_locked_at_without_failures_idx
    on :GRAPHILE_WORKER_SCHEMA.jobs (priority, run_at, id, locked_at)
    where (attempts < max_attempts) and front_of_the_queue = true;

create or replace function :GRAPHILE_WORKER_SCHEMA.complete_job(worker_id text, job_id bigint) returns :GRAPHILE_WORKER_SCHEMA.jobs as $$
declare
v_row :GRAPHILE_WORKER_SCHEMA.jobs;
declare
v_next_job_id bigint;
declare now timestamp with time zone := now();
declare job_expiry interval := '04:00:00'::interval;
begin
delete from :GRAPHILE_WORKER_SCHEMA.jobs
where id = job_id
    returning * into v_row;

if v_row.queue_name is not null then
select jobs.id into v_next_job_id
from :GRAPHILE_WORKER_SCHEMA.jobs
where (jobs.locked_at is null or jobs.locked_at < (now - job_expiry))
  and queue_name = v_row.queue_name
  and front_of_the_queue = false
  and attempts < max_attempts
order by priority asc, run_at asc, id asc
    limit 1
    for update
                skip locked;

update :GRAPHILE_WORKER_SCHEMA.jobs
set front_of_the_queue = true
where id = v_next_job_id;
end if;

return v_row;
end;
$$ language plpgsql;

create or replace function :GRAPHILE_WORKER_SCHEMA.fail_job(worker_id text, job_id bigint, error_message text) returns :GRAPHILE_WORKER_SCHEMA.jobs as $$
declare
v_row :GRAPHILE_WORKER_SCHEMA.jobs;
declare
v_next_job_id bigint;
declare now timestamp with time zone := now();
declare job_expiry interval := '04:00:00'::interval;
begin
update :GRAPHILE_WORKER_SCHEMA.jobs
set
    last_error = error_message,
    run_at = greatest(now(), run_at) + (exp(least(attempts, 10))::text || ' seconds')::interval,
    locked_by = null,
    locked_at = null
where id = job_id and locked_by = worker_id
    returning * into v_row;

if v_row.queue_name is not null then
select jobs.id into v_next_job_id
from :GRAPHILE_WORKER_SCHEMA.jobs
where (jobs.locked_at is null or jobs.locked_at < (now - job_expiry))
  and queue_name = v_row.queue_name
  and front_of_the_queue = false
  and attempts < max_attempts
order by priority asc, run_at asc, id asc
    limit 1
    for update
                skip locked;

update :GRAPHILE_WORKER_SCHEMA.jobs
set front_of_the_queue = true
where id = v_next_job_id;
end if;

return v_row;
end;
$$ language plpgsql volatile strict;
