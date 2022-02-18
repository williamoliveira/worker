alter table :GRAPHILE_WORKER_SCHEMA.jobs add column front_of_the_queue boolean default false not null;
create unique index front_of_the_queue_unique on :GRAPHILE_WORKER_SCHEMA.jobs (front_of_the_queue, queue_name) where front_of_the_queue = true and queue_name is not null;
create index front_of_the_queue_false on :GRAPHILE_WORKER_SCHEMA.jobs (front_of_the_queue, queue_name) where front_of_the_queue = false and queue_name is not null;

drop index :GRAPHILE_WORKER_SCHEMA.jobs_priority_run_at_id_locked_at_without_failures_idx;

create index jobs_priority_run_at_id_locked_at_without_failures_idx
    on :GRAPHILE_WORKER_SCHEMA.jobs (priority, run_at, id, locked_at, front_of_the_queue)
    where (attempts < max_attempts) and front_of_the_queue = true;

drop trigger _500_decrease_job_queue_count on :GRAPHILE_WORKER_SCHEMA.jobs;
drop trigger _500_decrease_job_queue_count_update on :GRAPHILE_WORKER_SCHEMA.jobs;
drop trigger _500_increase_job_queue_count on :GRAPHILE_WORKER_SCHEMA.jobs;
drop trigger _500_increase_job_queue_count_update on :GRAPHILE_WORKER_SCHEMA.jobs;

drop table :GRAPHILE_WORKER_SCHEMA.job_queues;

drop function :GRAPHILE_WORKER_SCHEMA.get_job(text, text[], interval, text[], timestamp with time zone);
create function :GRAPHILE_WORKER_SCHEMA.get_job(
  worker_id text,
  task_identifiers text[] = null,
  job_expiry interval = interval '4 hours',
  forbidden_flags text[] = null,
  now timestamptz = now()
) returns :GRAPHILE_WORKER_SCHEMA.jobs as $$
declare
  v_job_id bigint;
  v_queue_name text;
  v_row :GRAPHILE_WORKER_SCHEMA.jobs;
begin
  if worker_id is null or length(worker_id) < 10 then
    raise exception 'invalid worker id';
  end if;

  select jobs.queue_name, jobs.id into v_queue_name, v_job_id
  from :GRAPHILE_WORKER_SCHEMA.jobs
  where (jobs.locked_at is null or jobs.locked_at < (now - job_expiry))
    and front_of_the_queue = true
    and run_at <= now
    and attempts < max_attempts
    and (task_identifiers is null or task_identifier = any(task_identifiers))
    and (forbidden_flags is null or (flags ?| forbidden_flags) is not true)
  order by priority asc, run_at asc, id asc
  limit 1
  for update
  skip locked;

  if v_job_id is null then
    return null;
  end if;

  update :GRAPHILE_WORKER_SCHEMA.jobs
    set
      attempts = attempts + 1,
      locked_by = worker_id,
      locked_at = now
    where id = v_job_id
    returning * into v_row;

  return v_row;
end;
$$ language plpgsql volatile;

drop function :GRAPHILE_WORKER_SCHEMA.add_job(text, json, text, timestamp with time zone, integer, text, integer, text[], text);
create function :GRAPHILE_WORKER_SCHEMA.add_job(
  identifier text,
  payload json = null,
  queue_name text = null,
  run_at timestamptz = null,
  max_attempts integer = null,
  job_key text = null,
  priority integer = null,
  flags text[] = null,
  job_key_mode text = 'replace'
) returns :GRAPHILE_WORKER_SCHEMA.jobs as $$
declare
v_job :GRAPHILE_WORKER_SCHEMA.jobs;
begin
  -- Apply rationality checks
  if length(identifier) > 128 then
    raise exception 'Task identifier is too long (max length: 128).' using errcode = 'GWBID';
end if;
  if queue_name is not null and length(queue_name) > 128 then
    raise exception 'Job queue name is too long (max length: 128).' using errcode = 'GWBQN';
end if;
  if job_key is not null and length(job_key) > 512 then
    raise exception 'Job key is too long (max length: 512).' using errcode = 'GWBJK';
end if;
  if max_attempts < 1 then
    raise exception 'Job maximum attempts must be at least 1.' using errcode = 'GWBMA';
end if;
  if job_key is not null and (job_key_mode is null or job_key_mode in ('replace', 'preserve_run_at')) then
    -- Upsert job if existing job isn't locked, but in the case of locked
    -- existing job create a new job instead as it must have already started
    -- executing (i.e. it's world state is out of date, and the fact add_job
    -- has been called again implies there's new information that needs to be
    -- acted upon).
    insert into :GRAPHILE_WORKER_SCHEMA.jobs (
      task_identifier,
      payload,
      queue_name,
      run_at,
      max_attempts,
      key,
      priority,
      flags
    )
      values(
        identifier,
        coalesce(payload, '{}'::json),
        queue_name,
        coalesce(run_at, now()),
        coalesce(max_attempts, 25),
        job_key,
        coalesce(priority, 0),
        (
          select jsonb_object_agg(flag, true)
          from unnest(flags) as item(flag)
        )
      )
      on conflict (key) do update set
    task_identifier=excluded.task_identifier,
                                    payload=excluded.payload,
                                    queue_name=excluded.queue_name,
                                    max_attempts=excluded.max_attempts,
                                    run_at=(case
                                    when job_key_mode = 'preserve_run_at' and jobs.attempts = 0 then jobs.run_at
                                    else excluded.run_at
                                    end),
                                    priority=excluded.priority,
                                    revision=jobs.revision + 1,
                                    flags=excluded.flags,
                                    -- always reset error/retry state
                                    attempts=0,
                                    last_error=null
                           where jobs.locked_at is null
                                    returning *
                           into v_job;
-- If upsert succeeded (insert or update), return early
if not (v_job is null) then
      return v_job;
end if;
    -- Upsert failed -> there must be an existing job that is locked. Remove
    -- existing key to allow a new one to be inserted, and prevent any
    -- subsequent retries of existing job by bumping attempts to the max
    -- allowed.
update :GRAPHILE_WORKER_SCHEMA.jobs
set
    key = null,
    attempts = jobs.max_attempts
where key = job_key;
elsif job_key is not null and job_key_mode = 'unsafe_dedupe' then
    -- Insert job, but if one already exists then do nothing, even if the
    -- existing job has already started (and thus represents an out-of-date
    -- world state). This is dangerous because it means that whatever state
    -- change triggered this add_job may not be acted upon (since it happened
    -- after the existing job started executing, but no further job is being
    -- scheduled), but it is useful in very rare circumstances for
    -- de-duplication. If in doubt, DO NOT USE THIS.
    insert into :GRAPHILE_WORKER_SCHEMA.jobs (
      task_identifier,
      payload,
      queue_name,
      run_at,
      max_attempts,
      key,
      priority,
      flags
    )
      values(
        identifier,
        coalesce(payload, '{}'::json),
        queue_name,
        coalesce(run_at, now()),
        coalesce(max_attempts, 25),
        job_key,
        coalesce(priority, 0),
        (
          select jsonb_object_agg(flag, true)
          from unnest(flags) as item(flag)
        )
      )
      on conflict (key)
      -- Bump the revision so that there's something to return
      do update set revision = jobs.revision + 1
                   returning *
         into v_job;
return v_job;
elsif job_key is not null then
    raise exception 'Invalid job_key_mode value, expected ''replace'', ''preserve_run_at'' or ''unsafe_dedupe''.' using errcode = 'GWBKM';
end if;
  -- insert the new job. Assume no conflicts due to the update above
begin
insert into :GRAPHILE_WORKER_SCHEMA.jobs(
    task_identifier,
    payload,
    queue_name,
    run_at,
    max_attempts,
    key,
    priority,
    flags,
    front_of_the_queue
)
values(
          identifier,
          coalesce(payload, '{}'::json),
          queue_name,
          coalesce(run_at, now()),
          coalesce(max_attempts, 25),
          job_key,
          coalesce(priority, 0),
          (
              select jsonb_object_agg(flag, true)
              from unnest(flags) as item(flag)
          ),
          true
      )
    returning *
into v_job;
exception
      when unique_violation then
        insert into :GRAPHILE_WORKER_SCHEMA.jobs(
            task_identifier,
            payload,
            queue_name,
            run_at,
            max_attempts,
            key,
            priority,
            flags,
            front_of_the_queue
          )
            values(
              identifier,
              coalesce(payload, '{}'::json),
              queue_name,
              coalesce(run_at, now()),
              coalesce(max_attempts, 25),
              job_key,
              coalesce(priority, 0),
              (
                select jsonb_object_agg(flag, true)
                from unnest(flags) as item(flag)
              ),
              false
            )
            returning *
            into v_job;
end;

return v_job;
end;
$$ language plpgsql volatile;

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
  and run_at <= now
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
  and run_at <= now
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
