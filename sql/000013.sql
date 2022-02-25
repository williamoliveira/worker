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
    locked_at = null,
    front_of_the_queue = false
where id = job_id and locked_by = worker_id
    returning * into v_row;

if v_row.queue_name is not null then
    select jobs.id into v_next_job_id
    from "graphile_worker".jobs
    where (jobs.locked_at is null or jobs.locked_at < (now - job_expiry))
      and queue_name = v_row.queue_name
      and front_of_the_queue = false
      and attempts < max_attempts
    order by priority asc, run_at asc, id asc
    limit 1
    for update
    skip locked;

    begin
        update "graphile_worker".jobs
        set front_of_the_queue = true
        where id = v_next_job_id;
    exception
        when unique_violation then
            null;
    end;
end if;

return v_row;
end;
$$ language plpgsql volatile strict;

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
#variable_conflict use_variable
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

update :GRAPHILE_WORKER_SCHEMA.jobs j
set front_of_the_queue = false
where j.queue_name = queue_name
  and j.front_of_the_queue = true
  and j.run_at > coalesce(run_at, now());

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
