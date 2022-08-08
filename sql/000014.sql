DO
$do$
    declare
        queue         text;
        v_next_job_id bigint;
    BEGIN
        update :GRAPHILE_WORKER_SCHEMA.jobs set front_of_the_queue = false where front_of_the_queue = true;

        FOR queue in (select distinct queue_name from :GRAPHILE_WORKER_SCHEMA.jobs)
            LOOP
                select jobs.id
                into v_next_job_id
                from "graphile_worker".jobs
                where queue_name = queue
                  and front_of_the_queue = false
                  and attempts < max_attempts
                order by priority asc, run_at asc, id asc
                limit 1 for update
                    skip locked;

                if v_next_job_id is not null then
                    begin
                        update "graphile_worker".jobs
                        set front_of_the_queue = true
                        where id = v_next_job_id;

                        raise notice 'Set job id "%" to be front of the queue "%"', v_next_job_id, queue;
                    exception
                        when unique_violation then
                            null;
                    end;
                end if;
            END LOOP;
    END
$do$;
