import { migrate } from "../src/migrate";
import { withPgClient } from "./helpers";
import { TaskList, Task } from "../src/interfaces";
import { runAllJobs } from "../src/main";
import { PoolClient } from "pg";

async function reset(pgClient: PoolClient) {
  await pgClient.query("drop schema if exists graphile_worker cascade;");
  await migrate(pgClient);
}

test("runs jobs", () =>
  withPgClient(async pgClient => {
    await reset(pgClient);

    // Schedule a job
    const start = new Date();
    await pgClient.query(`select graphile_worker.add_job('job1', '{"a": 1}')`);

    // Assert that it has an entry in jobs / job_queues
    const { rows: jobs } = await pgClient.query(
      `select * from graphile_worker.jobs`
    );
    expect(jobs).toHaveLength(1);
    const job = jobs[0];
    expect(+job.run_at).toBeGreaterThanOrEqual(+start);
    expect(+job.run_at).toBeLessThanOrEqual(+new Date());
    const { rows: jobQueues } = await pgClient.query(
      `select * from graphile_worker.job_queues`
    );
    expect(jobQueues).toHaveLength(1);
    const q = jobQueues[0];
    expect(q.queue_name).toEqual(job.queue_name);
    expect(q.job_count).toEqual(1);
    expect(q.locked_at).toBeFalsy();
    expect(q.locked_by).toBeFalsy();

    // Run the task
    const job1: Task = jest.fn(o => {
      expect(o).toMatchInlineSnapshot(`
Object {
  "a": 1,
}
`);
    });
    const job2: Task = jest.fn();
    const tasks: TaskList = {
      job1,
      job2
    };
    await runAllJobs(tasks, pgClient);

    // Job should have been called once only
    expect(job1).toHaveBeenCalledTimes(1);
    expect(job2).not.toHaveBeenCalled();
  }));

test("schedules errors for retry", () =>
  withPgClient(async pgClient => {
    await reset(pgClient);

    // Schedule a job
    const start = new Date();
    await pgClient.query(`select graphile_worker.add_job('job1', '{"a": 1}')`);

    {
      const { rows: jobs } = await pgClient.query(
        `select * from graphile_worker.jobs`
      );
      expect(jobs).toHaveLength(1);
      const job = jobs[0];
      expect(job.task_identifier).toEqual("job1");
      expect(job.payload).toEqual({ a: 1 });
      expect(+job.run_at).toBeGreaterThanOrEqual(+start);
      expect(+job.run_at).toBeLessThanOrEqual(+new Date());

      const { rows: jobQueues } = await pgClient.query(
        `select * from graphile_worker.job_queues`
      );
      expect(jobQueues).toHaveLength(1);
      const q = jobQueues[0];
      expect(q.queue_name).toEqual(job.queue_name);
      expect(q.job_count).toEqual(1);
      expect(q.locked_at).toBeFalsy();
      expect(q.locked_by).toBeFalsy();
    }

    // Run the job (it will fail)
    const job1: Task = jest.fn(() => {
      throw new Error("TEST_ERROR");
    });
    const tasks: TaskList = {
      job1
    };
    await runAllJobs(tasks, pgClient);
    expect(job1).toHaveBeenCalledTimes(1);

    // Check that it failed as expected
    {
      const { rows: jobs } = await pgClient.query(
        `select * from graphile_worker.jobs`
      );
      expect(jobs).toHaveLength(1);
      const job = jobs[0];
      expect(job.task_identifier).toEqual("job1");
      expect(job.attempts).toEqual(1);
      expect(job.max_attempts).toEqual(25);
      expect(job.last_error).toEqual("TEST_ERROR");
      // It's the first attempt, so delay is exp(1) ~= 2.719 seconds
      expect(+job.run_at).toBeGreaterThanOrEqual(+start + 2718);
      expect(+job.run_at).toBeLessThanOrEqual(+new Date() + 2719);

      const { rows: jobQueues } = await pgClient.query(
        `select * from graphile_worker.job_queues`
      );
      expect(jobQueues).toHaveLength(1);
      const q = jobQueues[0];
      expect(q.queue_name).toEqual(job.queue_name);
      expect(q.job_count).toEqual(1);
      expect(q.locked_at).toBeFalsy();
      expect(q.locked_by).toBeFalsy();
    }
  }));

test("retries job", () =>
  withPgClient(async pgClient => {
    await reset(pgClient);

    // Add the job
    await pgClient.query(`select graphile_worker.add_job('job1', '{"a": 1}')`);
    let counter = 0;
    const job1: Task = jest.fn(() => {
      throw new Error(`TEST_ERROR ${++counter}`);
    });
    const tasks: TaskList = {
      job1
    };

    // Run the job (it will error)
    await runAllJobs(tasks, pgClient);
    expect(job1).toHaveBeenCalledTimes(1);

    // Should do nothing the second time, because it's queued for the future (assuming we run this fast enough afterwards!)
    await runAllJobs(tasks, pgClient);
    expect(job1).toHaveBeenCalledTimes(1);

    // Tell the job to be runnable
    await pgClient.query(
      `update graphile_worker.jobs set run_at = now() where task_identifier = 'job1'`
    );

    // Run the job
    const start = new Date();
    await runAllJobs(tasks, pgClient);

    // It should have ran again
    expect(job1).toHaveBeenCalledTimes(2);

    // And it should have been rejected again
    {
      const { rows: jobs } = await pgClient.query(
        `select * from graphile_worker.jobs`
      );
      expect(jobs).toHaveLength(1);
      const job = jobs[0];
      expect(job.task_identifier).toEqual("job1");
      expect(job.attempts).toEqual(2);
      expect(job.max_attempts).toEqual(25);
      expect(job.last_error).toEqual("TEST_ERROR 2");
      // It's the second attempt, so delay is exp(2) ~= 7.389 seconds
      expect(+job.run_at).toBeGreaterThanOrEqual(+start + 7388);
      expect(+job.run_at).toBeLessThanOrEqual(+new Date() + 7389);

      const { rows: jobQueues } = await pgClient.query(
        `select * from graphile_worker.job_queues`
      );
      expect(jobQueues).toHaveLength(1);
      const q = jobQueues[0];
      expect(q.queue_name).toEqual(job.queue_name);
      expect(q.job_count).toEqual(1);
      expect(q.locked_at).toBeFalsy();
      expect(q.locked_by).toBeFalsy();
    }
  }));

test("supports future-scheduled jobs", () =>
  withPgClient(async pgClient => {
    await reset(pgClient);

    // Add the job
    await pgClient.query(
      `select graphile_worker.add_job('future', run_at := now() + interval '3 seconds')`
    );
    const future: Task = jest.fn();
    const tasks: TaskList = {
      future
    };

    // Run all jobs (none are ready)
    await runAllJobs(tasks, pgClient);
    expect(future).not.toHaveBeenCalled();

    // Still not ready
    await runAllJobs(tasks, pgClient);
    expect(future).not.toHaveBeenCalled();

    // Tell the job to be runnable
    await pgClient.query(
      `update graphile_worker.jobs set run_at = now() where task_identifier = 'future'`
    );

    // Run the job
    await runAllJobs(tasks, pgClient);

    // It should have ran again
    expect(future).toHaveBeenCalledTimes(1);

    // It should be successful
    {
      const { rows: jobs } = await pgClient.query(
        `select * from graphile_worker.jobs`
      );
      expect(jobs).toHaveLength(0);
      const { rows: jobQueues } = await pgClient.query(
        `select * from graphile_worker.job_queues`
      );
      expect(jobQueues).toHaveLength(0);
    }
  }));
