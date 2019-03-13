import { TaskList, Worker, Job, WithPgClient } from "./interfaces";
import globalDebug from "./debug";
import { IDLE_DELAY, MAX_CONTIGUOUS_ERRORS } from "./config";
import * as assert from "assert";
import deferred from "./deferred";
import { makeHelpers } from "./helpers";

export function makeNewWorker(
  tasks: TaskList,
  withPgClient: WithPgClient,
  continuous = true,
  workerId = `worker-${Math.random()}`
): Worker {
  const promise = deferred();
  let activeJob: Job | null = null;

  let doNextTimer: NodeJS.Timer | null = null;
  const cancelDoNext = () => {
    if (doNextTimer) {
      clearTimeout(doNextTimer);
      doNextTimer = null;
      return true;
    }
    return false;
  };
  let active = true;

  const debug = (msg: string) => {
    globalDebug("[Worker %s] %s", workerId, msg);
  };

  const release = () => {
    if (!active) {
      return;
    }
    active = false;
    if (!cancelDoNext()) {
      // Nothing in progress; resolve the promise
      promise.resolve();
    }
    return promise;
  };

  debug(`Spawned`);

  let contiguousErrors = 0;

  const doNext = async (): Promise<void> => {
    cancelDoNext();
    assert(active, "doNext called when active was false");
    assert(!activeJob, "There should be no active job");

    // Find us a job
    try {
      const supportedTaskNames = Object.keys(tasks);
      assert(supportedTaskNames.length, "No runnable tasks!");

      const {
        rows: [jobRow]
      } = await withPgClient(client =>
        client.query("SELECT * FROM graphile_worker.get_job($1, $2);", [
          workerId,
          supportedTaskNames
        ])
      );
      activeJob = jobRow && jobRow.id ? jobRow : null;
    } catch (err) {
      if (continuous) {
        contiguousErrors++;
        debug(
          `Failed to acquire job: ${
            err.message
          } (${contiguousErrors}/${MAX_CONTIGUOUS_ERRORS})`
        );
        if (contiguousErrors >= MAX_CONTIGUOUS_ERRORS) {
          promise.reject(
            new Error(
              `Failed ${contiguousErrors} times in a row to acquire job; latest error: ${
                err.message
              }`
            )
          );
          release();
          return;
        } else {
          if (active) {
            // Error occurred fetching a job; try again...
            doNextTimer = setTimeout(() => doNext(), IDLE_DELAY);
          } else {
            promise.reject(err);
          }
          return;
        }
      } else {
        promise.reject(err);
        release();
        return;
      }
    }
    contiguousErrors = 0;

    // If we didn't get a job, try again later (if appropriate)
    if (!activeJob) {
      if (continuous) {
        if (active) {
          doNextTimer = setTimeout(() => doNext(), IDLE_DELAY);
        } else {
          promise.resolve();
        }
      } else {
        promise.resolve();
        release();
      }
      return;
    }

    // We did get a job then; store it into the current scope.
    const job = activeJob;

    // We may want to know if an error occurred or not
    let err: Error | null = null;
    try {
      /*
       * Be **VERY** careful about which parts of this code can throw - we
       * **MUST** release the job once we've attempted it (success or error).
       */
      const startTimestamp = process.hrtime();
      try {
        debug(`Found task ${job.id} (${job.task_identifier})`);
        const worker = tasks[job.task_identifier];
        assert(worker, `Unsupported task '${job.task_identifier}'`);
        const helpers = makeHelpers(job, { withPgClient });
        await worker(job.payload, helpers);
      } catch (error) {
        err = error;
      }
      const durationRaw = process.hrtime(startTimestamp);
      const duration = durationRaw[0] * 1e3 + durationRaw[1] * 1e-6;
      if (err) {
        const { message, stack } = err;
        // tslint:disable-next-line no-console
        console.error(
          `Failed task ${job.id} (${job.task_identifier}) with error ${
            err.message
          } (${duration.toFixed(2)}ms)`,
          { stack }
        );
        // TODO: retry logic, in case of server connection interruption
        await withPgClient(client =>
          client.query("SELECT * FROM graphile_worker.fail_job($1, $2, $3);", [
            workerId,
            job.id,
            message
          ])
        );
      } else {
        // tslint:disable-next-line no-console
        console.log(
          `Completed task ${job.id} (${
            job.task_identifier
          }) with success (${duration.toFixed(2)}ms)`
        );
        // TODO: retry logic, in case of server connection interruption
        await withPgClient(client =>
          client.query("SELECT * FROM graphile_worker.complete_job($1, $2);", [
            workerId,
            job.id
          ])
        );
      }
    } catch (fatalError) {
      const when = err ? `after failure '${err.message}'` : "after success";
      // tslint:disable-next-line no-console
      console.error(
        `Failed to release job '${job.id}' ${when}; committing seppuku\n${
          fatalError.message
        }`
      );
      promise.reject(fatalError);
      release();
      return;
    } finally {
      activeJob = null;
    }
    if (active) {
      doNext();
    } else {
      promise.resolve();
    }
  };

  const nudge = () => {
    assert(active, "nudge called after worker terminated");
    if (doNextTimer) {
      // Must be idle; call early
      doNext();
      return true;
    }
    // Not idle; find someone else!
    return false;
  };

  // Start!
  doNext();

  return {
    nudge,
    workerId,
    release,
    promise,
    getActiveJob: () => activeJob
  };
}