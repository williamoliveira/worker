const { Pool } = require("pg");

const pgPool = new Pool({ connectionString: process.env.PERF_DATABASE_URL, max: 50 });

const jobCount = parseInt(process.argv[2], 10) || 1;
const queueCount = parseInt(process.argv[3], 10) || 1;

async function main() {
  let promises = []

  for (var i = queueCount - 1; i >= 0; i--) {
    console.log(i)
    promises.push(pgPool.query(
      `
      do $$
      begin
        perform graphile_worker.add_job('log_if_999', json_build_object('id', i), 'name-${i}') from generate_series(1, ${jobCount}) i;
      end;
      $$ language plpgsql;
    `,
    ));
  }

  await Promise.all(promises)

  pgPool.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
