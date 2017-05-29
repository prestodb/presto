ALTER TABLE benchmark_runs ADD COLUMN executions_mean_duration DOUBLE PRECISION NOT NULL DEFAULT -1;
ALTER TABLE benchmark_runs ADD COLUMN executions_stddev_duration DOUBLE PRECISION NOT NULL DEFAULT -1;

UPDATE benchmark_runs b
SET executions_mean_duration = bro.mean_duration, executions_stddev_duration = bro.stddev_duration
FROM
  (SELECT
     br.id,
     avg(m.value)                  AS mean_duration,
     coalesce(stddev(m.value), -1) AS stddev_duration
   FROM benchmark_runs br
     INNER JOIN executions e ON br.id = e.benchmark_run_id
     INNER JOIN execution_measurements em ON e.id = em.execution_id
     INNER JOIN measurements m ON m.id = em.measurement_id
   WHERE m.name = 'duration' AND br.status = 'ENDED'
   GROUP BY br.id) bro
WHERE bro.id = b.id;