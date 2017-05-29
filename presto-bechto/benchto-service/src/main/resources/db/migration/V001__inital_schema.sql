CREATE TABLE benchmark_run_measurements
(
  benchmark_run_id BIGINT NOT NULL,
  measurement_id   BIGINT NOT NULL,
  PRIMARY KEY (benchmark_run_id, measurement_id)
);

CREATE TABLE benchmark_runs
(
  id             BIGSERIAL PRIMARY KEY NOT NULL,
  name           VARCHAR(255)          NOT NULL,
  unique_name    VARCHAR(1024)         NOT NULL,
  sequence_id    VARCHAR(64)           NOT NULL,
  started        TIMESTAMP             NOT NULL,
  ended          TIMESTAMP,
  version        BIGINT                NOT NULL,
  environment_id BIGINT                NOT NULL,
  status         VARCHAR(10)           NOT NULL
);

CREATE TABLE benchmark_runs_attributes
(
  benchmark_run_id BIGINT       NOT NULL,
  name             VARCHAR(255) NOT NULL,
  value            VARCHAR      NOT NULL,
  PRIMARY KEY (benchmark_run_id, name)
);

CREATE TABLE benchmark_runs_variables
(
  benchmark_run_id BIGINT       NOT NULL,
  name             VARCHAR(255) NOT NULL,
  value            VARCHAR      NOT NULL,
  PRIMARY KEY (benchmark_run_id, name)
);

CREATE TABLE environment_attributes
(
  environment_id BIGINT       NOT NULL,
  name           VARCHAR(255) NOT NULL,
  value          VARCHAR      NOT NULL,
  PRIMARY KEY (environment_id, name)
);

CREATE TABLE environments
(
  id      BIGSERIAL PRIMARY KEY NOT NULL,
  name    VARCHAR(64)           NOT NULL,
  version BIGINT                NOT NULL,
  started TIMESTAMP             NOT NULL
);

CREATE TABLE execution_attributes
(
  execution_id BIGINT       NOT NULL,
  name         VARCHAR(255) NOT NULL,
  value        VARCHAR      NOT NULL,
  PRIMARY KEY (execution_id, name)
);

CREATE TABLE execution_measurements
(
  execution_id   BIGINT NOT NULL,
  measurement_id BIGINT NOT NULL,
  PRIMARY KEY (execution_id, measurement_id)
);

CREATE TABLE executions
(
  id               BIGSERIAL PRIMARY KEY NOT NULL,
  sequence_id      VARCHAR(64)           NOT NULL,
  benchmark_run_id BIGINT                NOT NULL,
  started          TIMESTAMP             NOT NULL,
  ended            TIMESTAMP,
  version          BIGINT                NOT NULL,
  status           VARCHAR(10)           NOT NULL
);

CREATE TABLE measurements
(
  id    BIGSERIAL PRIMARY KEY NOT NULL,
  name  VARCHAR(64)           NOT NULL,
  unit  VARCHAR(16)           NOT NULL,
  value DOUBLE PRECISION      NOT NULL
);

ALTER TABLE benchmark_run_measurements ADD FOREIGN KEY (benchmark_run_id) REFERENCES benchmark_runs (id);
ALTER TABLE benchmark_run_measurements ADD FOREIGN KEY (measurement_id) REFERENCES measurements (id);
CREATE UNIQUE INDEX idx_uk_benchmark_measurements_mes_id ON benchmark_run_measurements (measurement_id);

ALTER TABLE benchmark_runs ADD FOREIGN KEY (environment_id) REFERENCES environments (id);
ALTER TABLE benchmark_runs_attributes ADD FOREIGN KEY (benchmark_run_id) REFERENCES benchmark_runs (id);
CREATE UNIQUE INDEX idx_uk_benchmarks_unique_name_seq_id ON benchmark_runs (unique_name, sequence_id);

CREATE UNIQUE INDEX idx_uk_environments_name ON environments (name);

ALTER TABLE environment_attributes ADD FOREIGN KEY (environment_id) REFERENCES environments (id);

ALTER TABLE executions ADD FOREIGN KEY (benchmark_run_id) REFERENCES benchmark_runs (id);
CREATE INDEX idx_executions_benchmark_run_id ON executions (benchmark_run_id);

ALTER TABLE execution_attributes ADD FOREIGN KEY (execution_id) REFERENCES executions (id);

ALTER TABLE execution_measurements ADD FOREIGN KEY (execution_id) REFERENCES executions (id);
ALTER TABLE execution_measurements ADD FOREIGN KEY (measurement_id) REFERENCES measurements (id);
CREATE UNIQUE INDEX idx_uk_execution_measurements_mes_id ON execution_measurements (measurement_id);


