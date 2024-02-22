--This script first creates a database named presto_resource_groups and then
--creates the resource_groups_global_properties, resource_groups, and selectors tables within that database.
--It then inserts some example data into these tables.

--Please remember to replace 'user' with the actual username in your environment.
-- Also note that this is a simple example and may not cover all your use cases.
-- Always refer to the official PrestoDB documentation for the most accurate and up-to-date information.
CREATE DATABASE IF NOT EXISTS presto_resource_groups;
USE presto_resource_groups;

CREATE TABLE IF NOT EXISTS resource_groups_global_properties (
    name VARCHAR(128) NOT NULL PRIMARY KEY,
    value VARCHAR(512) NOT NULL,
    UNIQUE (name)
    );

INSERT INTO resource_groups_global_properties (name, value)
VALUES ('cpu_quota_period', '1h');

CREATE TABLE IF NOT EXISTS resource_groups (
                                               id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                               name VARCHAR(128) NOT NULL,
    soft_memory_limit VARCHAR(128) NOT NULL,
    hard_concurrency_limit INT NOT NULL,
    max_queued INT NOT NULL,
    jmx_export BOOLEAN NOT NULL,
    soft_cpu_limit VARCHAR(128),
    hard_cpu_limit VARCHAR(128),
    scheduling_policy VARCHAR(128),
    scheduling_weight INT,
    parent_id BIGINT,
    environment VARCHAR(128),
    UNIQUE (name, environment)
    );

INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, jmx_export)
VALUES ('global', '80%', 100, 1000, true);

INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, jmx_export, parent_id)
VALUES ('user', '50%', 50, 500, false, LAST_INSERT_ID());

CREATE TABLE IF NOT EXISTS selectors (
                                         id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                         resource_group_id BIGINT NOT NULL,
                                         user_regex VARCHAR(512),
    source_regex VARCHAR(512),
    query_type VARCHAR(512),
    priority INT NOT NULL,
    UNIQUE (resource_group_id, priority)
    );

INSERT INTO selectors (resource_group_id, user_regex, priority)
VALUES (LAST_INSERT_ID(), 'user', 1);