/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptor.metadata;

import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface PostgreSqlSchemaDao
        extends SchemaDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS distributions (\n" +
            "  distribution_id BIGSERIAL PRIMARY KEY,\n" +
            "  distribution_name VARCHAR(255),\n" +
            "  column_types TEXT NOT NULL,\n" +
            "  bucket_count INT NOT NULL,\n" +
            "  UNIQUE (distribution_name)\n" +
            ")")
    void createTableDistributions();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS tables (\n" +
            "  table_id BIGSERIAL PRIMARY KEY,\n" +
            "  schema_name VARCHAR(255) NOT NULL,\n" +
            "  table_name VARCHAR(255) NOT NULL,\n" +
            "  temporal_column_id BIGINT,\n" +
            "  compaction_enabled BOOLEAN NOT NULL,\n" +
            "  organization_enabled BOOLEAN NOT NULL,\n" +
            "  distribution_id BIGINT,\n" +
            "  create_time BIGINT NOT NULL,\n" +
            "  update_time BIGINT NOT NULL,\n" +
            "  table_version BIGINT NOT NULL,\n" +
            "  shard_count BIGINT NOT NULL,\n" +
            "  row_count BIGINT NOT NULL,\n" +
            "  compressed_size BIGINT NOT NULL,\n" +
            "  uncompressed_size BIGINT NOT NULL,\n" +
            "  maintenance_blocked TIMESTAMP,\n" +
            "  UNIQUE (schema_name, table_name),\n" +
            "  UNIQUE (distribution_id, table_id),\n" +
            "  UNIQUE (maintenance_blocked, table_id),\n" +
            "  FOREIGN KEY (distribution_id) REFERENCES distributions (distribution_id)\n" +
            ")")
    void createTableTables();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS nodes (\n" +
            "  node_id SERIAL PRIMARY KEY,\n" +
            "  node_identifier VARCHAR(255) NOT NULL,\n" +
            "  UNIQUE (node_identifier)\n" +
            ")")
    void createTableNodes();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS shards (\n" +
            "  shard_id BIGSERIAL PRIMARY KEY,\n" +
            "  shard_uuid BYTEA NOT NULL,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  bucket_number INT,\n" +
            "  create_time TIMESTAMP NOT NULL,\n" +
            "  row_count BIGINT NOT NULL,\n" +
            "  compressed_size BIGINT NOT NULL,\n" +
            "  uncompressed_size BIGINT NOT NULL,\n" +
            "  xxhash64 BIGINT NOT NULL,\n" +
            "  UNIQUE (shard_uuid),\n" +
            // include a covering index organized by table_id
            "  UNIQUE (table_id, bucket_number, shard_id, shard_uuid, create_time, row_count, compressed_size, uncompressed_size, xxhash64),\n" +
            "  FOREIGN KEY (table_id) REFERENCES tables (table_id)\n" +
            ")")
    void createTableShards();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS transactions (\n" +
            "  transaction_id BIGSERIAL PRIMARY KEY,\n" +
            "  successful BOOLEAN,\n" +
            "  start_time TIMESTAMP NOT NULL,\n" +
            "  end_time TIMESTAMP,\n" +
            "  UNIQUE (successful, start_time, transaction_id),\n" +
            "  UNIQUE (end_time, transaction_id, successful)\n" +
            ")")
    void createTableTransactions();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS created_shards (\n" +
            "  shard_uuid BYTEA NOT NULL,\n" +
            "  transaction_id BIGINT NOT NULL,\n" +
            "  PRIMARY KEY (shard_uuid),\n" +
            "  UNIQUE (transaction_id, shard_uuid),\n" +
            "  FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id)\n" +
            ")")
    void createTableCreatedShards();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS deleted_shards (\n" +
            "  shard_uuid BYTEA PRIMARY KEY,\n" +
            "  delete_time TIMESTAMP NOT NULL,\n" +
            "  UNIQUE (delete_time, shard_uuid)\n" +
            ")")
    void createTableDeletedShards();
}
