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
package com.facebook.presto.raptorx.metadata;

import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface MasterSchemaDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS sequences (\n" +
            "  sequence_name VARCHAR(50) PRIMARY KEY,\n" +
            "  next_value BIGINT NOT NULL\n" +
            ")")
    void createSequences();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS current_commit (\n" +
            "  singleton CHAR(1) PRIMARY KEY,\n" +
            "  commit_id BIGINT NOT NULL\n" +
            ")")
    void createCurrentCommit();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS active_commit (\n" +
            "  singleton CHAR(1) PRIMARY KEY,\n" +
            "  commit_id BIGINT NOT NULL,\n" +
            "  start_time BIGINT NOT NULL,\n" +
            "  rolling_back BOOLEAN NOT NULL,\n" +
            "  rollback_info LONGBLOB\n" +
            ")")
    void createActiveCommit();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS commits (\n" +
            "  commit_id BIGINT PRIMARY KEY,\n" +
            "  commit_time BIGINT NOT NULL\n" +
            ")")
    void createCommits();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS schemata (\n" +
            "  row_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  start_commit_id BIGINT NOT NULL,\n" +
            "  end_commit_id BIGINT,\n" +
            "  schema_id BIGINT,\n" +
            "  schema_name VARBINARY(512),\n" +
            "  UNIQUE (schema_id, start_commit_id, end_commit_id, row_id),\n" +
            "  UNIQUE (schema_name, start_commit_id, end_commit_id, row_id),\n" +
            "  UNIQUE (start_commit_id, end_commit_id, row_id, schema_name),\n" +
            "  UNIQUE (end_commit_id, row_id)\n" +
            ")")
    void createSchemata();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS tables (\n" +
            "  row_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  start_commit_id BIGINT NOT NULL,\n" +
            "  end_commit_id BIGINT,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  table_name VARBINARY(512) NOT NULL,\n" +
            "  schema_id BIGINT NOT NULL,\n" +
            "  distribution_id BIGINT NOT NULL,\n" +
            "  temporal_column_id BIGINT,\n" +
            "  organization_enabled BOOLEAN,\n" +
            "  compression_type VARCHAR(16) NOT NULL,\n" +
            "  create_time BIGINT NOT NULL,\n" +
            "  update_time BIGINT NOT NULL,\n" +
            "  row_count BIGINT NOT NULL,\n" +
            "  comment BLOB,\n" +
            "  UNIQUE (schema_id, start_commit_id, end_commit_id, row_id, table_name),\n" +
            "  UNIQUE (table_id, start_commit_id, end_commit_id, row_id),\n" +
            "  UNIQUE (table_name, start_commit_id, end_commit_id, row_id),\n" +
            "  UNIQUE (start_commit_id, row_id),\n" +
            "  UNIQUE (end_commit_id, row_id)\n" +
            ")")
    void createTables();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS columns (\n" +
            "  row_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  start_commit_id BIGINT NOT NULL,\n" +
            "  end_commit_id BIGINT,\n" +
            "  column_id BIGINT NOT NULL,\n" +
            "  column_name VARBINARY(512) NOT NULL,\n" +
            "  data_type VARBINARY(128) NOT NULL,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  ordinal_position INT NOT NULL,\n" +
            "  bucket_ordinal_position INT,\n" +
            "  sort_ordinal_position INT,\n" +
            "  comment BLOB,\n" +
            "  UNIQUE (table_id, start_commit_id, end_commit_id, row_id),\n" +
            "  UNIQUE (start_commit_id, row_id),\n" +
            "  UNIQUE (end_commit_id, row_id)\n" +
            ")")
    void createColumns();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS views (\n" +
            "  row_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  start_commit_id BIGINT NOT NULL,\n" +
            "  end_commit_id BIGINT,\n" +
            "  view_id BIGINT NOT NULL,\n" +
            "  view_name VARBINARY(512) NOT NULL,\n" +
            "  schema_id BIGINT NOT NULL,\n" +
            "  create_time BIGINT NOT NULL,\n" +
            "  update_time BIGINT NOT NULL,\n" +
            "  view_data MEDIUMBLOB NOT NULL,\n" +
            "  comment BLOB,\n" +
            "  UNIQUE (schema_id, start_commit_id, end_commit_id, row_id, view_name),\n" +
            "  UNIQUE (view_id, start_commit_id, end_commit_id, row_id),\n" +
            "  UNIQUE (view_name, start_commit_id, end_commit_id, row_id),\n" +
            "  UNIQUE (start_commit_id, row_id),\n" +
            "  UNIQUE (end_commit_id, row_id)\n" +
            ")")
    void createViews();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS nodes (\n" +
            "  node_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  identifier VARBINARY(100) NOT NULL,\n" +
            "  UNIQUE (identifier)\n" +
            ")")
    void createNodes();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS distributions (\n" +
            "  distribution_id BIGINT PRIMARY KEY,\n" +
            "  distribution_name VARBINARY(512),\n" +
            "  bucket_count INT NOT NULL,\n" +
            "  column_types BLOB NOT NULL,\n" +
            "  UNIQUE (distribution_name)\n" +
            ")")
    void createDistributions();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS bucket_nodes (\n" +
            "  distribution_id BIGINT NOT NULL,\n" +
            "  bucket_number INT NOT NULL,\n" +
            "  node_id BIGINT NOT NULL,\n" +
            "  PRIMARY KEY (distribution_id, bucket_number, node_id)\n" +
            ")")
    void createBucketNodes();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS transactions (\n" +
            "  transaction_id BIGINT PRIMARY KEY,\n" +
            "  successful BOOLEAN,\n" +
            "  start_time BIGINT NOT NULL,\n" +
            "  end_time BIGINT,\n" +
            "  UNIQUE (successful, start_time, transaction_id)\n" +
            ")")
    void createTransactions();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS transaction_tables (\n" +
            "  transaction_id BIGINT NOT NULL,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  UNIQUE (transaction_id, table_id)\n" +
            ")")
    void createTransactionTables();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS deleted_chunks (\n" +
            "  chunk_id BIGINT PRIMARY KEY,\n" +
            "  size BIGINT NOT NULL,\n" +
            "  delete_time BIGINT NOT NULL,\n" +
            "  purge_time BIGINT NOT NULL,\n" +
            "  UNIQUE (purge_time, chunk_id)\n" +
            ")")
    void createDeletedChunks();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS chunk_organizer_jobs (\n" +
            "  node_id BIGINT NOT NULL,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  last_start_time BIGINT,\n" +
            "  PRIMARY KEY (node_id, table_id),\n" +
            "  UNIQUE (table_id, node_id)\n" +
            ")")
    void createTableChunkOrganizerJobs();
}
