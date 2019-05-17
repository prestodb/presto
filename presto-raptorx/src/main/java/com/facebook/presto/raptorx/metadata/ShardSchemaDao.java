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

public interface ShardSchemaDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS aborted_commit (\n" +
            "  singleton CHAR(1) PRIMARY KEY,\n" +
            "  commit_id BIGINT NOT NULL\n" +
            ")")
    void createAbortedCommit();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS chunks (\n" +
            "  chunk_id BIGINT PRIMARY KEY,\n" +
            "  start_commit_id BIGINT NOT NULL,\n" +
            "  end_commit_id BIGINT,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  bucket_number INT NOT NULL,\n" +
            "  create_time BIGINT NOT NULL,\n" +
            "  row_count BIGINT NOT NULL,\n" +
            "  compressed_size BIGINT NOT NULL,\n" +
            "  uncompressed_size BIGINT NOT NULL,\n" +
            "  xxhash64 BIGINT NOT NULL,\n" +
            "  temporal_min BIGINT,\n" +
            "  temporal_max BIGINT,\n" +
            "  UNIQUE (table_id, bucket_number, chunk_id, compressed_size),\n" +
            "  UNIQUE (table_id, start_commit_id, end_commit_id, bucket_number, chunk_id, compressed_size, create_time, row_count, uncompressed_size, xxhash64, temporal_min, temporal_max),\n" +
            "  UNIQUE (start_commit_id, chunk_id),\n" +
            "  UNIQUE (end_commit_id, chunk_id)\n" +
            ")")
    void createChunks();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS table_sizes (\n" +
            "  row_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  start_commit_id BIGINT NOT NULL,\n" +
            "  end_commit_id BIGINT,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  chunk_count BIGINT NOT NULL,\n" +
            "  compressed_size BIGINT NOT NULL,\n" +
            "  uncompressed_size BIGINT NOT NULL,\n" +
            "  UNIQUE (table_id, start_commit_id, end_commit_id, row_id),\n" +
            "  UNIQUE (start_commit_id, row_id),\n" +
            "  UNIQUE (table_id, end_commit_id, row_id), \n" +
            "  UNIQUE (end_commit_id, row_id)\n" +
            ")")
    void createTableSizes();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS created_chunks (\n" +
            "  chunk_id BIGINT PRIMARY KEY,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  transaction_id BIGINT NOT NULL,\n" +
            "  size BIGINT NOT NULL,\n" +
            "  create_time BIGINT NOT NULL,\n" +
            "  UNIQUE (transaction_id, chunk_id, table_id, size)\n" +
            ")")
    void createCreatedChunks();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS worker_transactions (\n" +
            "  transaction_id BIGINT PRIMARY KEY,\n" +
            "  node_id BIGINT,\n" +
            "  successful BOOLEAN,\n" +
            "  start_time BIGINT NOT NULL,\n" +
            "  end_time BIGINT,\n" +
            "  UNIQUE (node_id, successful, transaction_id, end_time)\n" +
            ")")
    void createWorkerTransactions();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS maintenance (\n" +
            " table_id BIGINT PRIMARY KEY,\n" +
            " timestamp BIGINT\n" +
            ")")
    void createMaintenance();
}
