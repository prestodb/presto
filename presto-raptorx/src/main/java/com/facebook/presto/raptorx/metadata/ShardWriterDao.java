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

import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.customizer.Define;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.Iterator;
import java.util.Set;

public interface ShardWriterDao
        extends SqlObject
{
    // transaction

    @SqlQuery("SELECT commit_id FROM aborted_commit FOR UPDATE")
    Long getLockedAbortedCommitId();

    @SqlUpdate("UPDATE aborted_commit SET commit_id = :commitId")
    void updateAbortedCommitId(
            @Bind long commitId);

    default void rollback(long commitId)
    {
        rollbackCreatedChunks(commitId);
        rollbackDeletedChunks(commitId);

        rollbackCreatedTableSizes(commitId);
        rollbackDeletedTableSizes(commitId);
    }

    // chunk

    @SqlBatch("INSERT INTO chunks (\n" +
            "  start_commit_id,\n" +
            "  chunk_id,\n" +
            "  table_id,\n" +
            "  bucket_number,\n" +
            "  create_time,\n" +
            "  row_count,\n" +
            "  compressed_size,\n" +
            "  uncompressed_size,\n" +
            "  xxhash64,\n" +
            "  temporal_min,\n" +
            "  temporal_max)\n" +
            "VALUES (\n" +
            "  :commitId,\n" +
            "  :chunkId,\n" +
            "  :tableId,\n" +
            "  :bucketNumber,\n" +
            "  :createTime,\n" +
            "  :rowCount,\n" +
            "  :compressedSize,\n" +
            "  :uncompressedSize,\n" +
            "  :xxhash64,\n" +
            "  :temporalMin,\n" +
            "  :temporalMax)")
    void insertChunks(
            @Bind long commitId,
            @Bind long tableId,
            @Bind long createTime,
            @BindBean Iterator<ChunkMetadata> chunks);

    @SqlUpdate("UPDATE chunks SET end_commit_id = :commitId\n" +
            "WHERE chunk_id IN (<chunkIds>)")
    int deleteChunks(
            @Bind long commitId,
            @BindList Set<Long> chunkIds);

    @SqlUpdate("UPDATE <table> SET end_commit_id = :commitId\n" +
            "WHERE chunk_id IN (<chunkIds>)")
    int deleteIndexChunks(
            @Define String table,
            @Bind long commitId,
            @BindList Set<Long> chunkIds);

    @SqlUpdate("DELETE FROM chunks WHERE start_commit_id = :commitId")
    void rollbackCreatedChunks(
            @Bind long commitId);

    @SqlUpdate("UPDATE chunks SET end_commit_id = NULL\n" +
            "WHERE end_commit_id = :commitId")
    void rollbackDeletedChunks(
            @Bind long commitId);

    @SqlUpdate("DELETE FROM <table> WHERE start_commit_id = :commitId")
    void rollbackCreatedIndexChunks(
            @Define String table,
            @Bind long commitId);

    @SqlUpdate("UPDATE <table> SET end_commit_id = NULL\n" +
            "WHERE end_commit_id = :commitId")
    void rollbackDeletedIndexChunks(
            @Define String table,
            @Bind long commitId);

    @SqlQuery("SELECT\n" +
            "  count(*) AS chunk_count,\n" +
            "  sum(row_count) AS row_count,\n" +
            "  sum(compressed_size) AS compressed_size,\n" +
            "  sum(uncompressed_size) AS uncompressed_size\n" +
            "FROM chunks\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND chunk_id IN (<chunkIds>)")
    @UseRowMapper(ChunkSummary.Mapper.class)
    ChunkSummary getChunkSummary(
            @Bind long commitId,
            @BindList Set<Long> chunkIds);

    // table size

    @SqlUpdate("INSERT INTO table_sizes (\n" +
            "  start_commit_id,\n" +
            "  table_id,\n" +
            "  chunk_count,\n" +
            "  compressed_size,\n" +
            "  uncompressed_size)\n" +
            "VALUES (\n" +
            "  :commitId,\n" +
            "  :tableId,\n" +
            "  :chunkCount,\n" +
            "  :compressedSize,\n" +
            "  :uncompressedSize)")
    void insertTableSize(
            @Bind long commitId,
            @Bind long tableId,
            @Bind long chunkCount,
            @Bind long compressedSize,
            @Bind long uncompressedSize);

    @SqlUpdate("UPDATE table_sizes SET end_commit_id = :commitId\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND table_id = :tableId")
    int deleteTableSize(
            @Bind long commitId,
            @Bind long tableId);

    @SqlUpdate("DELETE FROM table_sizes WHERE start_commit_id = :commitId")
    void rollbackCreatedTableSizes(
            @Bind long commitId);

    @SqlUpdate("UPDATE table_sizes SET end_commit_id = NULL\n" +
            "WHERE end_commit_id = :commitId")
    void rollbackDeletedTableSizes(
            @Bind long commitId);

    @SqlQuery("SELECT *\n" +
            "FROM table_sizes\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND table_id = :tableId\n")
    @UseRowMapper(TableSize.Mapper.class)
    TableSize getTableSize(
            @Bind long tableId);
}
