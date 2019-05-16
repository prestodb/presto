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
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.customizer.Define;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface ShardCommitCleanerDao
        extends SqlObject
{
    // chunks

    @SqlQuery("SELECT table_id, chunk_id, compressed_size AS size\n" +
            "FROM chunks\n" +
            "WHERE end_commit_id <= :activeCommitId\n" +
            "LIMIT <limit>")
    @UseRowMapper(TableChunk.Mapper.class)
    List<TableChunk> getDeletedChunks(
            @Bind long activeCommitId,
            @Define int limit);

    @SqlQuery("SELECT table_id, chunk_id, compressed_size AS size\n" +
            "FROM chunks\n" +
            "WHERE table_id IN (<tableIds>)\n" +
            "LIMIT <limit>")
    @UseRowMapper(TableChunk.Mapper.class)
    List<TableChunk> getDeletedChunks(
            @BindList Set<Long> tableIds,
            @Define int limit);

    @SqlQuery("SELECT chunk_id\n" +
            "FROM chunks\n" +
            "WHERE chunk_id IN (<chunkIds>)")
    Set<Long> getExistChunks(@BindList Set<Long> chunkIds);

    @SqlUpdate("DELETE FROM chunks WHERE chunk_id IN (<chunkIds>)")
    void deleteChunks(
            @BindList Set<Long> chunkIds);

    @SqlUpdate("DELETE FROM <table> WHERE chunk_id IN (<chunkIds>)")
    void deleteIndexChunks(
            @Define String table,
            @BindList Set<Long> chunkIds);

    // sizes

    @SqlQuery("SELECT row_id\n" +
            "FROM table_sizes\n" +
            "WHERE end_commit_id <= :activeCommitId\n" +
            "LIMIT <limit>")
    List<Long> getDeletedTableSize(
            @Bind long activeCommitId,
            @Define int limit);

    @SqlUpdate("DELETE FROM table_sizes WHERE row_id IN (<rowIds>)")
    void cleanupTableSizes(@BindList List<Long> rowIds);

    // created chunks

    @SqlUpdate("DELETE FROM created_chunks WHERE transaction_id IN (<transactionIds>)")
    void cleanupCreatedChunks(
            @BindList Iterable<Long> transactionIds);

    @SqlQuery("SELECT table_id, chunk_id, size\n" +
            "FROM created_chunks\n" +
            "WHERE transaction_id IN (<transactionIds>)\n" +
            "LIMIT <limit>")
    @UseRowMapper(TableChunk.Mapper.class)
    List<TableChunk> getCreatedChunks(
            @BindList Iterable<Long> transactionIds,
            @Define int limit);

    @SqlUpdate("DELETE FROM created_chunks WHERE chunk_id IN (<chunkIds>)")
    void deleteCreatedChunks(
            @BindList Set<Long> chunkIds);

    // worker transactions

    default void abortWorkerTransactions(Collection<Long> excludedTransactionIds, long endTime, long nodeId)
    {
        if (excludedTransactionIds.isEmpty()) {
            doAbortWorkerTransactions(endTime, nodeId);
        }
        else {
            doAbortWorkerTransactions(excludedTransactionIds, endTime, nodeId);
        }
    }

    @SqlUpdate("UPDATE worker_transactions SET successful = FALSE, end_time = :endTime\n" +
            "WHERE successful IS NULL AND node_id = :nodeId")
    void doAbortWorkerTransactions(@Bind long endTime, @Bind long nodeId);

    @SqlUpdate("UPDATE worker_transactions SET successful = FALSE, end_time = :endTime\n" +
            "WHERE successful IS NULL\n" +
            "  AND transaction_id NOT IN (<excludedTransactionIds>)\n" +
            "  AND node_id = :nodeId")
    void doAbortWorkerTransactions(
            @BindList Iterable<Long> excludedTransactionIds,
            @Bind long endTime,
            @Bind long nodeId);

    @SqlQuery("SELECT transaction_id FROM worker_transactions WHERE successful = TRUE AND node_id = :nodeId")
    Set<Long> getSuccessfulWorkerTransactionIds(@Bind long nodeId);

    @SqlUpdate("DELETE FROM worker_transactions WHERE transaction_id IN (<transactionIds>)")
    void cleanupWorkerTransactions(@BindList Iterable<Long> transactionIds);

    // only for uni-testing purpose
    @SqlQuery("SELECT transaction_id FROM worker_transactions")
    Set<Long> getAllWorkerTransactions();

    @SqlQuery("SELECT transaction_id\n" +
            "FROM worker_transactions\n" +
            "WHERE successful = FALSE\n" +
            " AND node_id = :nodeId")
    Set<Long> getFailedWorkerTransactions(@Bind long nodeId);

    @SqlQuery("SELECT transaction_id\n" +
            "FROM worker_transactions\n" +
            "WHERE successful = TRUE\n" +
            "  AND end_time <= :maxEndTime")
    Set<Long> getOldestSuccessfulWorkerTransactions(@Bind long maxEndTime);

    @SqlQuery("SELECT transaction_id\n" +
            "FROM worker_transactions\n" +
            "WHERE successful = FALSE\n" +
            "  AND end_time <= :maxEndTime")
    Set<Long> getOldestFailedWorkerTransactions(@Bind long maxEndTime);
}
