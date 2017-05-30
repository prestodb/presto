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

import com.facebook.presto.raptorx.TransactionManager;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;

import javax.inject.Inject;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.metadata.IndexWriter.chunkIndexTable;
import static com.facebook.presto.raptorx.metadata.IndexWriter.dropIndexTable;
import static com.facebook.presto.raptorx.metadata.IndexWriter.dropIndexTableColumn;
import static com.facebook.presto.raptorx.metadata.ShardHashing.tableShard;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.partition;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class CommitCleaner
{
    private static final Duration FAILED_TRANSACTION_AGE = Duration.ofHours(1);
    private static final int CHUNK_BATCH_SIZE = 1000;

    private final TransactionManager transactionManager;
    private final Database.Type databaseType;
    private final MasterCommitCleanerDao masterDao;
    private final DeletedChunksDao deletedChunksDao;
    private final List<ShardCommitCleanerDao> shardDao;
    private final Clock clock;

    @Inject
    public CommitCleaner(TransactionManager transactionManager, Database database)
    {
        this(transactionManager, database, Clock.systemUTC());
    }

    public CommitCleaner(TransactionManager transactionManager, Database database, Clock clock)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.databaseType = requireNonNull(database.getType(), "database.type is null");

        this.masterDao = createJdbi(database.getMasterConnection())
                .onDemand(MasterCommitCleanerDao.class);

        this.deletedChunksDao = createDeletedChunksDao(database);

        this.shardDao = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .map(dbi -> dbi.onDemand(ShardCommitCleanerDao.class))
                .collect(toImmutableList());

        this.clock = requireNonNull(clock, "clock is null");
    }

    public synchronized void removeOldCommits()
    {
        long activeCommitId = transactionManager.oldestActiveCommitId();

        Set<Long> droppedTableIds = masterDao.getDroppedTableIds(activeCommitId);

        Map<Integer, Set<Long>> droppedTableIdsByShard = droppedTableIds.stream()
                .collect(groupingBy(tableId -> tableShard(tableId, shardDao.size()), toSet()));

        // drop index tables
        for (long tableId : droppedTableIds) {
            shardDao.get(tableShard(tableId, shardDao.size())).useHandle(handle ->
                    dropIndexTable(handle, tableId));
        }

        // cleanup deleted chunks
        long now = clock.millis();
        for (int i = 0; i < shardDao.size(); i++) {
            ShardCommitCleanerDao shard = shardDao.get(i);
            Set<Long> tableIds = droppedTableIdsByShard.getOrDefault(i, ImmutableSet.of());

            // cleanup chunks for old commits
            while (true) {
                List<TableChunk> chunks = shard.getDeletedChunks(activeCommitId, CHUNK_BATCH_SIZE);
                if (chunks.isEmpty()) {
                    break;
                }
                cleanupChunks(shard, now, chunks, tableIds);
            }

            // cleanup chunks for dropped tables
            if (!tableIds.isEmpty()) {
                while (true) {
                    List<TableChunk> chunks = shard.getDeletedChunks(tableIds, CHUNK_BATCH_SIZE);
                    if (chunks.isEmpty()) {
                        break;
                    }
                    cleanupChunks(shard, now, chunks, tableIds);
                }
            }
        }

        // drop index table columns
        for (TableColumn column : masterDao.getDroppedColumns(activeCommitId)) {
            shardDao.get(tableShard(column.getTableId(), shardDao.size())).useHandle(handle ->
                    dropIndexTableColumn(databaseType, handle, column.getTableId(), column.getColumnId()));
        }

        // cleanup columns for dropped tables
        if (!droppedTableIds.isEmpty()) {
            masterDao.cleanupDroppedTableColumns(droppedTableIds);
        }

        // cleanup schemas, tables, columns, views, commits
        masterDao.cleanup(activeCommitId);

        // cleanup table sizes
        for (ShardCommitCleanerDao dao : shardDao) {
            dao.cleanupTableSizes(activeCommitId);
        }

        // cleanup successful transactions
        Set<Long> transactionIds = masterDao.getSuccessfulTransactionIds();
        for (Iterable<Long> ids : partition(transactionIds, 1000)) {
            for (ShardCommitCleanerDao shard : shardDao) {
                shard.cleanupCreatedChunks(ids);
            }
            masterDao.cleanupTransactions(ids);
        }

        // abort failed transactions
        masterDao.abortTransactions(transactionManager.activeTransactions(), clock.millis());

        // cleanup failed transactions
        now = clock.millis();
        long maxEndTime = now - FAILED_TRANSACTION_AGE.toMillis();
        long purgeTime = now + FAILED_TRANSACTION_AGE.toMillis();
        transactionIds = masterDao.getFailedTransactions(maxEndTime);

        for (Iterable<Long> ids : partition(transactionIds, 1000)) {
            // move created chunks to deletion queue
            for (ShardCommitCleanerDao shard : shardDao) {
                while (true) {
                    List<TableChunk> chunks = shard.getCreatedChunks(ids, CHUNK_BATCH_SIZE);
                    if (chunks.isEmpty()) {
                        break;
                    }
                    deletedChunksDao.insertDeletedChunks(now, purgeTime, chunks);
                    shard.deleteCreatedChunks(toChunkIds(chunks));
                }
            }
            masterDao.cleanupTransactions(ids);
        }
    }

    private void cleanupChunks(ShardCommitCleanerDao shard, long now, List<TableChunk> chunks, Set<Long> droppedTableIds)
    {
        // queue for deletion
        deletedChunksDao.insertDeletedChunks(now, now, chunks);

        // delete from index table
        chunks.stream()
                .filter(chunk -> !droppedTableIds.contains(chunk.getTableId()))
                .collect(groupingBy(TableChunk::getTableId))
                .forEach((tableId, tableChunks) ->
                        shard.deleteIndexChunks(chunkIndexTable(tableId), toChunkIds(tableChunks)));

        // cleanup deleted chunks
        shard.deleteChunks(toChunkIds(chunks));
    }

    private static Set<Long> toChunkIds(Collection<TableChunk> tableChunks)
    {
        return tableChunks.stream()
                .map(TableChunk::getChunkId)
                .collect(toImmutableSet());
    }

    private static DeletedChunksDao createDeletedChunksDao(Database database)
    {
        Jdbi dbi = createJdbi(database.getMasterConnection());
        switch (database.getType()) {
            case H2:
            case MYSQL:
                return dbi.onDemand(MySqlDeletedChunksDao.class);
            case POSTGRESQL:
                return dbi.onDemand(PostgreSqlDeletedChunksDao.class);
        }
        throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Unhandled database: " + database.getType());
    }

    public interface DeletedChunksDao
    {
        void insertDeletedChunks(long deleteTime, long purgeTime, Iterable<TableChunk> chunks);
    }

    public interface MySqlDeletedChunksDao
            extends DeletedChunksDao
    {
        @Override
        @SqlBatch("INSERT IGNORE INTO deleted_chunks (chunk_id, size, delete_time, purge_time)\n" +
                "VALUES (:chunkId, :size, :deleteTime, :purgeTime)")
        void insertDeletedChunks(
                @Bind long deleteTime,
                @Bind long purgeTime,
                @BindBean Iterable<TableChunk> chunks);
    }

    public interface PostgreSqlDeletedChunksDao
            extends DeletedChunksDao
    {
        @Override
        @SqlBatch("INSERT INTO deleted_chunks (chunk_id, size, delete_time, purge_time)\n" +
                "VALUES (:chunkId, :size, :deleteTime, :purgeTime)\n" +
                "ON CONFLICT DO NOTHING\n")
        void insertDeletedChunks(
                @Bind long deleteTime,
                @Bind long purgeTime,
                @BindBean Iterable<TableChunk> chunks);
    }
}
