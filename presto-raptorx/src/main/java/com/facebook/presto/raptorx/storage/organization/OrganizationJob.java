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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.TransactionManager;
import com.facebook.presto.raptorx.metadata.ChunkMetadata;
import com.facebook.presto.raptorx.metadata.ChunkSummary;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.IndexWriter;
import com.facebook.presto.raptorx.metadata.MetadataWriter;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.metadata.TableSize;
import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.raptorx.metadata.IndexWriter.chunkIndexTable;
import static com.facebook.presto.raptorx.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptorx.util.DatabaseUtil.verifyMetadata;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class OrganizationJob
        implements Runnable
{
    private static final Logger log = Logger.get(OrganizationJob.class);

    private final ChunkCompactor compactor;
    private final OrganizationSet organizationSet;
    private final MetadataWriter metadataWriter;
    private final TransactionManager transactionManager;
    private final long nodeId;

    public OrganizationJob(OrganizationSet organizationSet, ChunkCompactor compactor, MetadataWriter metadataWriter, TransactionManager transactionManager, long nodeId)
    {
        this.compactor = requireNonNull(compactor, "compactor is null");
        this.organizationSet = requireNonNull(organizationSet, "organizationSet is null");
        this.metadataWriter = requireNonNull(metadataWriter, "metadataWriter is null");
        this.transactionManager = requireNonNull(transactionManager, "sequenceManager is null");
        this.nodeId = nodeId;
    }

    @Override
    public void run()
    {
        try {
            runJob(organizationSet.getTableInfo(), organizationSet.getBucketNumber(), organizationSet.getChunkIds());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void runJob(TableInfo tableInfo, int bucketNumber, Set<Long> chunkIds)
            throws IOException
    {
        long tableId = tableInfo.getTableId();
        if (metadataWriter.isMaintenanceBlocked(tableId)) { // this check is for outside Mysql LOCK, otherwise it still contends the LOCK.
            throw new PrestoException(TRANSACTION_CONFLICT, "tableID " + tableId + " is blocked by DELETE");
        }

        ConnectorTransactionHandle transactionHandle = transactionManager.create();
        Transaction transaction = transactionManager.get(transactionHandle);
        metadataWriter.insertWorkerTransaction(transaction.getTransactionId(), nodeId);
        TableMetadata tableMetadata = getTableMetadata(tableInfo);
        try {
            List<ChunkInfo> newChunks = performCompaction(transaction.getTransactionId(), bucketNumber, chunkIds, tableMetadata);
            metadataWriter.runWorkerTransaction(tableId, (dao) -> {
                // 2 parties get this same LOCK. DatabaseMetadataWriter.writeShard(), and this.
                Long lockedTableId = dao.getLockedTableId(tableId);
                log.info("got LOCK, tableID: %s, chunks: %s", tableId, chunkIds);
                verifyMetadata(lockedTableId != null, " Cannot get LOCK for tableID " + lockedTableId + ", the table may not exist");

                if (metadataWriter.isMaintenanceBlocked(tableId)) { // here checks again, because the above check can be far away in time
                    throw new PrestoException(TRANSACTION_CONFLICT, "tableID " + tableId + " is blocked by DELETE");
                }

                TableSize tableSize = dao.getTableSize(tableId);
                if (tableSize == null) {
                    throw new PrestoException(TRANSACTION_CONFLICT, "tableID " + lockedTableId + " already changed.");
                }
                // An interesting data race to rule out here:
                // 1. DatabaseMetadataWriter.dropTable(), deleteTable() done;
                //        DatabaseMetadataWriter.dropTable(), deleteTableSize() not started yet;
                // 2. the compact happens,  chunkA+chunkB changes to chunkC;
                // 3. DatabaseMetadataWriter.dropTable(), deleteTableSize() done;
                // CommitCleaner cannot jump in between <1,2> or <2,3>, CommitCleaner can only happen after 3
                // because CommitCleaner only handles inActive transactions.
                // So when CommitCleaner later deals with this table, it can still deal with chunkC;

                // this commitId is just CurrentCommitID, and doesn't need to be a new or a larger value.
                long commitId = transaction.getCommitId();

                // There are 3 changing conditions:
                // 1. chunk changes;  2. table changes; 3. column changes;
                // 1 is what "dao.getChunkSummary" is for
                // 2 is handled in above codes
                // 3 doesn't matter, as long as the following is in one Mysql transaction, the new Chunks are as good as old ones.
                ChunkSummary summary = dao.getChunkSummary(chunkIds);
                if (summary.getChunkCount() != chunkIds.size()) {
                    throw new PrestoException(TRANSACTION_CONFLICT, "Chunks in this organize set are changed, abort, tableID:" + tableId);
                }

                long currentTime = System.currentTimeMillis();
                dao.insertChunks(commitId, tableId, currentTime, newChunks.stream()
                        .map(chunk -> ChunkMetadata.from(tableInfo, chunk))
                        .iterator());

                try (IndexWriter writer = new IndexWriter(dao.getHandle().getConnection(), commitId, tableId, tableMetadata.getColumns())) {
                    for (ChunkInfo chunk : newChunks) {
                        writer.add(chunk.getChunkId(), chunk.getBucketNumber(), chunk.getColumnStats());
                    }
                    writer.execute();
                }
                catch (SQLException e) {
                    throw metadataError(e);
                }

                // delete oldChunks
                dao.deleteChunks(commitId, chunkIds);
                dao.deleteIndexChunks(chunkIndexTable(tableId), commitId, chunkIds);

                // change tableStats
                long chunkCount = tableSize.getChunkCount();
                long compressedSize = tableSize.getCompressedSize();
                long uncompressedSize = tableSize.getUncompressedSize();

                chunkCount += newChunks.size() - summary.getChunkCount();
                compressedSize += newChunks.stream().mapToLong(ChunkInfo::getCompressedSize).sum() - summary.getCompressedSize();
                uncompressedSize += newChunks.stream().mapToLong(ChunkInfo::getUncompressedSize).sum() - summary.getUncompressedSize();

                // Don't INSERT and DELETE like in DatabaseMetadataWriter, to prevent too many rows.
                dao.updateTableSize(chunkCount, compressedSize, uncompressedSize, tableId);
            });
            metadataWriter.updateWorkerTransaction(true, transaction.getTransactionId(), nodeId);
            log.info("Compacted chunks %s into %s, tableID: %d", chunkIds, newChunks.stream().map(ChunkInfo::getChunkId).collect(toList()), tableId);
        }
        catch (Exception e) {
            metadataWriter.updateWorkerTransaction(false, transaction.getTransactionId(), nodeId);
            throw e;
        }
        finally {
            transactionManager.remove(transactionHandle);
        }
    }

    private TableMetadata getTableMetadata(TableInfo tableInfo)
    {
        List<ColumnInfo> sortColumns = tableInfo.getSortColumns();

        List<Long> sortColumnIds = sortColumns.stream()
                .map(ColumnInfo::getColumnId)
                .collect(toList());

        List<ColumnInfo> columns = tableInfo.getColumns();
        return new TableMetadata(tableInfo.getTableId(), columns, sortColumnIds, tableInfo.getCompressionType());
    }

    private List<ChunkInfo> performCompaction(long transactionId, int bucketNumber, Set<Long> oldChunkIds, TableMetadata tableMetadata)
            throws IOException
    {
        if (tableMetadata.getSortColumnIds().isEmpty()) {
            return compactor.compact(transactionId, tableMetadata.getTableId(), bucketNumber, oldChunkIds, tableMetadata.getColumns(), tableMetadata.getCompressionType());
        }
        return compactor.compactSorted(
                transactionId,
                tableMetadata.getTableId(),
                bucketNumber,
                oldChunkIds,
                tableMetadata.getColumns(),
                tableMetadata.getSortColumnIds(),
                nCopies(tableMetadata.getSortColumnIds().size(), ASC_NULLS_FIRST),
                tableMetadata.getCompressionType());
    }

    public class TableMetadata
    {
        private final long tableId;
        private final List<ColumnInfo> columns;
        private final List<Long> sortColumnIds;
        private final CompressionType compressionType;

        public TableMetadata(long tableId, List<ColumnInfo> columns, List<Long> sortColumnIds, CompressionType compressionType)
        {
            this.tableId = tableId;
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.sortColumnIds = ImmutableList.copyOf(requireNonNull(sortColumnIds, "sortColumnIds is null"));
            this.compressionType = requireNonNull(compressionType, "compressionType is null");
        }

        public long getTableId()
        {
            return tableId;
        }

        public List<ColumnInfo> getColumns()
        {
            return columns;
        }

        public List<Long> getSortColumnIds()
        {
            return sortColumnIds;
        }

        public CompressionType getCompressionType()
        {
            return compressionType;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableMetadata that = (TableMetadata) o;
            return Objects.equals(tableId, that.tableId) &&
                    Objects.equals(columns, that.columns) &&
                    Objects.equals(sortColumnIds, that.sortColumnIds) &&
                    Objects.equals(compressionType, that.compressionType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableId, columns, sortColumnIds, compressionType);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("tableId", tableId)
                    .add("columns", columns)
                    .add("sortColumnIds", sortColumnIds)
                    .add("compressionType", compressionType)
                    .toString();
        }
    }
}
