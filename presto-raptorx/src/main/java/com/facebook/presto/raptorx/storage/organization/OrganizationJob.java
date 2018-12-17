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

import com.facebook.presto.raptorx.RaptorConnector;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
    private final RaptorConnector raptorConnector;

    public OrganizationJob(OrganizationSet organizationSet, ChunkCompactor compactor, RaptorConnector raptorConnector)
    {
        this.compactor = requireNonNull(compactor, "compactor is null");
        this.organizationSet = requireNonNull(organizationSet, "organizationSet is null");
        this.raptorConnector = requireNonNull(raptorConnector, "transactionManager is null");
    }

    @Override
    public void run()
    {
        try {
            runJob(organizationSet.getTableId(), organizationSet.getBucketNumber(), organizationSet.getChunkIds());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void runJob(long tableId, int bucketNumber, Set<Long> chunkIds)
            throws IOException
    {
        ConnectorTransactionHandle transactionHandle = raptorConnector.beginTransaction(IsolationLevel.REPEATABLE_READ, false);
        Transaction transaction = raptorConnector.getTransactionManager().get(transactionHandle);
        runJob(transaction, bucketNumber, tableId, chunkIds);
        raptorConnector.commit(transactionHandle);
    }

    private void runJob(Transaction transaction, int bucketNumber, long tableId, Set<Long> oldChunkIds)
            throws IOException
    {
        TableMetadata metadata = getTableMetadata(transaction, tableId);
        List<ChunkInfo> newChunks = performCompaction(transaction, bucketNumber, oldChunkIds, metadata);
        log.info("Compacted chunks %s into %s", oldChunkIds, newChunks.stream().map(ChunkInfo::getChunkId).collect(toList()));
        transaction.insertChunks(tableId, newChunks);
        transaction.deleteChunks(tableId, oldChunkIds);
    }

    private TableMetadata getTableMetadata(Transaction transaction, long tableId)
    {
        TableInfo tableInfo = transaction.getTableInfo(tableId);
        List<ColumnInfo> sortColumns = tableInfo.getSortColumns();

        List<Long> sortColumnIds = sortColumns.stream()
                .map(ColumnInfo::getColumnId)
                .collect(toList());

        List<ColumnInfo> columns = tableInfo.getColumns();
        return new TableMetadata(tableId, columns, sortColumnIds, tableInfo.getCompressionType());
    }

    private List<ChunkInfo> performCompaction(Transaction transaction, int bucketNumber, Set<Long> oldChunkIds, TableMetadata tableMetadata)
            throws IOException
    {
        if (tableMetadata.getSortColumnIds().isEmpty()) {
            return compactor.compact(transaction.getTransactionId(), tableMetadata.getTableId(), bucketNumber, oldChunkIds, tableMetadata.getColumns(), tableMetadata.getCompressionType());
        }
        return compactor.compactSorted(
                transaction.getTransactionId(),
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
