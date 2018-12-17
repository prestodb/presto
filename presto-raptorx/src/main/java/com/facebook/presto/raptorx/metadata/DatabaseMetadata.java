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

import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.util.CloseableIterator;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.raptorx.metadata.ShardHashing.tableShard;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.raptorx.util.DatabaseUtil.utf8Bytes;
import static com.facebook.presto.raptorx.util.DatabaseUtil.verifyMetadata;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Streams.stream;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;

public class DatabaseMetadata
        implements Metadata
{
    private final SequenceManager sequenceManager;
    private final ChunkSupplier chunkSupplier;
    private final MasterReaderDao dao;
    private final DistributionDao distributionDao;
    private final MasterTransactionDao masterTransactionDao;
    private final List<Jdbi> shardDbi;

    @Inject
    public DatabaseMetadata(SequenceManager sequenceManager, ChunkSupplier chunkSupplier, Database database, TypeManager typeManager)
    {
        this.sequenceManager = requireNonNull(sequenceManager, "sequenceManager is null");
        this.chunkSupplier = requireNonNull(chunkSupplier, "chunkSupplier is null");

        Jdbi dbi = createJdbi(database.getMasterConnection());
        dbi.registerRowMapper(new ColumnInfo.Mapper(typeManager));
        dbi.registerRowMapper(new DistributionInfo.Mapper(typeManager));
        this.dao = dbi.onDemand(MasterReaderDao.class);
        this.distributionDao = dbi.onDemand(DistributionDao.class);
        this.masterTransactionDao = dbi.onDemand(MasterTransactionDao.class);

        this.shardDbi = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .collect(toImmutableList());
    }

    @Override
    public long getCurrentCommitId()
    {
        return dao.getCurrentCommitId();
    }

    @Override
    public long nextTransactionId()
    {
        return sequenceManager.nextValue("transaction_id", 1000);
    }

    @Override
    public long nextSchemaId()
    {
        return sequenceManager.nextValue("schema_id", 10);
    }

    @Override
    public long nextTableId()
    {
        return sequenceManager.nextValue("table_id", 10);
    }

    @Override
    public long nextViewId()
    {
        return sequenceManager.nextValue("view_id", 10);
    }

    @Override
    public long nextColumnId()
    {
        return sequenceManager.nextValue("column_id", 100);
    }

    @Override
    public void registerTransaction(long transactionId)
    {
        masterTransactionDao.insertTransaction(transactionId, System.currentTimeMillis());
    }

    @Override
    public void registerTransactionTable(long transactionId, long tableId)
    {
        masterTransactionDao.insertTransactionTable(transactionId, System.currentTimeMillis());
    }

    @Override
    public long createDistribution(Optional<String> distributionName, List<Type> columnTypes, List<Long> bucketNodes)
    {
        long distributionId = sequenceManager.nextValue("distribution_id", 10);

        distributionDao.createDistribution(
                distributionId,
                utf8Bytes(distributionName),
                bucketNodes.size(),
                DistributionInfo.serializeColumnTypes(columnTypes));

        List<Integer> bucketNumbers = range(0, bucketNodes.size()).boxed().collect(toList());

        distributionDao.insertBucketNodes(distributionId, bucketNumbers, bucketNodes);

        return distributionId;
    }

    @Override
    public DistributionInfo getDistributionInfo(long distributionId)
    {
        DistributionInfo info = distributionDao.getDistributionInfo(distributionId);
        verifyMetadata(info != null, "Invalid distribution ID: %s", distributionId);
        return info;
    }

    @Override
    public Optional<DistributionInfo> getDistributionInfo(String distributionName)
    {
        return Optional.ofNullable(distributionDao.getDistributionInfo(distributionName.getBytes(UTF_8)));
    }

    @Override
    public Optional<Long> getSchemaId(long commitId, String schemaName)
    {
        return Optional.ofNullable(dao.getSchemaId(commitId, schemaName.getBytes(UTF_8)));
    }

    @Override
    public Optional<Long> getTableId(long commitId, long schemaId, String tableName)
    {
        return Optional.ofNullable(dao.getTableId(commitId, schemaId, tableName.getBytes(UTF_8)));
    }

    @Override
    public Optional<Long> getViewId(long commitId, long schemaId, String viewName)
    {
        return Optional.ofNullable(dao.getViewId(commitId, schemaId, viewName.getBytes(UTF_8)));
    }

    @Override
    public Collection<String> listSchemas(long commitId)
    {
        return dao.listSchemas(commitId).values().stream()
                .map(bytes -> new String(bytes, UTF_8))
                .collect(toImmutableList());
    }

    @Override
    public Collection<String> listTables(long commitId, long schemaId)
    {
        return dao.listTableNames(commitId, schemaId).stream()
                .map(bytes -> new String(bytes, UTF_8))
                .collect(toImmutableList());
    }

    @Override
    public Collection<String> listViews(long commitId, long schemaId)
    {
        return dao.listViewNames(commitId, schemaId).stream()
                .map(bytes -> new String(bytes, UTF_8))
                .collect(toImmutableList());
    }

    @Override
    public SchemaInfo getSchemaInfo(long commitId, long schemaId)
    {
        SchemaInfo info = dao.getSchemaInfo(commitId, schemaId);
        verifyMetadata(info != null, "Invalid schema ID: %s", schemaId);
        return info;
    }

    @Override
    public TableInfo getTableInfo(long commitId, long tableId)
    {
        TableInfo table = dao.getTableInfo(commitId, tableId);
        verifyMetadata(table != null, "Invalid table ID: %s", tableId);

        List<ColumnInfo> columns = dao.getColumnInfo(commitId, tableId);
        verifyMetadata(columns != null, "Invalid table ID: %s", tableId);
        verifyMetadata(!columns.isEmpty(), "No columns for table ID: %s", tableId);

        return table.builder()
                .setColumns(columns)
                .build();
    }

    @Override
    public ViewInfo getViewInfo(long commitId, long viewId)
    {
        ViewInfo view = dao.getViewInfo(commitId, viewId);
        verifyMetadata(view != null, "Invalid view ID: %s", viewId);
        return view;
    }

    @Override
    public Collection<TableStats> listTableStats(long commitId, Optional<Long> schemaId, Optional<Long> tableId)
    {
        Map<Long, String> schemas = dao.listSchemas(commitId).entrySet().stream()
                .collect(toMap(Map.Entry::getKey, value -> new String(value.getValue(), UTF_8)));

        List<Jdbi> filteredDbi = tableId
                .map(table -> singletonList(shardDbi.get(tableShard(table, shardDbi.size()))))
                .orElse(shardDbi);

        Map<Long, TableSize> tableSizes = new HashMap<>();
        for (Jdbi shard : filteredDbi) {
            shard.onDemand(ShardReaderDao.class)
                    .getTableSizes(commitId, tableId)
                    .forEach(size -> tableSizes.put(size.getTableId(), size));
        }

        return dao.listTableSummaries(commitId, schemaId, tableId).stream()
                .map(summary -> {
                    TableSize size = tableSizes.get(summary.getTableId());
                    return new TableStats(
                            schemas.get(summary.getSchemaId()),
                            summary.getTableName(),
                            summary.getCreateTime(),
                            summary.getUpdateTime(),
                            summary.getTableVersion(),
                            summary.getRowCount(),
                            size.getChunkCount(),
                            size.getCompressedSize(),
                            size.getUncompressedSize());
                })
                .collect(toImmutableList());
    }

    @Override
    public long getChunkRowCount(long commitId, long tableId, Set<Long> chunkIds)
    {
        try (Handle handle = shardDbi.get(tableShard(tableId, shardDbi.size())).open()) {
            ShardReaderDao dao = handle.attach(ShardReaderDao.class);
            return stream(partition(chunkIds, 1000))
                    .mapToLong(ids -> dao.getChunkRowCount(commitId, ids))
                    .sum();
        }
    }

    @Override
    public Collection<ChunkMetadata> getChunks(
            long commitId,
            long tableId,
            Collection<ChunkMetadata> addedChunks,
            Set<Long> deletedChunks)
    {
        return chunkSupplier.getChunks(commitId, tableId, addedChunks, deletedChunks);
    }

    @Override
    public CloseableIterator<BucketChunks> getBucketChunks(
            long commitId,
            long tableId,
            Collection<ChunkInfo> addedChunks,
            Set<Long> deletedChunks,
            TupleDomain<Long> constraint,
            boolean merged)
    {
        return chunkSupplier.getBucketChunks(
                commitId,
                tableId,
                addedChunks,
                deletedChunks,
                constraint,
                merged);
    }

    // test
    @Override
    public List<DistributionInfo> getActiveDistributions()
    {
        return distributionDao.listActiveDistributions();
    }

    @Override
    public long getDistributionSizeInBytes(long distributionId)
    {
        return distributionDao.getDistributionSizeInBytes(distributionId);
    }

    @Override
    public List<BucketNode> getBucketNodes(long distibutionId)
    {
        return distributionDao.getBucketNodes(distibutionId);
    }

    @Override
    public void updateBucketAssignment(long distributionId, int bucketNumber, long nodeId)
    {
        distributionDao.updateBucketNode(distributionId, bucketNumber, nodeId);
    }
}
