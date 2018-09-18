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

import com.facebook.presto.raptor.NodeSupplier;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.storage.organization.ShardOrganizerDao;
import com.facebook.presto.raptor.util.DaoSupplier;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.h2.jdbc.JdbcConnection;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.tweak.HandleConsumer;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS;
import static com.facebook.presto.raptor.storage.ColumnIndexStatsUtils.jdbcType;
import static com.facebook.presto.raptor.storage.ShardStats.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayFromBytes;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayToBytes;
import static com.facebook.presto.raptor.util.DatabaseUtil.bindOptionalInt;
import static com.facebook.presto.raptor.util.DatabaseUtil.isSyntaxOrAccessError;
import static com.facebook.presto.raptor.util.DatabaseUtil.isTransactionCacheFullError;
import static com.facebook.presto.raptor.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptor.util.DatabaseUtil.runIgnoringConstraintViolation;
import static com.facebook.presto.raptor.util.DatabaseUtil.runTransaction;
import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.Iterables.partition;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class DatabaseShardManager
        implements ShardManager
{
    private static final Logger log = Logger.get(DatabaseShardManager.class);

    private static final String INDEX_TABLE_PREFIX = "x_shards_t";
    private static final int MAX_ADD_COLUMN_ATTEMPTS = 100;

    private final IDBI dbi;
    private final DaoSupplier<ShardDao> shardDaoSupplier;
    private final ShardDao dao;
    private final NodeSupplier nodeSupplier;
    private final AssignmentLimiter assignmentLimiter;
    private final Ticker ticker;
    private final Duration startupGracePeriod;
    private final long startTime;

    private final LoadingCache<String, Integer> nodeIdCache = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .build(CacheLoader.from(this::loadNodeId));

    private final LoadingCache<Long, Map<Integer, String>> bucketAssignmentsCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, SECONDS)
            .build(CacheLoader.from(this::loadBucketAssignments));

    @Inject
    public DatabaseShardManager(
            @ForMetadata IDBI dbi,
            DaoSupplier<ShardDao> shardDaoSupplier,
            NodeSupplier nodeSupplier,
            AssignmentLimiter assignmentLimiter,
            Ticker ticker,
            MetadataConfig config)
    {
        this(dbi, shardDaoSupplier, nodeSupplier, assignmentLimiter, ticker, config.getStartupGracePeriod());
    }

    public DatabaseShardManager(
            IDBI dbi,
            DaoSupplier<ShardDao> shardDaoSupplier,
            NodeSupplier nodeSupplier,
            AssignmentLimiter assignmentLimiter,
            Ticker ticker,
            Duration startupGracePeriod)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.shardDaoSupplier = requireNonNull(shardDaoSupplier, "shardDaoSupplier is null");
        this.dao = shardDaoSupplier.onDemand();
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.assignmentLimiter = requireNonNull(assignmentLimiter, "assignmentLimiter is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.startupGracePeriod = requireNonNull(startupGracePeriod, "startupGracePeriod is null");
        this.startTime = ticker.read();
    }

    @Override
    public void createTable(long tableId, List<ColumnInfo> columns, boolean bucketed, OptionalLong temporalColumnId)
    {
        StringJoiner tableColumns = new StringJoiner(",\n  ", "  ", ",\n").setEmptyValue("");

        for (ColumnInfo column : columns) {
            String columnType = sqlColumnType(column.getType());
            if (columnType != null) {
                tableColumns.add(minColumn(column.getColumnId()) + " " + columnType);
                tableColumns.add(maxColumn(column.getColumnId()) + " " + columnType);
            }
        }

        StringJoiner coveringIndexColumns = new StringJoiner(", ");

        // Add the max temporal column first to accelerate queries that usually scan recent data
        temporalColumnId.ifPresent(id -> coveringIndexColumns.add(maxColumn(id)));
        temporalColumnId.ifPresent(id -> coveringIndexColumns.add(minColumn(id)));

        String sql;
        if (bucketed) {
            coveringIndexColumns
                    .add("bucket_number")
                    .add("shard_id")
                    .add("shard_uuid");

            sql = "" +
                    "CREATE TABLE " + shardIndexTable(tableId) + " (\n" +
                    "  shard_id BIGINT NOT NULL,\n" +
                    "  shard_uuid BINARY(16) NOT NULL,\n" +
                    "  bucket_number INT NOT NULL\n," +
                    tableColumns +
                    "  PRIMARY KEY (bucket_number, shard_uuid),\n" +
                    "  UNIQUE (shard_id),\n" +
                    "  UNIQUE (shard_uuid),\n" +
                    "  UNIQUE (" + coveringIndexColumns + ")\n" +
                    ")";
        }
        else {
            coveringIndexColumns
                    .add("node_ids")
                    .add("shard_id")
                    .add("shard_uuid");

            sql = "" +
                    "CREATE TABLE " + shardIndexTable(tableId) + " (\n" +
                    "  shard_id BIGINT NOT NULL,\n" +
                    "  shard_uuid BINARY(16) NOT NULL,\n" +
                    "  node_ids VARBINARY(128) NOT NULL,\n" +
                    tableColumns +
                    "  PRIMARY KEY (node_ids, shard_uuid),\n" +
                    "  UNIQUE (shard_id),\n" +
                    "  UNIQUE (shard_uuid),\n" +
                    "  UNIQUE (" + coveringIndexColumns + ")\n" +
                    ")";
        }

        try (Handle handle = dbi.open()) {
            handle.execute(sql);
        }
        catch (DBIException e) {
            throw metadataError(e);
        }
    }

    @Override
    public void dropTable(long tableId)
    {
        runTransaction(dbi, (handle, status) -> {
            lockTable(handle, tableId);

            ShardDao shardDao = shardDaoSupplier.attach(handle);
            shardDao.insertDeletedShards(tableId);
            shardDao.dropShardNodes(tableId);
            shardDao.dropShards(tableId);

            handle.attach(ShardOrganizerDao.class).dropOrganizerJobs(tableId);

            MetadataDao dao = handle.attach(MetadataDao.class);
            dao.dropColumns(tableId);
            dao.dropTable(tableId);
            return null;
        });

        // TODO: add a cleanup process for leftover index tables
        // It is not possible to drop the index tables in a transaction.
        try (Handle handle = dbi.open()) {
            handle.execute("DROP TABLE " + shardIndexTable(tableId));
        }
        catch (DBIException e) {
            log.warn(e, "Failed to drop index table %s", shardIndexTable(tableId));
        }
    }

    @Override
    public void addColumn(long tableId, ColumnInfo column)
    {
        String columnType = sqlColumnType(column.getType());
        if (columnType == null) {
            return;
        }

        String sql = format("ALTER TABLE %s ADD COLUMN (%s %s, %s %s)",
                shardIndexTable(tableId),
                minColumn(column.getColumnId()), columnType,
                maxColumn(column.getColumnId()), columnType);

        int attempts = 0;
        while (true) {
            attempts++;
            try (Handle handle = dbi.open()) {
                handle.execute(sql);
            }
            catch (DBIException e) {
                if (isSyntaxOrAccessError(e)) {
                    // exit when column already exists
                    return;
                }
                if (attempts >= MAX_ADD_COLUMN_ATTEMPTS) {
                    throw metadataError(e);
                }
                log.warn(e, "Failed to alter table on attempt %s, will retry. SQL: %s", attempts, sql);
                try {
                    SECONDS.sleep(3);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw metadataError(ie);
                }
            }
        }
    }

    @Override
    public void commitShards(long transactionId, long tableId, List<ColumnInfo> columns, Collection<ShardInfo> shards, Optional<String> externalBatchId, long updateTime)
    {
        // attempt to fail up front with a proper exception
        if (externalBatchId.isPresent() && dao.externalBatchExists(externalBatchId.get())) {
            throw new PrestoException(RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS, "External batch already exists: " + externalBatchId.get());
        }

        Map<String, Integer> nodeIds = toNodeIdMap(shards);

        runCommit(transactionId, (handle) -> {
            externalBatchId.ifPresent(shardDaoSupplier.attach(handle)::insertExternalBatch);
            lockTable(handle, tableId);
            insertShardsAndIndex(tableId, columns, shards, nodeIds, handle);

            ShardStats stats = shardStats(shards);
            MetadataDao metadata = handle.attach(MetadataDao.class);
            metadata.updateTableStats(tableId, shards.size(), stats.getRowCount(), stats.getCompressedSize(), stats.getUncompressedSize());
            metadata.updateTableVersion(tableId, updateTime);
        });
    }

    @Override
    public void replaceShardUuids(long transactionId, long tableId, List<ColumnInfo> columns, Set<UUID> oldShardUuids, Collection<ShardInfo> newShards, OptionalLong updateTime)
    {
        Map<String, Integer> nodeIds = toNodeIdMap(newShards);

        runCommit(transactionId, (handle) -> {
            lockTable(handle, tableId);

            if (!updateTime.isPresent() && handle.attach(MetadataDao.class).isMaintenanceBlockedLocked(tableId)) {
                throw new PrestoException(TRANSACTION_CONFLICT, "Maintenance is blocked for table");
            }

            ShardStats newStats = shardStats(newShards);
            long rowCount = newStats.getRowCount();
            long compressedSize = newStats.getCompressedSize();
            long uncompressedSize = newStats.getUncompressedSize();

            for (List<ShardInfo> shards : partition(newShards, 1000)) {
                insertShardsAndIndex(tableId, columns, shards, nodeIds, handle);
            }

            for (List<UUID> uuids : partition(oldShardUuids, 1000)) {
                ShardStats stats = deleteShardsAndIndex(tableId, ImmutableSet.copyOf(uuids), handle);
                rowCount -= stats.getRowCount();
                compressedSize -= stats.getCompressedSize();
                uncompressedSize -= stats.getUncompressedSize();
            }

            long shardCount = newShards.size() - oldShardUuids.size();

            if (!oldShardUuids.isEmpty() || !newShards.isEmpty()) {
                MetadataDao metadata = handle.attach(MetadataDao.class);
                metadata.updateTableStats(tableId, shardCount, rowCount, compressedSize, uncompressedSize);
                updateTime.ifPresent(time -> metadata.updateTableVersion(tableId, time));
            }
        });
    }

    private void runCommit(long transactionId, HandleConsumer callback)
    {
        int maxAttempts = 5;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                dbi.useTransaction((handle, status) -> {
                    ShardDao dao = shardDaoSupplier.attach(handle);
                    if (commitTransaction(dao, transactionId)) {
                        callback.useHandle(handle);
                        dao.deleteCreatedShards(transactionId);
                    }
                });
                return;
            }
            catch (DBIException e) {
                if (isTransactionCacheFullError(e)) {
                    throw metadataError(e, "Transaction too large");
                }

                if (e.getCause() != null) {
                    throwIfInstanceOf(e.getCause(), PrestoException.class);
                }

                if (attempt == maxAttempts) {
                    throw metadataError(e);
                }
                log.warn(e, "Failed to commit shards on attempt %d, will retry.", attempt);
                try {
                    SECONDS.sleep(multiplyExact(attempt, 2));
                }
                catch (InterruptedException ie) {
                    throw metadataError(ie);
                }
            }
        }
    }

    private static boolean commitTransaction(ShardDao dao, long transactionId)
    {
        if (dao.finalizeTransaction(transactionId, true) != 1) {
            if (TRUE.equals(dao.transactionSuccessful(transactionId))) {
                return false;
            }
            throw new PrestoException(TRANSACTION_CONFLICT, "Transaction commit failed. Please retry the operation.");
        }
        return true;
    }

    private ShardStats deleteShardsAndIndex(long tableId, Set<UUID> shardUuids, Handle handle)
            throws SQLException
    {
        String args = Joiner.on(",").join(nCopies(shardUuids.size(), "?"));

        ImmutableSet.Builder<Long> shardIdSet = ImmutableSet.builder();
        long rowCount = 0;
        long compressedSize = 0;
        long uncompressedSize = 0;

        String selectShards = format("" +
                "SELECT shard_id, row_count, compressed_size, uncompressed_size\n" +
                "FROM shards\n" +
                "WHERE shard_uuid IN (%s)", args);

        try (PreparedStatement statement = handle.getConnection().prepareStatement(selectShards)) {
            bindUuids(statement, shardUuids);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    shardIdSet.add(rs.getLong("shard_id"));
                    rowCount += rs.getLong("row_count");
                    compressedSize += rs.getLong("compressed_size");
                    uncompressedSize += rs.getLong("uncompressed_size");
                }
            }
        }

        Set<Long> shardIds = shardIdSet.build();
        if (shardIds.size() != shardUuids.size()) {
            throw transactionConflict();
        }

        ShardDao dao = shardDaoSupplier.attach(handle);
        dao.insertDeletedShards(shardUuids);

        String where = " WHERE shard_id IN (" + args + ")";
        String deleteFromShardNodes = "DELETE FROM shard_nodes " + where;
        String deleteFromShards = "DELETE FROM shards " + where;
        String deleteFromShardIndex = "DELETE FROM " + shardIndexTable(tableId) + where;

        try (PreparedStatement statement = handle.getConnection().prepareStatement(deleteFromShardNodes)) {
            bindLongs(statement, shardIds);
            statement.executeUpdate();
        }

        for (String sql : asList(deleteFromShards, deleteFromShardIndex)) {
            try (PreparedStatement statement = handle.getConnection().prepareStatement(sql)) {
                bindLongs(statement, shardIds);
                if (statement.executeUpdate() != shardIds.size()) {
                    throw transactionConflict();
                }
            }
        }

        return new ShardStats(rowCount, compressedSize, uncompressedSize);
    }

    private static void bindUuids(PreparedStatement statement, Iterable<UUID> uuids)
            throws SQLException
    {
        int i = 1;
        for (UUID uuid : uuids) {
            statement.setBytes(i, uuidToBytes(uuid));
            i++;
        }
    }

    private static void bindLongs(PreparedStatement statement, Iterable<Long> values)
            throws SQLException
    {
        int i = 1;
        for (long value : values) {
            statement.setLong(i, value);
            i++;
        }
    }

    private static void insertShardsAndIndex(long tableId, List<ColumnInfo> columns, Collection<ShardInfo> shards, Map<String, Integer> nodeIds, Handle handle)
            throws SQLException
    {
        if (shards.isEmpty()) {
            return;
        }
        boolean bucketed = shards.iterator().next().getBucketNumber().isPresent();

        Connection connection = handle.getConnection();
        try (IndexInserter indexInserter = new IndexInserter(connection, tableId, columns)) {
            for (List<ShardInfo> batch : partition(shards, batchSize(connection))) {
                List<Long> shardIds = insertShards(connection, tableId, batch);

                if (!bucketed) {
                    insertShardNodes(connection, nodeIds, shardIds, batch);
                }

                for (int i = 0; i < batch.size(); i++) {
                    ShardInfo shard = batch.get(i);
                    Set<Integer> shardNodes = shard.getNodeIdentifiers().stream()
                            .map(nodeIds::get)
                            .collect(toSet());
                    indexInserter.insert(
                            shardIds.get(i),
                            shard.getShardUuid(),
                            shard.getBucketNumber(),
                            shardNodes,
                            shard.getColumnStats());
                }
                indexInserter.execute();
            }
        }
    }

    private static int batchSize(Connection connection)
    {
        // H2 does not return generated keys properly
        // https://github.com/h2database/h2database/issues/156
        return (connection instanceof JdbcConnection) ? 1 : 1000;
    }

    private Map<String, Integer> toNodeIdMap(Collection<ShardInfo> shards)
    {
        Set<String> identifiers = shards.stream()
                .map(ShardInfo::getNodeIdentifiers)
                .flatMap(Collection::stream)
                .collect(toSet());
        return Maps.toMap(identifiers, this::getOrCreateNodeId);
    }

    @Override
    public ShardMetadata getShard(UUID shardUuid)
    {
        return dao.getShard(shardUuid);
    }

    @Override
    public Set<ShardMetadata> getNodeShards(String nodeIdentifier)
    {
        return dao.getNodeShards(nodeIdentifier, null);
    }

    @Override
    public Set<ShardMetadata> getNodeShards(String nodeIdentifier, long tableId)
    {
        return dao.getNodeShards(nodeIdentifier, tableId);
    }

    @Override
    public ResultIterator<BucketShards> getShardNodes(long tableId, TupleDomain<RaptorColumnHandle> effectivePredicate)
    {
        return new ShardIterator(tableId, false, Optional.empty(), effectivePredicate, dbi);
    }

    @Override
    public ResultIterator<BucketShards> getShardNodesBucketed(long tableId, boolean merged, Map<Integer, String> bucketToNode, TupleDomain<RaptorColumnHandle> effectivePredicate)
    {
        return new ShardIterator(tableId, merged, Optional.of(bucketToNode), effectivePredicate, dbi);
    }

    @Override
    public void assignShard(long tableId, UUID shardUuid, String nodeIdentifier, boolean gracePeriod)
    {
        if (gracePeriod && (nanosSince(startTime).compareTo(startupGracePeriod) < 0)) {
            throw new PrestoException(SERVER_STARTING_UP, "Cannot reassign shards while server is starting");
        }

        int nodeId = getOrCreateNodeId(nodeIdentifier);

        runTransaction(dbi, (handle, status) -> {
            ShardDao dao = shardDaoSupplier.attach(handle);

            Set<Integer> nodes = new HashSet<>(fetchLockedNodeIds(handle, tableId, shardUuid));
            if (nodes.add(nodeId)) {
                updateNodeIds(handle, tableId, shardUuid, nodes);
                dao.insertShardNode(shardUuid, nodeId);
            }

            return null;
        });
    }

    @Override
    public void unassignShard(long tableId, UUID shardUuid, String nodeIdentifier)
    {
        int nodeId = getOrCreateNodeId(nodeIdentifier);

        runTransaction(dbi, (handle, status) -> {
            ShardDao dao = shardDaoSupplier.attach(handle);

            Set<Integer> nodes = new HashSet<>(fetchLockedNodeIds(handle, tableId, shardUuid));
            if (nodes.remove(nodeId)) {
                updateNodeIds(handle, tableId, shardUuid, nodes);
                dao.deleteShardNode(shardUuid, nodeId);
            }

            return null;
        });
    }

    @Override
    public Map<String, Long> getNodeBytes()
    {
        return dao.getNodeSizes().stream()
                .collect(toMap(NodeSize::getNodeIdentifier, NodeSize::getSizeInBytes));
    }

    @Override
    public long beginTransaction()
    {
        return dao.insertTransaction();
    }

    @Override
    public void rollbackTransaction(long transactionId)
    {
        dao.finalizeTransaction(transactionId, false);
    }

    @Override
    public void createBuckets(long distributionId, int bucketCount)
    {
        Iterator<String> nodeIterator = cyclingShuffledIterator(getNodeIdentifiers());

        List<Integer> bucketNumbers = new ArrayList<>();
        List<Integer> nodeIds = new ArrayList<>();
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            bucketNumbers.add(bucket);
            nodeIds.add(getOrCreateNodeId(nodeIterator.next()));
        }

        runIgnoringConstraintViolation(() -> dao.insertBuckets(distributionId, bucketNumbers, nodeIds));
    }

    @Override
    public Map<Integer, String> getBucketAssignments(long distributionId)
    {
        try {
            return bucketAssignmentsCache.getUnchecked(distributionId);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    @Override
    public void updateBucketAssignment(long distributionId, int bucketNumber, String nodeId)
    {
        dao.updateBucketNode(distributionId, bucketNumber, getOrCreateNodeId(nodeId));
    }

    @Override
    public List<Distribution> getDistributions()
    {
        return dao.listActiveDistributions();
    }

    @Override
    public long getDistributionSizeInBytes(long distributionId)
    {
        return dao.getDistributionSizeBytes(distributionId);
    }

    @Override
    public List<BucketNode> getBucketNodes(long distibutionId)
    {
        return dao.getBucketNodes(distibutionId);
    }

    @Override
    public Set<UUID> getExistingShardUuids(long tableId, Set<UUID> shardUuids)
    {
        try (Handle handle = dbi.open()) {
            String args = Joiner.on(",").join(nCopies(shardUuids.size(), "?"));
            String selectShards = format(
                    "SELECT shard_uuid FROM %s WHERE shard_uuid IN (%s)",
                    shardIndexTable(tableId), args);

            ImmutableSet.Builder<UUID> existingShards = ImmutableSet.builder();
            try (PreparedStatement statement = handle.getConnection().prepareStatement(selectShards)) {
                bindUuids(statement, shardUuids);
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        existingShards.add(uuidFromBytes(rs.getBytes("shard_uuid")));
                    }
                }
            }
            return existingShards.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<BucketNode> getBuckets(long distributionId)
    {
        return dao.getBucketNodes(distributionId);
    }

    private Map<Integer, String> loadBucketAssignments(long distributionId)
    {
        Set<String> nodeIds = getNodeIdentifiers();
        List<BucketNode> bucketNodes = getBuckets(distributionId);
        BucketReassigner reassigner = new BucketReassigner(nodeIds, bucketNodes);

        ImmutableMap.Builder<Integer, String> assignments = ImmutableMap.builder();
        PrestoException limiterException = null;
        Set<String> offlineNodes = new HashSet<>();

        for (BucketNode bucketNode : bucketNodes) {
            int bucket = bucketNode.getBucketNumber();
            String nodeId = bucketNode.getNodeIdentifier();

            if (!nodeIds.contains(nodeId)) {
                if (nanosSince(startTime).compareTo(startupGracePeriod) < 0) {
                    throw new PrestoException(SERVER_STARTING_UP, "Cannot reassign buckets while server is starting");
                }

                try {
                    if (offlineNodes.add(nodeId)) {
                        assignmentLimiter.checkAssignFrom(nodeId);
                    }
                }
                catch (PrestoException e) {
                    if (limiterException == null) {
                        limiterException = e;
                    }
                    continue;
                }

                String oldNodeId = nodeId;
                nodeId = reassigner.getNextReassignmentDestination();
                dao.updateBucketNode(distributionId, bucket, getOrCreateNodeId(nodeId));
                log.info("Reassigned bucket %s for distribution ID %s from %s to %s", bucket, distributionId, oldNodeId, nodeId);
            }

            assignments.put(bucket, nodeId);
        }

        if (limiterException != null) {
            throw limiterException;
        }

        return assignments.build();
    }

    private Set<String> getNodeIdentifiers()
    {
        Set<String> nodeIds = nodeSupplier.getWorkerNodes().stream()
                .map(Node::getNodeIdentifier)
                .collect(toSet());
        if (nodeIds.isEmpty()) {
            throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available for bucket assignments");
        }
        return nodeIds;
    }

    private int getOrCreateNodeId(String nodeIdentifier)
    {
        try {
            return nodeIdCache.getUnchecked(nodeIdentifier);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private int loadNodeId(String nodeIdentifier)
    {
        Integer id = dao.getNodeId(nodeIdentifier);
        if (id != null) {
            return id;
        }

        // creating a node is idempotent
        runIgnoringConstraintViolation(() -> dao.insertNode(nodeIdentifier));

        id = dao.getNodeId(nodeIdentifier);
        if (id == null) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "node does not exist after insert");
        }
        return id;
    }

    private Duration nanosSince(long nanos)
    {
        return new Duration(ticker.read() - nanos, NANOSECONDS);
    }

    private static List<Long> insertShards(Connection connection, long tableId, List<ShardInfo> shards)
            throws SQLException
    {
        String sql = "" +
                "INSERT INTO shards (shard_uuid, table_id, create_time, row_count, compressed_size, uncompressed_size, xxhash64, bucket_number)\n" +
                "VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(sql, RETURN_GENERATED_KEYS)) {
            for (ShardInfo shard : shards) {
                statement.setBytes(1, uuidToBytes(shard.getShardUuid()));
                statement.setLong(2, tableId);
                statement.setLong(3, shard.getRowCount());
                statement.setLong(4, shard.getCompressedSize());
                statement.setLong(5, shard.getUncompressedSize());
                statement.setLong(6, shard.getXxhash64());
                bindOptionalInt(statement, 7, shard.getBucketNumber());
                statement.addBatch();
            }
            statement.executeBatch();

            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            try (ResultSet keys = statement.getGeneratedKeys()) {
                while (keys.next()) {
                    builder.add(keys.getLong(1));
                }
            }
            List<Long> shardIds = builder.build();

            if (shardIds.size() != shards.size()) {
                throw new PrestoException(RAPTOR_ERROR, "Wrong number of generated keys for inserted shards");
            }
            return shardIds;
        }
    }

    private static void insertShardNodes(Connection connection, Map<String, Integer> nodeIds, List<Long> shardIds, List<ShardInfo> shards)
            throws SQLException
    {
        checkArgument(shardIds.size() == shards.size(), "lists are not the same size");
        String sql = "INSERT INTO shard_nodes (shard_id, node_id) VALUES (?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < shards.size(); i++) {
                for (String identifier : shards.get(i).getNodeIdentifiers()) {
                    statement.setLong(1, shardIds.get(i));
                    statement.setInt(2, nodeIds.get(identifier));
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        }
    }

    private static Collection<Integer> fetchLockedNodeIds(Handle handle, long tableId, UUID shardUuid)
    {
        String sql = format(
                "SELECT node_ids FROM %s WHERE shard_uuid = ? FOR UPDATE",
                shardIndexTable(tableId));

        byte[] nodeArray = handle.createQuery(sql)
                .bind(0, uuidToBytes(shardUuid))
                .map(ByteArrayMapper.FIRST)
                .first();

        return intArrayFromBytes(nodeArray);
    }

    private static void updateNodeIds(Handle handle, long tableId, UUID shardUuid, Set<Integer> nodeIds)
    {
        String sql = format(
                "UPDATE %s SET node_ids = ? WHERE shard_uuid = ?",
                shardIndexTable(tableId));

        handle.execute(sql, intArrayToBytes(nodeIds), uuidToBytes(shardUuid));
    }

    private static void lockTable(Handle handle, long tableId)
    {
        if (handle.attach(MetadataDao.class).getLockedTableId(tableId) == null) {
            throw transactionConflict();
        }
    }

    private static PrestoException transactionConflict()
    {
        return new PrestoException(TRANSACTION_CONFLICT, "Table was updated by a different transaction. Please retry the operation.");
    }

    public static String shardIndexTable(long tableId)
    {
        return INDEX_TABLE_PREFIX + tableId;
    }

    public static String minColumn(long columnId)
    {
        checkArgument(columnId >= 0, "invalid columnId %s", columnId);
        return format("c%s_min", columnId);
    }

    public static String maxColumn(long columnId)
    {
        checkArgument(columnId >= 0, "invalid columnId %s", columnId);
        return format("c%s_max", columnId);
    }

    private static String sqlColumnType(Type type)
    {
        JDBCType jdbcType = jdbcType(type);
        if (jdbcType != null) {
            switch (jdbcType) {
                case BOOLEAN:
                    return "boolean";
                case BIGINT:
                    return "bigint";
                case DOUBLE:
                    return "double";
                case INTEGER:
                    return "int";
                case VARBINARY:
                    return format("varbinary(%s)", MAX_BINARY_INDEX_SIZE);
            }
        }
        return null;
    }

    private static <T> Iterator<T> cyclingShuffledIterator(Collection<T> collection)
    {
        List<T> list = new ArrayList<>(collection);
        Collections.shuffle(list);
        return Iterables.cycle(list).iterator();
    }

    private static ShardStats shardStats(Collection<ShardInfo> shards)
    {
        return new ShardStats(
                shards.stream().mapToLong(ShardInfo::getRowCount).sum(),
                shards.stream().mapToLong(ShardInfo::getCompressedSize).sum(),
                shards.stream().mapToLong(ShardInfo::getUncompressedSize).sum());
    }

    private static class ShardStats
    {
        private final long rowCount;
        private final long compressedSize;
        private final long uncompressedSize;

        public ShardStats(long rowCount, long compressedSize, long uncompressedSize)
        {
            this.rowCount = rowCount;
            this.compressedSize = compressedSize;
            this.uncompressedSize = uncompressedSize;
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public long getCompressedSize()
        {
            return compressedSize;
        }

        public long getUncompressedSize()
        {
            return uncompressedSize;
        }
    }
}
