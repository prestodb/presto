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
import com.facebook.presto.raptor.util.DaoSupplier;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ExecutionError;
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
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.storage.ColumnIndexStatsUtils.jdbcType;
import static com.facebook.presto.raptor.storage.ShardStats.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayFromBytes;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayToBytes;
import static com.facebook.presto.raptor.util.DatabaseUtil.bindOptionalInt;
import static com.facebook.presto.raptor.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptor.util.DatabaseUtil.runIgnoringConstraintViolation;
import static com.facebook.presto.raptor.util.DatabaseUtil.runTransaction;
import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
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
            .build(new CacheLoader<String, Integer>()
            {
                @Override
                public Integer load(String nodeIdentifier)
                {
                    return loadNodeId(nodeIdentifier);
                }
            });

    private final LoadingCache<Long, Map<Integer, String>> bucketAssignmentsCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, SECONDS)
            .build(new CacheLoader<Long, Map<Integer, String>>()
            {
                @Override
                public Map<Integer, String> load(Long distributionId)
                {
                    return loadBucketAssignments(distributionId);
                }
            });

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

        createTablesWithRetry(dbi);
    }

    @Override
    public void createTable(long tableId, List<ColumnInfo> columns, boolean bucketed)
    {
        StringJoiner tableColumns = new StringJoiner(",\n  ", "  ", ",\n").setEmptyValue("");

        for (ColumnInfo column : columns) {
            String columnType = sqlColumnType(column.getType());
            if (columnType != null) {
                tableColumns.add(minColumn(column.getColumnId()) + " " + columnType);
                tableColumns.add(maxColumn(column.getColumnId()) + " " + columnType);
            }
        }

        String sql;
        if (bucketed) {
            sql = "" +
                    "CREATE TABLE " + shardIndexTable(tableId) + " (\n" +
                    "  shard_id BIGINT NOT NULL,\n" +
                    "  shard_uuid BINARY(16) NOT NULL,\n" +
                    "  bucket_number INT NOT NULL\n," +
                    tableColumns +
                    "  PRIMARY KEY (bucket_number, shard_uuid),\n" +
                    "  UNIQUE (shard_id),\n" +
                    "  UNIQUE (shard_uuid)\n" +
                    ")";
        }
        else {
            sql = "" +
                    "CREATE TABLE " + shardIndexTable(tableId) + " (\n" +
                    "  shard_id BIGINT NOT NULL PRIMARY KEY,\n" +
                    "  shard_uuid BINARY(16) NOT NULL,\n" +
                    "  node_ids VARBINARY(128) NOT NULL,\n" +
                    tableColumns +
                    "  UNIQUE (shard_uuid)\n" +
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

        try (Handle handle = dbi.open()) {
            handle.execute(sql);
        }
        catch (DBIException e) {
            throw metadataError(e);
        }
    }

    @Override
    public void commitShards(long transactionId, long tableId, List<ColumnInfo> columns, Collection<ShardInfo> shards, Optional<String> externalBatchId)
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
        });
    }

    @Override
    public void replaceShardUuids(long transactionId, long tableId, List<ColumnInfo> columns, Set<UUID> oldShardUuids, Collection<ShardInfo> newShards)
    {
        Map<String, Integer> nodeIds = toNodeIdMap(newShards);

        runCommit(transactionId, (handle) -> {
            lockTable(handle, tableId);
            for (List<ShardInfo> shards : partition(newShards, 1000)) {
                insertShardsAndIndex(tableId, columns, shards, nodeIds, handle);
            }
            for (List<UUID> uuids : partition(oldShardUuids, 1000)) {
                deleteShardsAndIndex(tableId, ImmutableSet.copyOf(uuids), handle);
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
                propagateIfInstanceOf(e.getCause(), PrestoException.class);
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

    private void deleteShardsAndIndex(long tableId, Set<UUID> shardUuids, Handle handle)
            throws SQLException
    {
        String args = Joiner.on(",").join(nCopies(shardUuids.size(), "?"));

        ImmutableSet.Builder<Long> shardIdSet = ImmutableSet.builder();

        String selectShardNodes = format(
                "SELECT shard_id FROM %s WHERE shard_uuid IN (%s)",
                shardIndexTable(tableId), args);

        try (PreparedStatement statement = handle.getConnection().prepareStatement(selectShardNodes)) {
            bindUuids(statement, shardUuids);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    shardIdSet.add(rs.getLong("shard_id"));
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
    public Set<ShardMetadata> getNodeShards(String nodeIdentifier)
    {
        return dao.getNodeShards(nodeIdentifier);
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
        catch (UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private Map<Integer, String> loadBucketAssignments(long distributionId)
    {
        Set<String> nodeIds = getNodeIdentifiers();
        Iterator<String> nodeIterator = cyclingShuffledIterator(nodeIds);

        ImmutableMap.Builder<Integer, String> assignments = ImmutableMap.builder();

        for (BucketNode bucketNode : dao.getBucketNodes(distributionId)) {
            int bucket = bucketNode.getBucketNumber();
            String nodeId = bucketNode.getNodeIdentifier();

            if (!nodeIds.contains(nodeId)) {
                if (nanosSince(startTime).compareTo(startupGracePeriod) < 0) {
                    throw new PrestoException(SERVER_STARTING_UP, "Cannot reassign buckets while server is starting");
                }
                assignmentLimiter.checkAssignFrom(nodeId);

                String oldNodeId = nodeId;
                // TODO: use smarter system to choose replacement node
                nodeId = nodeIterator.next();
                dao.updateBucketNode(distributionId, bucket, getOrCreateNodeId(nodeId));
                log.info("Reassigned bucket %s for distribution ID %s from %s to %s", bucket, distributionId, oldNodeId, nodeId);
            }

            assignments.put(bucket, nodeId);
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
        catch (UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
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
                "INSERT INTO shards (shard_uuid, table_id, create_time, row_count, compressed_size, uncompressed_size, bucket_number)\n" +
                "VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(sql, RETURN_GENERATED_KEYS)) {
            for (ShardInfo shard : shards) {
                statement.setBytes(1, uuidToBytes(shard.getShardUuid()));
                statement.setLong(2, tableId);
                statement.setLong(3, shard.getRowCount());
                statement.setLong(4, shard.getCompressedSize());
                statement.setLong(5, shard.getUncompressedSize());
                bindOptionalInt(statement, 6, shard.getBucketNumber());
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
}
