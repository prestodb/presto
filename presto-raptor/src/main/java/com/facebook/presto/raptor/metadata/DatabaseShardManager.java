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

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.util.CloseableIterator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import javax.inject.Inject;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS;
import static com.facebook.presto.raptor.metadata.ShardManagerDaoUtils.createShardTablesWithRetry;
import static com.facebook.presto.raptor.metadata.ShardPredicate.jdbcType;
import static com.facebook.presto.raptor.metadata.SqlUtils.runIgnoringConstraintViolation;
import static com.facebook.presto.raptor.storage.ShardStats.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayFromBytes;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayToBytes;
import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toSet;

public class DatabaseShardManager
        implements ShardManager
{
    private static final String INDEX_TABLE_PREFIX = "x_shards_t";

    private static final Logger log = Logger.get(DatabaseShardManager.class);

    private final IDBI dbi;
    private final ShardManagerDao dao;

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

    @Inject
    public DatabaseShardManager(@ForMetadata IDBI dbi)
    {
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(ShardManagerDao.class);

        // keep retrying if database is unavailable when the server starts
        createShardTablesWithRetry(dao);
    }

    @Override
    public void createTable(long tableId, List<ColumnInfo> columns)
    {
        StringJoiner tableColumns = new StringJoiner(",\n  ", "  ", ",\n").setEmptyValue("");

        for (ColumnInfo column : columns) {
            String columnType = sqlColumnType(column.getType());
            if (columnType != null) {
                tableColumns.add(minColumn(column.getColumnId()) + " " + columnType);
                tableColumns.add(maxColumn(column.getColumnId()) + " " + columnType);
            }
        }

        String sql = "" +
                "CREATE TABLE " + shardIndexTable(tableId) + " (\n" +
                "  shard_id BIGINT NOT NULL PRIMARY KEY,\n" +
                "  shard_uuid BINARY(16) NOT NULL,\n" +
                "  node_ids VARBINARY(128) NOT NULL,\n" +
                tableColumns +
                "  UNIQUE (shard_uuid)\n" +
                ")";

        try (Handle handle = dbi.open()) {
            handle.execute(sql);
        }
    }

    @Override
    public void commitShards(long tableId, List<ColumnInfo> columns, Collection<ShardInfo> shards, Optional<String> externalBatchId)
    {
        // attempt to fail up front with a proper exception
        if (externalBatchId.isPresent() && dao.externalBatchExists(externalBatchId.get())) {
            throw new PrestoException(RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS, "External batch already exists: " + externalBatchId.get());
        }

        Map<String, Integer> nodeIds = toNodeIdMap(shards);

        dbi.inTransaction((handle, status) -> {
            ShardManagerDao dao = handle.attach(ShardManagerDao.class);

            insertShardsAndIndex(tableId, columns, shards, nodeIds, handle, dao);

            if (externalBatchId.isPresent()) {
                dao.insertExternalBatch(externalBatchId.get());
            }
            return null;
        });
    }

    @Override
    public void replaceShards(long tableId, List<ColumnInfo> columns, Set<Long> oldShardIds, Collection<ShardInfo> newShards)
    {
        Map<String, Integer> nodeIds = toNodeIdMap(newShards);

        runTransaction((handle, status) -> {
            ShardManagerDao dao = handle.attach(ShardManagerDao.class);
            insertShardsAndIndex(tableId, columns, newShards, nodeIds, handle, dao);
            deleteShardsAndIndex(tableId, oldShardIds, handle);
            return null;
        });
    }

    private static void deleteShardsAndIndex(long tableId, Set<Long> shardIds, Handle handle)
            throws SQLException
    {
        String args = Joiner.on(",").join(nCopies(shardIds.size(), "?"));
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
                    throw new PrestoException(TRANSACTION_CONFLICT, "Shard was updated by a different transaction. Please retry the operation.");
                }
            }
        }
    }

    private static void bindLongs(PreparedStatement statement, Set<Long> values)
            throws SQLException
    {
        int i = 1;
        for (long value : values) {
            statement.setLong(i, value);
            i++;
        }
    }

    private static void insertShardsAndIndex(long tableId, List<ColumnInfo> columns, Collection<ShardInfo> shards, Map<String, Integer> nodeIds, Handle handle, ShardManagerDao dao)
            throws SQLException
    {
        try (IndexInserter indexInserter = new IndexInserter(handle.getConnection(), tableId, columns)) {
            for (ShardInfo shard : shards) {
                long shardId = dao.insertShard(shard.getShardUuid(), tableId, shard.getRowCount(), shard.getCompressedSize(), shard.getUncompressedSize());
                Set<Integer> shardNodes = shard.getNodeIdentifiers().stream()
                        .map(nodeIds::get)
                        .collect(toSet());
                for (int nodeId : shardNodes) {
                    dao.insertShardNode(shardId, nodeId);
                }

                indexInserter.insert(shardId, shard.getShardUuid(), shardNodes, shard.getColumnStats());
            }
        }
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
    public Set<ShardMetadata> getNodeTableShards(String nodeIdentifier, long tableId)
    {
        return dao.getNodeTableShards(nodeIdentifier, tableId);
    }

    @Override
    public CloseableIterator<ShardNodes> getShardNodes(long tableId, TupleDomain<RaptorColumnHandle> effectivePredicate)
    {
        return new ShardIterator(tableId, effectivePredicate, dbi);
    }

    @Override
    public Set<UUID> getNodeShards(String nodeIdentifier)
    {
        return dao.getNodeShards(nodeIdentifier);
    }

    @Override
    public void dropTableShards(long tableId)
    {
        dbi.inTransaction((handle, status) -> {
            ShardManagerDao dao = handle.attach(ShardManagerDao.class);
            dao.dropShardNodes(tableId);
            dao.dropShards(tableId);
            return null;
        });

        try (Handle handle = dbi.open()) {
            handle.execute("DROP TABLE " + shardIndexTable(tableId));
        }
        catch (DBIException e) {
            log.warn(e, "Failed to drop table %s", shardIndexTable(tableId));
        }
    }

    @Override
    public void assignShard(long tableId, UUID shardUuid, String nodeIdentifier)
    {
        int nodeId = getOrCreateNodeId(nodeIdentifier);

        // assigning a shard is idempotent
        dbi.inTransaction((handle, status) -> runIgnoringConstraintViolation(() -> {
            ShardManagerDao dao = handle.attach(ShardManagerDao.class);
            dao.insertShardNode(shardUuid, nodeId);

            Set<Integer> nodeIds = ImmutableSet.<Integer>builder()
                    .addAll(fetchLockedNodeIds(handle, tableId, shardUuid))
                    .add(nodeId)
                    .build();
            updateNodeIds(handle, tableId, shardUuid, nodeIds);

            return null;
        }));
    }

    private <T> T runTransaction(TransactionCallback<T> callback)
    {
        try {
            return dbi.inTransaction(callback);
        }
        catch (DBIException e) {
            propagateIfInstanceOf(e.getCause(), PrestoException.class);
            throw new PrestoException(RAPTOR_ERROR, "Failed to perform metadata operation", e);
        }
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
            throw new PrestoException(INTERNAL_ERROR, "node does not exist after insert");
        }
        return id;
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

    static String shardIndexTable(long tableId)
    {
        return INDEX_TABLE_PREFIX + tableId;
    }

    public static String minColumn(long columnId)
    {
        return format("c%s_min", columnId);
    }

    public static String maxColumn(long columnId)
    {
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
}
