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
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import javax.inject.Inject;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS;
import static com.facebook.presto.raptor.metadata.ShardManagerDaoUtils.createShardTablesWithRetry;
import static com.facebook.presto.raptor.metadata.ShardPredicate.jdbcType;
import static com.facebook.presto.raptor.metadata.SqlUtils.runIgnoringConstraintViolation;
import static com.facebook.presto.raptor.storage.ShardStats.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

public class DatabaseShardManager
        implements ShardManager
{
    private static final String INDEX_TABLE_PREFIX = "x_shards_t";

    private final IDBI dbi;
    private final ShardManagerDao dao;

    private final LoadingCache<String, Long> nodeIdCache = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .build(new CacheLoader<String, Long>()
            {
                @Override
                public Long load(String nodeIdentifier)
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
                tableColumns +
                "  UNIQUE (shard_uuid)\n" +
                ")";

        dbi.withHandle(handle -> {
            handle.execute(sql);
            return null;
        });
    }

    @Override
    public void commitShards(long tableId, List<ColumnInfo> columns, Collection<ShardInfo> shards, Optional<String> externalBatchId)
    {
        // attempt to fail up front with a proper exception
        if (externalBatchId.isPresent() && dao.externalBatchExists(externalBatchId.get())) {
            throw new PrestoException(RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS, "External batch already exists: " + externalBatchId.get());
        }

        Set<String> identifiers = shards.stream()
                .map(ShardInfo::getNodeIdentifiers)
                .flatMap(Collection::stream)
                .collect(toSet());
        Map<String, Long> nodeIds = Maps.toMap(identifiers, this::getOrCreateNodeId);

        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws SQLException
            {
                ShardManagerDao dao = handle.attach(ShardManagerDao.class);

                try (IndexInserter indexInserter = new IndexInserter(handle.getConnection(), tableId, columns)) {
                    for (ShardInfo shard : shards) {
                        long shardId = dao.insertShard(shard.getShardUuid(), shard.getRowCount(), shard.getDataSize());
                        dao.insertTableShard(tableId, shardId);

                        for (String nodeIdentifier : shard.getNodeIdentifiers()) {
                            dao.insertShardNode(shardId, nodeIds.get(nodeIdentifier));
                        }

                        indexInserter.insert(shardId, shard.getShardUuid(), shard.getColumnStats());
                    }
                }

                if (externalBatchId.isPresent()) {
                    dao.insertExternalBatch(externalBatchId.get());
                }
            }
        });
    }

    @Override
    public CloseableIterator<ShardNodes> getShardNodes(long tableId, TupleDomain<RaptorColumnHandle> effectivePredicate)
    {
        return new ShardIterator(tableId, effectivePredicate, dbi.open().getConnection());
    }

    @Override
    public Set<UUID> getNodeShards(String nodeIdentifier)
    {
        return dao.getNodeShards(nodeIdentifier);
    }

    @Override
    public void dropTableShards(long tableId)
    {
        dao.dropTableShards(tableId);
    }

    @Override
    public void assignShard(UUID shardUuid, String nodeIdentifier)
    {
        long nodeId = getOrCreateNodeId(nodeIdentifier);

        // assigning a shard is idempotent
        runIgnoringConstraintViolation(() -> dao.insertShardNode(shardUuid, nodeId));
    }

    private long getOrCreateNodeId(String nodeIdentifier)
    {
        try {
            return nodeIdCache.getUnchecked(nodeIdentifier);
        }
        catch (UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private long loadNodeId(String nodeIdentifier)
    {
        Long id = dao.getNodeId(nodeIdentifier);
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
