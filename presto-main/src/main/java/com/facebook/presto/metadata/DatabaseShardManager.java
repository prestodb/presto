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
package com.facebook.presto.metadata;

import com.facebook.presto.metadata.ShardManagerDao.Utils;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.split.NativePartitionKey;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.util.SqlUtils.runIgnoringConstraintViolation;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.immutableEntry;

public class DatabaseShardManager
        implements ShardManager
{
    private final IDBI dbi;
    private final ShardManagerDao dao;

    @Inject
    public DatabaseShardManager(@ForShardManager IDBI dbi)
            throws InterruptedException
    {
        this.dbi = dbi;
        this.dao = dbi.onDemand(ShardManagerDao.class);

        // keep retrying if database is unavailable when the server starts
        Utils.createShardTablesWithRetry(dao);
    }

    @Override
    public void commitPartition(TableHandle tableHandle, final String partition, final List<? extends PartitionKey> partitionKeys, final Map<UUID, String> shards)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(partition, "partition is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(shards, "shards is null");

        checkState(tableHandle instanceof NativeTableHandle, "can only commit partitions for native tables");
        final long tableId = ((NativeTableHandle) tableHandle).getTableId();

        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
            {
                ShardManagerDao dao = handle.attach(ShardManagerDao.class);
                long partitionId = dao.insertPartition(tableId, partition);

                for (PartitionKey partitionKey : partitionKeys) {
                    dao.insertPartitionKey(tableId, partition, partitionKey.getName(), partitionKey.getType().toString(), partitionKey.getValue());
                }

                for (Map.Entry<UUID, String> entry : shards.entrySet()) {
                    long nodeId = getOrCreateNodeId(entry.getValue());
                    UUID shardUuid = entry.getKey();
                    long shardId = dao.insertShard(shardUuid);
                    dao.insertShardNode(shardId, nodeId);
                    dao.insertPartitionShard(shardId, tableId, partitionId);
                }
            }
        });
    }

    @Override
    public void commitUnpartitionedTable(TableHandle tableHandle, Map<UUID, String> shards)
    {
        commitPartition(tableHandle, "<UNPARTITIONED>", ImmutableList.<PartitionKey>of(), shards);
    }

    @Override
    public void disassociateShard(long shardId, @Nullable String nodeIdentifier)
    {
        dao.dropShardNode(shardId, nodeIdentifier);
    }

    @Override
    public void dropShard(final long shardId)
    {
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
            {
                ShardManagerDao dao = handle.attach(ShardManagerDao.class);
                dao.deleteShardFromPartitionShards(shardId);
                dao.deleteShard(shardId);
            }
        });
    }

    @Override
    public Set<TablePartition> getPartitions(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(tableHandle instanceof NativeTableHandle, "can only commit partitions for native tables");
        final long tableId = ((NativeTableHandle) tableHandle).getTableId();
        return dao.getPartitions(tableId);
    }

    @Override
    public Multimap<String, ? extends PartitionKey> getAllPartitionKeys(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(tableHandle instanceof NativeTableHandle, "can only commit partitions for native tables");
        final long tableId = ((NativeTableHandle) tableHandle).getTableId();

        Set<NativePartitionKey> partitionKeys = dao.getPartitionKeys(tableId);
        ImmutableMultimap.Builder<String, PartitionKey> builder = ImmutableMultimap.builder();
        for (NativePartitionKey partitionKey : partitionKeys) {
            builder.put(partitionKey.getPartitionName(), partitionKey);
        }

        return builder.build();
    }

    @Override
    public Multimap<Long, Entry<UUID, String>> getShardNodesByPartition(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(tableHandle instanceof NativeTableHandle, "tableHandle not a native table");
        final long tableId = ((NativeTableHandle) tableHandle).getTableId();

        ImmutableMultimap.Builder<Long, Entry<UUID, String>> map = ImmutableMultimap.builder();
        for (ShardNode shardNode : dao.getShardNodes(tableId)) {
            map.put(shardNode.getPartitionId(), immutableEntry(shardNode.getShardUuid(), shardNode.getNodeIdentifier()));
        }
        return map.build();
    }

    @Override
    public Set<String> getTableNodes(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(tableHandle instanceof NativeTableHandle, "tableHandle not a native table");
        final long tableId = ((NativeTableHandle) tableHandle).getTableId();
        return ImmutableSet.copyOf(dao.getTableNodes(tableId));
    }

    @Override
    public Iterable<String> getAllNodesInUse()
    {
        return dao.getAllNodesInUse();
    }

    @Override
    public void dropPartition(final TableHandle tableHandle, final String partitionName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(tableHandle instanceof NativeTableHandle, "can only commit partitions for native tables");
        final long tableId = ((NativeTableHandle) tableHandle).getTableId();

        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws Exception
            {
                ShardManagerDao dao = handle.attach(ShardManagerDao.class);
                List<Long> shardIds = dao.getAllShards(tableId, partitionName);
                for (Long shardId : shardIds) {
                    dao.deleteShardFromPartitionShards(shardId);
                }
                dao.dropPartitionKeys(tableId, partitionName);
                dao.dropPartition(tableId, partitionName);
            }
        });
    }

    @Override
    public Iterable<Long> getOrphanedShardIds(Optional<String> nodeIdentifier)
    {
        if (nodeIdentifier.isPresent()) {
            return dao.getOrphanedShards(nodeIdentifier.get());
        }
        else {
            return dao.getAllOrphanedShards();
        }
    }

    @Override
    public void dropOrphanedPartitions()
    {
        dao.dropAllOrphanedPartitions();
    }

    private long getOrCreateNodeId(final String nodeIdentifier)
    {
        Long id = dao.getNodeId(nodeIdentifier);
        if (id != null) {
            return id;
        }

        // creating a node is idempotent
        runIgnoringConstraintViolation(new Runnable()
        {
            @Override
            public void run()
            {
                dao.insertNode(nodeIdentifier);
            }
        });

        id = dao.getNodeId(nodeIdentifier);
        if (id == null) {
            throw new IllegalStateException("node does not exist after insert");
        }
        return id;
    }
}
