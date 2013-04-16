package com.facebook.presto.metadata;

import com.facebook.presto.metadata.ShardManagerDao.Utils;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.util.SqlUtils.runIgnoringConstraintViolation;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

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
    public long allocateShard(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(tableHandle instanceof NativeTableHandle, "can only allocate shards for native tables");
        long tableId = ((NativeTableHandle) tableHandle).getTableId();
        return dao.insertShard(tableId, false);
    }

    @Override
    public void commitShard(final long shardId, String nodeIdentifier)
    {
        final long nodeId = getOrCreateNodeId(nodeIdentifier);
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
            {
                ShardManagerDao dao = handle.attach(ShardManagerDao.class);
                dao.commitShard(shardId);
                dao.insertShardNode(shardId, nodeId);
            }
        });
    }

    @Override
    public void commitPartition(TableHandle tableHandle, final String partition, final Map<Long, String> shards)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(tableHandle instanceof NativeTableHandle, "can only commit partitions for native tables");
        final long tableId = ((NativeTableHandle) tableHandle).getTableId();

        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
            {
                ShardManagerDao dao = handle.attach(ShardManagerDao.class);
                long partitionId = dao.insertPartition(tableId, partition);

                for (Map.Entry<Long, String> entry : shards.entrySet()) {
                    long nodeId = getOrCreateNodeId(entry.getValue());
                    long shardId = entry.getKey();
                    dao.commitShard(shardId);
                    dao.insertShardNode(shardId, nodeId);
                    dao.insertPartitionShard(shardId, tableId, partitionId);
                }
            }
        });
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
    public Set<String> getPartitions(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(tableHandle instanceof NativeTableHandle, "can only commit partitions for native tables");
        final long tableId = ((NativeTableHandle) tableHandle).getTableId();
        return dao.getPartitions(tableId);
    }

    @Override
    public Multimap<Long, String> getCommittedShardNodes(long tableId)
    {
        ImmutableMultimap.Builder<Long, String> map = ImmutableMultimap.builder();
        for (ShardNode shardNode : dao.getCommittedShardNodes(tableId)) {
            map.put(shardNode.getShardId(), shardNode.getNodeIdentifier());
        }
        return map.build();
    }

    @Override
    public Multimap<Long, String> getShardNodes(long tableId, String partitionName)
    {
        ImmutableMultimap.Builder<Long, String> map = ImmutableMultimap.builder();
        for (ShardNode shardNode : dao.getAllShardNodes(tableId, partitionName)) {
            map.put(shardNode.getShardId(), shardNode.getNodeIdentifier());
        }
        return map.build();
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
