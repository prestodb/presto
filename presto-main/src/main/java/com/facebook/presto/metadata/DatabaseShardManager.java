package com.facebook.presto.metadata;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.SqlUtils.runIgnoringConstraintViolation;
import static com.google.common.base.Preconditions.checkState;

public class DatabaseShardManager
        implements ShardManager
{
    private static final Logger log = Logger.get(DatabaseShardManager.class);

    private final IDBI dbi;
    private final ShardManagerDao dao;

    @Inject
    public DatabaseShardManager(@ForShardManager IDBI dbi)
            throws InterruptedException
    {
        this.dbi = dbi;
        this.dao = dbi.onDemand(ShardManagerDao.class);

        // keep retrying if database is unavailable when the server starts
        createTablesWithRetry();
    }

    @Override
    public void createImportTable(final long tableId, final String sourceName, final String databaseName, final String tableName)
    {
        // creating a table is idempotent
        runIgnoringConstraintViolation(new Runnable()
        {
            @Override
            public void run()
            {
                dao.insertImportTable(tableId, sourceName, databaseName, tableName);
            }
        });
        checkState(dao.importTableExists(tableId), "import table does not exist after insert");
    }

    @Override
    public List<Long> createImportPartition(final long tableId, final String partitionName, final Iterable<SerializedPartitionChunk> partitionChunks)
    {
        return dbi.inTransaction(new TransactionCallback<List<Long>>()
        {
            @Override
            public List<Long> inTransaction(Handle handle, TransactionStatus status)
            {
                ShardManagerDao dao = handle.attach(ShardManagerDao.class);
                ImmutableList.Builder<Long> shardIds = ImmutableList.builder();
                long importPartitionId = dao.insertImportPartition(tableId, partitionName);
                for (SerializedPartitionChunk chunk : partitionChunks) {
                    long shardId = dao.insertShard(tableId, false);
                    dao.insertImportPartitionShard(importPartitionId, shardId, chunk.getBytes());
                    shardIds.add(shardId);
                }
                return shardIds.build();
            }
        });
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
                dao.deleteShardFromImportPartitionShards(shardId);
                dao.dropShardNode(shardId, null);
                dao.deleteShard(shardId);
            }
        });
    }

    @Override
    public Set<String> getAllPartitions(long tableId)
    {
        return dao.getAllPartitions(tableId);
    }

    @Override
    public Set<String> getCommittedPartitions(long tableId)
    {
        return dao.getCommittedPartitions(tableId);
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
    public void dropPartition(final long tableId, final String partitionName)
    {
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws Exception
            {
                ShardManagerDao dao = handle.attach(ShardManagerDao.class);
                List<Long> shardIds = dao.getAllShards(tableId, partitionName);
                for (Long shardId : shardIds) {
                    dao.deleteShardFromShardNodes(shardId);
                    dao.deleteShardFromImportPartitionShards(shardId);
                    dao.deleteShard(shardId);
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

    private void createTablesWithRetry()
            throws InterruptedException
    {
        Duration delay = new Duration(10, TimeUnit.SECONDS);
        while (true) {
            try {
                createTables();
                return;
            }
            catch (UnableToObtainConnectionException e) {
                log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                Thread.sleep((long) delay.toMillis());
            }
        }
    }

    private void createTables()
    {
        dao.createTableNodes();
        dao.createTableShards();
        dao.createTableShardNodes();
        dao.createTableImportTables();
        dao.createTableImportPartitions();
        dao.createTableImportPartitionShards();
    }
}
