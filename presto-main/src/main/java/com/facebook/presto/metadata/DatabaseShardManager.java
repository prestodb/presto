package com.facebook.presto.metadata;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class DatabaseShardManager
        implements ShardManager
{
    private final IDBI dbi;
    private final ShardManagerDao dao;

    @Inject
    public DatabaseShardManager(@ForShardManager IDBI dbi)
    {
        this.dbi = dbi;
        this.dao = dbi.onDemand(ShardManagerDao.class);
        createTables();
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
    public List<Long> createImportPartition(final long tableId, final String partitionName, final List<SerializedPartitionChunk> partitionChunks)
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
    public Set<String> getImportedPartitions(long tableId)
    {
        return dao.getImportedPartitions(tableId);
    }

    @Override
    public Multimap<Long, String> getShardNodes(long tableId)
    {
        ImmutableMultimap.Builder<Long, String> map = ImmutableMultimap.builder();
        for (ShardNode sn : dao.getShardNodes(tableId)) {
            map.put(sn.getShardId(), sn.getNodeIdentifier());
        }
        return map.build();
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

    private void createTables()
    {
        dao.createTableNodes();
        dao.createTableShards();
        dao.createTableShardNodes();
        dao.createTableImportTables();
        dao.createTableImportPartitions();
        dao.createTableImportPartitionShards();
    }

    private static void runIgnoringConstraintViolation(Runnable runnable)
    {
        try {
            runnable.run();
        }
        catch (UnableToExecuteStatementException e) {
            if (e.getCause() instanceof SQLException) {
                String state = ((SQLException) e.getCause()).getSQLState();
                if (state.startsWith("23")) {
                    return;
                }
            }
            throw e;
        }
    }
}
