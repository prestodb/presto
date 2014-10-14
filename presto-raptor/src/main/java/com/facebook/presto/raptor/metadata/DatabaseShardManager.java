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

import com.facebook.presto.spi.PrestoException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.raptor.metadata.ShardManagerDaoUtils.createShardTablesWithRetry;
import static com.facebook.presto.raptor.metadata.SqlUtils.runIgnoringConstraintViolation;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkNotNull;

public class DatabaseShardManager
        implements ShardManager
{
    private final IDBI dbi;
    private final ShardManagerDao dao;

    @Inject
    public DatabaseShardManager(@ForMetadata IDBI dbi)
    {
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(ShardManagerDao.class);

        // keep retrying if database is unavailable when the server starts
        createShardTablesWithRetry(dao);
    }

    @Override
    public void commitTable(final long tableId, final Iterable<ShardNode> shardNodes)
    {
        final Map<String, Long> nodeIds = new HashMap<>();
        for (ShardNode shardNode : shardNodes) {
            String nodeIdentifier = shardNode.getNodeIdentifier();
            if (!nodeIds.containsKey(nodeIdentifier)) {
                nodeIds.put(nodeIdentifier, getOrCreateNodeId(nodeIdentifier));
            }
        }

        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
            {
                ShardManagerDao dao = handle.attach(ShardManagerDao.class);
                for (ShardNode shardNode : shardNodes) {
                    long nodeId = nodeIds.get(shardNode.getNodeIdentifier());
                    long shardId = dao.insertShard(shardNode.getShardUuid());
                    dao.insertShardNode(shardId, nodeId);
                    dao.insertTableShard(tableId, shardId);
                }
            }
        });
    }

    @Override
    public Iterable<ShardNode> getShardNodes(long tableId)
    {
        return dao.getShardNodes(tableId);
    }

    @Override
    public void dropTableShards(long tableId)
    {
        dao.dropTableShards(tableId);
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
            throw new PrestoException(INTERNAL_ERROR, "node does not exist after insert");
        }
        return id;
    }
}
