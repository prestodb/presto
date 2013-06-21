package com.facebook.presto.metadata;

import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ShardNode
{
    private final long shardId;
    private final String nodeIdentifier;
    private final long tableId;
    private final long partitionId;

    public ShardNode(long shardId, String nodeIdentifier, long tableId, long partitionId)
    {
        this.shardId = shardId;
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null");
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    public long getShardId()
    {
        return shardId;
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    public long getTableId()
    {
        return tableId;
    }

    public long getPartitionId()
    {
        return partitionId;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("shardId", shardId)
                .add("nodeIdentifier", nodeIdentifier)
                .add("tableId", tableId)
                .add("partitionId", partitionId)
                .toString();
    }

    public static class Mapper
            implements ResultSetMapper<ShardNode>
    {
        @Override
        public ShardNode map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new ShardNode(r.getLong("shard_id"),
                    r.getString("node_identifier"),
                    r.getLong("table_id"),
                    r.getLong("partition_id"));
        }
    }
}
