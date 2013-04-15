package com.facebook.presto.metadata;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ShardNode
{
    private final long shardId;
    private final String nodeIdentifier;

    public ShardNode(long shardId, String nodeIdentifier)
    {
        this.shardId = shardId;
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null");
    }

    public long getShardId()
    {
        return shardId;
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    public static class Mapper
            implements ResultSetMapper<ShardNode>
    {
        @Override
        public ShardNode map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new ShardNode(r.getLong("shard_id"),
                    r.getString("node_identifier"));
        }
    }
}
