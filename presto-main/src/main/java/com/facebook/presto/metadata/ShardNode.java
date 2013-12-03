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

import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import static com.facebook.presto.metadata.UuidArguments.uuidFromBytes;
import static com.google.common.base.Preconditions.checkNotNull;

public class ShardNode
{
    private final UUID shardUuid;
    private final String nodeIdentifier;
    private final long tableId;
    private final long partitionId;

    public ShardNode(UUID shardUuid, String nodeIdentifier, long tableId, long partitionId)
    {
        this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null");
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    public UUID getShardUuid()
    {
        return shardUuid;
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
                .add("shardUuid", shardUuid)
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
            return new ShardNode(
                    uuidFromBytes(r.getBytes("shard_uuid")),
                    r.getString("node_identifier"),
                    r.getLong("table_id"),
                    r.getLong("partition_id"));
        }
    }
}
