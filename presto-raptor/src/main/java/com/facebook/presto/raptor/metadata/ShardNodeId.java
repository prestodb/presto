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

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static java.util.Objects.requireNonNull;

public class ShardNodeId
{
    private final UUID shardUuid;
    private final int nodeId;

    public ShardNodeId(UUID shardUuid, int nodeId)
    {
        this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
        this.nodeId = nodeId;
    }

    public UUID getShardUuid()
    {
        return shardUuid;
    }

    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShardNodeId that = (ShardNodeId) o;
        return (nodeId == that.nodeId) &&
                Objects.equals(shardUuid, that.shardUuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(shardUuid, nodeId);
    }

    @Override
    public String toString()
    {
        return shardUuid + ":" + nodeId;
    }

    public static class Mapper
            implements ResultSetMapper<ShardNodeId>
    {
        @Override
        public ShardNodeId map(int index, ResultSet rs, StatementContext ctx)
                throws SQLException
        {
            return new ShardNodeId(
                    uuidFromBytes(rs.getBytes("shard_uuid")),
                    rs.getInt("node_id"));
        }
    }
}
