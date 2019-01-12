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
package io.prestosql.plugin.raptor.legacy.metadata;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.raptor.legacy.util.UuidUtil.uuidFromBytes;
import static java.util.Objects.requireNonNull;

public class ShardNode
{
    private final UUID shardUuid;
    private final String nodeIdentifier;

    public ShardNode(UUID shardUuid, String nodeIdentifier)
    {
        this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null");
    }

    public UUID getShardUuid()
    {
        return shardUuid;
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShardNode other = (ShardNode) obj;
        return Objects.equals(this.shardUuid, other.shardUuid) &&
                Objects.equals(this.nodeIdentifier, other.nodeIdentifier);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(shardUuid, nodeIdentifier);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("shardUuid", shardUuid)
                .add("nodeIdentifier", nodeIdentifier)
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
                    r.getString("node_identifier"));
        }
    }
}
