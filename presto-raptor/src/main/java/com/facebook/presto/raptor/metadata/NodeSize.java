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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NodeSize
{
    private final String nodeIdentifier;
    private final long sizeInBytes;

    public NodeSize(String nodeIdentifier, long sizeInBytes)
    {
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null");
        checkArgument(sizeInBytes >= 0, "sizeInBytes must be >= 0");
        this.sizeInBytes = sizeInBytes;
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        NodeSize nodeSize = (NodeSize) o;
        return (sizeInBytes == nodeSize.sizeInBytes) &&
                Objects.equals(nodeIdentifier, nodeSize.nodeIdentifier);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeIdentifier, sizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeIdentifier", nodeIdentifier)
                .add("sizeInBytes", sizeInBytes)
                .toString();
    }

    public static class Mapper
            implements ResultSetMapper<NodeSize>
    {
        @Override
        public NodeSize map(int index, ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new NodeSize(
                    rs.getString("node_identifier"),
                    rs.getLong("bytes"));
        }
    }
}
