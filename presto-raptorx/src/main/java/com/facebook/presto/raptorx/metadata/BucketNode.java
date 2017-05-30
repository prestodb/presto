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
package com.facebook.presto.raptorx.metadata;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class BucketNode
{
    private final int bucketNumber;
    private final long nodeId;

    public BucketNode(int bucketNumber, long nodeId)
    {
        this.bucketNumber = bucketNumber;
        this.nodeId = nodeId;
    }

    public int getBucketNumber()
    {
        return bucketNumber;
    }

    public long getNodeId()
    {
        return nodeId;
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
        BucketNode that = (BucketNode) o;
        return (bucketNumber == that.bucketNumber) &&
                (nodeId == that.nodeId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketNumber, nodeId);
    }

    public static class Mapper
            implements RowMapper<BucketNode>
    {
        @Override
        public BucketNode map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new BucketNode(
                    rs.getInt("bucket_number"),
                    rs.getLong("node_id"));
        }
    }
}
