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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BucketNode
{
    private final int bucketNumber;
    private final String nodeIdentifier;

    public BucketNode(int bucketNumber, String nodeIdentifier)
    {
        checkArgument(bucketNumber >= 0, "bucket number must be positive");
        this.bucketNumber = bucketNumber;
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null");
    }

    public int getBucketNumber()
    {
        return bucketNumber;
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
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
                Objects.equals(nodeIdentifier, that.nodeIdentifier);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketNumber, nodeIdentifier);
    }

    @Override
    public String toString()
    {
        return bucketNumber + ":" + nodeIdentifier;
    }

    public static class Mapper
            implements ResultSetMapper<BucketNode>
    {
        @Override
        public BucketNode map(int index, ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new BucketNode(
                    rs.getInt("bucket_number"),
                    rs.getString("node_identifier"));
        }
    }
}
