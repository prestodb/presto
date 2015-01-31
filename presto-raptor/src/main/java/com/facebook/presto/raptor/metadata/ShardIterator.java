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

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.util.CloseableIterator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static com.google.common.base.Preconditions.checkNotNull;

final class ShardIterator
        extends AbstractIterator<ShardNodes>
        implements CloseableIterator<ShardNodes>
{
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;

    private boolean done;
    private UUID currentShardId;
    private ImmutableSet.Builder<String> nodes = ImmutableSet.builder();

    public ShardIterator(long tableId, TupleDomain<RaptorColumnHandle> effectivePredicate, Connection connection)
    {
        ShardPredicate predicate = ShardPredicate.create(effectivePredicate);

        String sql = "" +
                "SELECT shard_uuid, node_identifier\n" +
                "FROM " + shardIndexTable(tableId) + " t\n" +
                "LEFT JOIN shard_nodes sn ON (t.shard_id = sn.shard_id)\n" +
                "LEFT JOIN nodes n ON (sn.node_id = n.node_id)\n" +
                "WHERE " + predicate.getPredicate() + "\n" +
                "ORDER BY shard_uuid";

        this.connection = checkNotNull(connection, "connection is null");
        try {
            statement = connection.prepareStatement(sql);
            predicate.bind(statement);
            resultSet = statement.executeQuery();
        }
        catch (SQLException e) {
            close();
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    @Override
    protected ShardNodes computeNext()
    {
        try {
            return compute();
        }
        catch (SQLException e) {
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    @SuppressWarnings({"UnusedDeclaration", "EmptyTryBlock"})
    @Override
    public void close()
    {
        // use try-with-resources to close everything properly
        try (ResultSet resultSet = this.resultSet;
                Statement statement = this.statement;
                Connection connection = this.connection) {
            // do nothing
        }
        catch (SQLException ignored) {
        }
    }

    private ShardNodes compute()
            throws SQLException
    {
        if (done) {
            return endOfData();
        }

        while (resultSet.next()) {
            UUID shardId = uuidFromBytes(resultSet.getBytes("shard_uuid"));
            String nodeId = resultSet.getString("node_identifier");

            ShardNodes value = null;
            if ((currentShardId != null) && !shardId.equals(currentShardId)) {
                value = new ShardNodes(currentShardId, nodes.build());
                nodes = ImmutableSet.builder();
            }

            currentShardId = shardId;
            if (nodeId != null) {
                nodes.add(nodeId);
            }

            if (value != null) {
                return value;
            }
        }
        done = true;

        if (currentShardId != null) {
            return new ShardNodes(currentShardId, nodes.build());
        }
        return endOfData();
    }
}
