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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.ResultIterator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayFromBytes;
import static com.facebook.presto.raptor.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

final class ShardIterator
        extends AbstractIterator<BucketShards>
        implements ResultIterator<BucketShards>
{
    private static final Logger log = Logger.get(ShardIterator.class);
    private final Map<Integer, String> nodeMap = new HashMap<>();

    private final boolean merged;
    private final Map<Integer, String> bucketToNode;
    private final ShardDao dao;
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;
    private boolean first = true;

    public ShardIterator(
            long tableId,
            boolean merged,
            Optional<Map<Integer, String>> bucketToNode,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            IDBI dbi)
    {
        this.merged = merged;
        this.bucketToNode = bucketToNode.orElse(null);
        ShardPredicate predicate = ShardPredicate.create(effectivePredicate, bucketToNode.isPresent());

        String sql;
        if (bucketToNode.isPresent()) {
            sql = "SELECT shard_uuid, bucket_number FROM %s WHERE %s ORDER BY bucket_number";
        }
        else {
            sql = "SELECT shard_uuid, node_ids FROM %s WHERE %s";
        }
        sql = format(sql, shardIndexTable(tableId), predicate.getPredicate());

        dao = onDemandDao(dbi, ShardDao.class);
        fetchNodes();

        try {
            connection = dbi.open().getConnection();
            statement = connection.prepareStatement(sql);
            enableStreamingResults(statement);
            predicate.bind(statement);
            log.debug("Running query: %s", statement);
            resultSet = statement.executeQuery();
        }
        catch (SQLException e) {
            close();
            throw metadataError(e);
        }
        catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    protected BucketShards computeNext()
    {
        try {
            return merged ? computeMerged() : compute();
        }
        catch (SQLException e) {
            throw metadataError(e);
        }
    }

    @SuppressWarnings({"UnusedDeclaration", "EmptyTryBlock"})
    @Override
    public void close()
    {
        // use try-with-resources to close everything properly
        try (Connection connection = this.connection;
                Statement statement = this.statement;
                ResultSet resultSet = this.resultSet) {
            // do nothing
        }
        catch (SQLException ignored) {
        }
    }

    /**
     * Compute split-per-shard (separate split for each shard).
     */
    private BucketShards compute()
            throws SQLException
    {
        if (!resultSet.next()) {
            return endOfData();
        }

        UUID shardUuid = uuidFromBytes(resultSet.getBytes("shard_uuid"));
        Set<String> nodeIdentifiers;
        OptionalInt bucketNumber = OptionalInt.empty();

        if (bucketToNode != null) {
            int bucket = resultSet.getInt("bucket_number");
            bucketNumber = OptionalInt.of(bucket);
            nodeIdentifiers = ImmutableSet.of(getBucketNode(bucket));
        }
        else {
            List<Integer> nodeIds = intArrayFromBytes(resultSet.getBytes("node_ids"));
            nodeIdentifiers = getNodeIdentifiers(nodeIds, shardUuid);
        }

        ShardNodes shard = new ShardNodes(shardUuid, nodeIdentifiers);
        return new BucketShards(bucketNumber, ImmutableSet.of(shard));
    }

    /**
     * Compute split-per-bucket (single split for all shards in a bucket).
     */
    private BucketShards computeMerged()
            throws SQLException
    {
        if (resultSet.isAfterLast()) {
            return endOfData();
        }
        if (first) {
            first = false;
            if (!resultSet.next()) {
                return endOfData();
            }
        }

        int bucketNumber = resultSet.getInt("bucket_number");
        ImmutableSet.Builder<ShardNodes> shards = ImmutableSet.builder();

        do {
            UUID shardUuid = uuidFromBytes(resultSet.getBytes("shard_uuid"));
            int bucket = resultSet.getInt("bucket_number");
            Set<String> nodeIdentifiers = ImmutableSet.of(getBucketNode(bucket));

            shards.add(new ShardNodes(shardUuid, nodeIdentifiers));
        }
        while (resultSet.next() && resultSet.getInt("bucket_number") == bucketNumber);

        return new BucketShards(OptionalInt.of(bucketNumber), shards.build());
    }

    private String getBucketNode(int bucket)
    {
        String node = bucketToNode.get(bucket);
        if (node == null) {
            throw new PrestoException(RAPTOR_ERROR, "No node mapping for bucket: " + bucket);
        }
        return node;
    }

    private Set<String> getNodeIdentifiers(List<Integer> nodeIds, UUID shardUuid)
    {
        Function<Integer, String> fetchNode = id -> fetchNode(id, shardUuid);
        return nodeIds.stream()
                .map(id -> nodeMap.computeIfAbsent(id, fetchNode))
                .collect(toSet());
    }

    private String fetchNode(int id, UUID shardUuid)
    {
        String node = dao.getNodeIdentifier(id);
        if (node == null) {
            throw new PrestoException(RAPTOR_ERROR, format("Missing node ID [%s] for shard: %s", id, shardUuid));
        }
        return node;
    }

    private void fetchNodes()
    {
        for (RaptorNode node : dao.getNodes()) {
            nodeMap.put(node.getNodeId(), node.getNodeIdentifier());
        }
    }

    private static void enableStreamingResults(Statement statement)
            throws SQLException
    {
        if (statement.isWrapperFor(com.mysql.jdbc.Statement.class)) {
            statement.unwrap(com.mysql.jdbc.Statement.class).enableStreamingResults();
        }
    }
}
