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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.ResultIterator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import static java.util.stream.Collectors.toList;

final class ShardIterator
        extends AbstractIterator<BucketShards>
        implements ResultIterator<BucketShards>
{
    private static final Logger log = Logger.get(ShardIterator.class);
    private final Map<Integer, String> nodeMap = new HashMap<>();

    private final boolean merged;
    private final Map<Integer, String> bucketToNode;
    private boolean first = true;
    private final List<ShardInfo> shardInfos;
    private int shardInfoIndex = -1;

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

        ShardDao dao = onDemandDao(dbi, ShardDao.class);
        fetchNodes(dao);

        try (Handle handle = dbi.open();
             PreparedStatement statement = handle.getConnection().prepareStatement(sql)) {
            predicate.bind(statement);
            log.debug("Running query: %s", statement);
            try (ResultSet rs = statement.executeQuery()) {
                shardInfos = load(rs, dao);
            }
        } catch (Exception e) {
            throw metadataError(e);
        }
    }

    private List<ShardInfo> load(ResultSet resultSet, ShardDao dao) throws SQLException
    {
        List<ShardInfo> result = new ArrayList<>();
        while (resultSet.next()) {
            UUID shardUuid = uuidFromBytes(resultSet.getBytes("shard_uuid"));
            List<String> nodeIdentifiers;
            OptionalInt bucketNumber = OptionalInt.empty();

            if (bucketToNode != null) {
                int bucket = resultSet.getInt("bucket_number");
                bucketNumber = OptionalInt.of(bucket);
                nodeIdentifiers = ImmutableList.of(getBucketNode(bucket));
            }
            else {
                List<Integer> nodeIds = intArrayFromBytes(resultSet.getBytes("node_ids"));
                nodeIdentifiers = getNodeIdentifiers(nodeIds, shardUuid, dao);
            }
            result.add(new ShardInfo(shardUuid, bucketNumber, nodeIdentifiers));
        }
        return result;
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

    @SuppressWarnings({"UnusedDeclaration"})
    @Override
    public void close()
    {
        shardInfos.clear();
    }

    /**
     * Compute split-per-shard (separate split for each shard).
     */
    private BucketShards compute()
            throws SQLException
    {
        if (!nextShard()) {
            return endOfData();
        }

        ShardInfo shardInfo = currentShard();
        UUID shardUuid = shardInfo.shardUuid;
        List<String> nodeIdentifiers = shardInfo.nodeIdentifiers;
        OptionalInt bucketNumber = shardInfo.bucketNumber;

        ShardNodes shard = new ShardNodes(shardUuid, new HashSet<>(nodeIdentifiers));
        return new BucketShards(bucketNumber, ImmutableSet.of(shard));
    }

    /**
     * Compute split-per-bucket (single split for all shards in a bucket).
     */
    private BucketShards computeMerged()
            throws SQLException
    {
        if (isEndOfData()) {
            return endOfData();
        }
        if (first) {
            first = false;
            if (!nextShard()) {
                return endOfData();
            }
        }

        int bucketNumber = currentShard().getBucketNumber();
        ImmutableSet.Builder<ShardNodes> shards = ImmutableSet.builder();

        do {
            ShardInfo shardInfo = currentShard();
            UUID shardUuid = shardInfo.shardUuid;
            int bucket = shardInfo.getBucketNumber();
            Set<String> nodeIdentifiers = ImmutableSet.of(getBucketNode(bucket));

            shards.add(new ShardNodes(shardUuid, nodeIdentifiers));
        }
        while (nextShard() && currentShard().getBucketNumber() == bucketNumber);

        return new BucketShards(OptionalInt.of(bucketNumber), shards.build());
    }

    private ShardInfo currentShard()
    {
        return shardInfos.get(shardInfoIndex);
    }

    private boolean isEndOfData()
    {
        return shardInfoIndex >= shardInfos.size();
    }

    private boolean nextShard()
    {
        ++shardInfoIndex;
        return !isEndOfData();
    }

    private String getBucketNode(int bucket)
    {
        String node = bucketToNode.get(bucket);
        if (node == null) {
            throw new PrestoException(RAPTOR_ERROR, "No node mapping for bucket: " + bucket);
        }
        return node;
    }

    private List<String> getNodeIdentifiers(List<Integer> nodeIds, UUID shardUuid, ShardDao dao)
    {
        Function<Integer, String> fetchNode = id -> fetchNode(id, shardUuid, dao);
        return nodeIds.stream()
                .map(id -> nodeMap.computeIfAbsent(id, fetchNode))
                .collect(toList());
    }

    private static String fetchNode(int id, UUID shardUuid, ShardDao dao)
    {
        String node = dao.getNodeIdentifier(id);
        if (node == null) {
            throw new PrestoException(RAPTOR_ERROR, format("Missing node ID [%s] for shard: %s", id, shardUuid));
        }
        return node;
    }

    private void fetchNodes(ShardDao dao)
    {
        for (RaptorNode node : dao.getNodes()) {
            nodeMap.put(node.getNodeId(), node.getNodeIdentifier());
        }
    }

    private static class ShardInfo
    {
        final UUID shardUuid;
        final List<String> nodeIdentifiers;
        private final OptionalInt bucketNumber;

        public ShardInfo(UUID shardUuid, OptionalInt bucketNumber, List<String> nodeIdentifiers)
        {
            this.shardUuid = shardUuid;
            this.bucketNumber = bucketNumber;
            this.nodeIdentifiers = nodeIdentifiers;
        }

        public int getBucketNumber()
        {
            return bucketNumber.getAsInt();
        }
    }
}
