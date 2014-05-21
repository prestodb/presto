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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.TablePartition;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.PrestoNode.getIdentifierFunction;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.slice.Slices.utf8Slice;

public class RaptorSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(RaptorSplitManager.class);

    private final String connectorId;
    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final RaptorMetadata metadata;

    @Inject
    public RaptorSplitManager(RaptorConnectorId connectorId, NodeManager nodeManager, ShardManager shardManager, RaptorMetadata metadata)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        Stopwatch partitionTimer = Stopwatch.createStarted();

        checkType(tableHandle, RaptorTableHandle.class, "table");

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);

        checkState(tableMetadata != null, "no metadata for %s found", tableHandle);

        Set<TablePartition> tablePartitions = shardManager.getPartitions(tableHandle);

        log.debug("Partition retrieval, raptor table %s (%d partitions): %dms", tableHandle, tablePartitions.size(), partitionTimer.elapsed(TimeUnit.MILLISECONDS));

        Multimap<String, ? extends PartitionKey> allPartitionKeys = shardManager.getAllPartitionKeys(tableHandle);
        Map<String, ConnectorColumnHandle> columnHandles = metadata.getColumnHandles(tableHandle);

        log.debug("Partition key retrieval, raptor table %s (%d keys): %dms", tableHandle, allPartitionKeys.size(), partitionTimer.elapsed(TimeUnit.MILLISECONDS));

        List<ConnectorPartition> partitions = ImmutableList.copyOf(transform(tablePartitions, partitionMapper(allPartitionKeys, columnHandles)));

        log.debug("Partition generation, raptor table %s (%d partitions): %dms", tableHandle, partitions.size(), partitionTimer.elapsed(TimeUnit.MILLISECONDS));

        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        Stopwatch splitTimer = Stopwatch.createStarted();

        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of());
        }

        Map<String, Node> nodesById = uniqueIndex(nodeManager.getActiveNodes(), getIdentifierFunction());

        List<ConnectorSplit> splits = new ArrayList<>();

        Multimap<Long, Entry<UUID, String>> partitionShardNodes = shardManager.getShardNodesByPartition(tableHandle);

        for (ConnectorPartition partition : partitions) {
            RaptorPartition raptorPartition = checkType(partition, RaptorPartition.class, "partition");

            ImmutableMultimap.Builder<UUID, String> shardNodes = ImmutableMultimap.builder();
            for (Entry<UUID, String> shardNode : partitionShardNodes.get(raptorPartition.getRaptorPartitionId())) {
                shardNodes.put(shardNode.getKey(), shardNode.getValue());
            }

            for (Map.Entry<UUID, Collection<String>> entry : shardNodes.build().asMap().entrySet()) {
                List<HostAddress> addresses = getAddressesForNodes(nodesById, entry.getValue());
                checkState(!addresses.isEmpty(), "no host for shard %s found: %s", entry.getKey(), entry.getValue());
                ConnectorSplit split = new RaptorSplit(entry.getKey(), addresses);
                splits.add(split);
            }
        }

        log.debug("Split retrieval for %d partitions (%d splits): %dms", partitions.size(), splits.size(), splitTimer.elapsed(TimeUnit.MILLISECONDS));

        // The query engine assumes that splits are returned in a somewhat random fashion. The Raptor split manager,
        // because it loads the data from a database table, will return the splits somewhat ordered by node ID,
        // so only a subset of nodes are fired up. Shuffle the splits to ensure random distribution.
        Collections.shuffle(splits);

        return new FixedSplitSource(connectorId, splits);
    }

    private static List<HostAddress> getAddressesForNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        ImmutableList.Builder<HostAddress> nodes = ImmutableList.builder();
        for (String id : nodeIdentifiers) {
            Node node = nodeMap.get(id);
            if (node != null) {
                nodes.add(node.getHostAndPort());
            }
        }
        return nodes.build();
    }

    public static class RaptorPartition
            implements ConnectorPartition
    {
        private final long partitionId;
        private final TupleDomain<ConnectorColumnHandle> tupleDomain;

        public RaptorPartition(long partitionId, TupleDomain<ConnectorColumnHandle> tupleDomain)
        {
            this.partitionId = partitionId;
            this.tupleDomain = checkNotNull(tupleDomain, "tupleDomain is null");
        }

        @Override
        public String getPartitionId()
        {
            return Long.toString(partitionId);
        }

        public long getRaptorPartitionId()
        {
            return partitionId;
        }

        @Override
        public TupleDomain<ConnectorColumnHandle> getTupleDomain()
        {
            return tupleDomain;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(partitionId, tupleDomain);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RaptorPartition other = (RaptorPartition) obj;
            return this.partitionId == other.partitionId
                    && Objects.equal(this.tupleDomain, other.tupleDomain);
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("partitionId", partitionId)
                    .add("tupleDomain", tupleDomain)
                    .toString();
        }
    }

    private static Function<TablePartition, ConnectorPartition> partitionMapper(
            final Multimap<String, ? extends PartitionKey> allPartitionKeys,
            final Map<String, ConnectorColumnHandle> columnHandles)
    {
        return new Function<TablePartition, ConnectorPartition>()
        {
            @Override
            public ConnectorPartition apply(TablePartition tablePartition)
            {
                String partitionName = tablePartition.getPartitionName();

                ImmutableMap.Builder<ConnectorColumnHandle, Domain> builder = ImmutableMap.builder();
                for (PartitionKey partitionKey : allPartitionKeys.get(partitionName)) {
                    ConnectorColumnHandle columnHandle = columnHandles.get(partitionKey.getName());
                    checkArgument(columnHandle != null, "Invalid partition key for column %s in partition %s", partitionKey.getName(), tablePartition.getPartitionName());

                    String value = partitionKey.getValue();
                    Class<?> javaType = partitionKey.getType().getJavaType();

                    if (javaType == boolean.class) {
                        if (value.isEmpty()) {
                            builder.put(columnHandle, Domain.singleValue(false));
                        }
                        else {
                            builder.put(columnHandle, Domain.singleValue(Boolean.parseBoolean(value)));
                        }
                    }
                    else if (javaType == long.class) {
                        if (value.isEmpty()) {
                            builder.put(columnHandle, Domain.singleValue(0L));
                        }
                        else {
                            builder.put(columnHandle, Domain.singleValue(Long.parseLong(value)));
                        }
                    }
                    else if (javaType == double.class) {
                        if (value.isEmpty()) {
                            builder.put(columnHandle, Domain.singleValue(0.0));
                        }
                        else {
                            builder.put(columnHandle, Domain.singleValue(Double.parseDouble(value)));
                        }
                    }
                    else if (javaType == Slice.class) {
                        builder.put(columnHandle, Domain.singleValue(utf8Slice(value)));
                    }
                }

                return new RaptorPartition(tablePartition.getPartitionId(), TupleDomain.withColumnDomains(builder.build()));
            }
        };
    }
}
