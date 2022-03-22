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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RaptorSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final Set<UUID> shardUuids;
    private final Map<UUID, UUID> shardDeltaMap;
    private final boolean tableSupportsDeltaDelete;
    private final OptionalInt bucketNumber;
    private final List<HostAddress> addresses;
    private final TupleDomain<RaptorColumnHandle> effectivePredicate;
    private final OptionalLong transactionId;
    private final Optional<Map<String, Type>> columnTypes;

    @JsonCreator
    public RaptorSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("shardUuids") Set<UUID> shardUuids,
            @JsonProperty("shardDeltaMap") Map<UUID, UUID> shardDeltaMap,
            @JsonProperty("tableSupportsDeltaDelete") boolean tableSupportsDeltaDelete,
            @JsonProperty("bucketNumber") OptionalInt bucketNumber,
            @JsonProperty("effectivePredicate") TupleDomain<RaptorColumnHandle> effectivePredicate,
            @JsonProperty("transactionId") OptionalLong transactionId,
            @JsonProperty("columnTypes") Optional<Map<String, Type>> columnTypes)
    {
        this(
                connectorId,
                shardUuids,
                shardDeltaMap,
                tableSupportsDeltaDelete,
                bucketNumber,
                ImmutableList.of(),
                effectivePredicate,
                transactionId,
                columnTypes);
    }

    public RaptorSplit(
            String connectorId,
            UUID shardUuid,
            Optional<UUID> deltaShardUuid,
            boolean tableSupportsDeltaDelete,
            List<HostAddress> addresses,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            OptionalLong transactionId,
            Optional<Map<String, Type>> columnTypes)
    {
        this(
                connectorId,
                ImmutableSet.of(shardUuid),
                deltaShardUuid.map(deltaUuid -> ImmutableMap.of(shardUuid, deltaUuid)).orElse(ImmutableMap.of()),
                tableSupportsDeltaDelete,
                OptionalInt.empty(),
                addresses,
                effectivePredicate,
                transactionId,
                columnTypes);
    }

    public RaptorSplit(
            String connectorId,
            Set<UUID> shardUuids,
            Map<UUID, UUID> shardDeltaMap,
            boolean tableSupportsDeltaDelete,
            int bucketNumber,
            HostAddress address,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            OptionalLong transactionId,
            Optional<Map<String, Type>> columnTypes)
    {
        this(
                connectorId,
                shardUuids,
                shardDeltaMap,
                tableSupportsDeltaDelete,
                OptionalInt.of(bucketNumber),
                ImmutableList.of(address),
                effectivePredicate,
                transactionId,
                columnTypes);
    }

    private RaptorSplit(
            String connectorId,
            Set<UUID> shardUuids,
            Map<UUID, UUID> shardDeltaMap,
            boolean tableSupportsDeltaDelete,
            OptionalInt bucketNumber,
            List<HostAddress> addresses,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            OptionalLong transactionId,
            Optional<Map<String, Type>> columnTypes)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.shardUuids = ImmutableSet.copyOf(requireNonNull(shardUuids, "shardUuid is null"));
        this.shardDeltaMap = requireNonNull(shardDeltaMap, "shardUuid is null");
        this.tableSupportsDeltaDelete = tableSupportsDeltaDelete;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return HARD_AFFINITY;
    }

    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return addresses;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public Set<UUID> getShardUuids()
    {
        return shardUuids;
    }

    @JsonProperty
    public Map<UUID, UUID> getShardDeltaMap()
    {
        return shardDeltaMap;
    }

    @JsonProperty
    public boolean isTableSupportsDeltaDelete()
    {
        return tableSupportsDeltaDelete;
    }

    @JsonProperty
    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    @JsonProperty
    public TupleDomain<RaptorColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @JsonProperty
    public OptionalLong getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public Optional<Map<String, Type>> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("shardUuids", shardUuids)
                .add("shardDeltaMap", shardDeltaMap.toString())
                .add("tableSupportsDeltaDelete", tableSupportsDeltaDelete)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .add("hosts", addresses)
                .omitNullValues()
                .toString();
    }
}
