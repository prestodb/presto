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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RaptorSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final UUID shardUuid;
    private final List<HostAddress> addresses;
    private final TupleDomain<RaptorColumnHandle> effectivePredicate;
    private final OptionalLong transactionId;

    @JsonCreator
    public RaptorSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("shardUuid") UUID shardUuid,
            @JsonProperty("effectivePredicate") TupleDomain<RaptorColumnHandle> effectivePredicate,
            @JsonProperty("transactionId") OptionalLong transactionId)
    {
        this(connectorId, shardUuid, ImmutableList.of(), effectivePredicate, transactionId);
    }

    public RaptorSplit(
            String connectorId,
            UUID shardUuid,
            List<HostAddress> addresses,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            OptionalLong transactionId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public UUID getShardUuid()
    {
        return shardUuid;
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

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("shardUuid", shardUuid)
                .add("hosts", addresses)
                .toString();
    }
}
