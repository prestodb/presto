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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplit
        implements ConnectorSplit
{
    private final String index;
    private final int shard;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<String> address;

    @JsonCreator
    public ElasticsearchSplit(
            @JsonProperty("index") String index,
            @JsonProperty("shard") int shard,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("address") Optional<String> address)
    {
        this.index = requireNonNull(index, "index is null");
        this.shard = shard;
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.address = requireNonNull(address, "address is null");
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public int getShard()
    {
        return shard;
    }

    @JsonProperty
    public Optional<String> getAddress()
    {
        return address;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return address.map(host -> ImmutableList.of(HostAddress.fromString(host)))
                .orElseGet(ImmutableList::of);
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
                .addValue(index)
                .addValue(shard)
                .addValue(tupleDomain)
                .addValue(address)
                .toString();
    }
}
