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
package com.facebook.presto.plugin.opensearch;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Split for OpenSearch connector.
 * Represents a shard or partition of an OpenSearch index.
 * Can optionally represent a k-NN vector search query.
 */
public class OpenSearchSplit
        implements ConnectorSplit
{
    private final String indexName;
    private final int shardId;
    private final List<HostAddress> addresses;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<String> vectorField;
    private final Optional<float[]> queryVector;
    private final Optional<Integer> k;
    private final Optional<String> spaceType;
    private final Optional<Integer> efSearch;

    @JsonCreator
    public OpenSearchSplit(
            @JsonProperty("indexName") String indexName,
            @JsonProperty("shardId") int shardId,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("vectorField") Optional<String> vectorField,
            @JsonProperty("queryVector") Optional<float[]> queryVector,
            @JsonProperty("k") Optional<Integer> k,
            @JsonProperty("spaceType") Optional<String> spaceType,
            @JsonProperty("efSearch") Optional<Integer> efSearch)
    {
        this.indexName = requireNonNull(indexName, "indexName is null");
        this.shardId = shardId;
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.vectorField = requireNonNull(vectorField, "vectorField is null");
        this.queryVector = requireNonNull(queryVector, "queryVector is null");
        this.k = requireNonNull(k, "k is null");
        this.spaceType = requireNonNull(spaceType, "spaceType is null");
        this.efSearch = requireNonNull(efSearch, "efSearch is null");
    }

    public OpenSearchSplit(String indexName, int shardId, List<HostAddress> addresses)
    {
        this(indexName, shardId, addresses, TupleDomain.all(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    // Constructor for k-NN searches
    public OpenSearchSplit(
            String indexName,
            int shardId,
            List<HostAddress> addresses,
            String vectorField,
            float[] queryVector,
            int k,
            String spaceType,
            Integer efSearch)
    {
        this(indexName, shardId, addresses, TupleDomain.all(), Optional.of(vectorField),
                Optional.of(queryVector), Optional.of(k), Optional.of(spaceType), Optional.ofNullable(efSearch));
    }

    @JsonProperty
    public String getIndexName()
    {
        return indexName;
    }

    @JsonProperty
    public int getShardId()
    {
        return shardId;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty
    public Optional<String> getVectorField()
    {
        return vectorField;
    }

    @JsonProperty
    public Optional<float[]> getQueryVector()
    {
        return queryVector;
    }

    @JsonProperty
    public Optional<Integer> getK()
    {
        return k;
    }

    @JsonProperty
    public Optional<String> getSpaceType()
    {
        return spaceType;
    }

    @JsonProperty
    public Optional<Integer> getEfSearch()
    {
        return efSearch;
    }

    public boolean isKnnSearch()
    {
        return vectorField.isPresent();
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        OpenSearchSplit other = (OpenSearchSplit) obj;
        return Objects.equals(indexName, other.indexName) &&
                shardId == other.shardId &&
                Objects.equals(addresses, other.addresses) &&
                Objects.equals(tupleDomain, other.tupleDomain) &&
                Objects.equals(vectorField, other.vectorField) &&
                Objects.deepEquals(queryVector.orElse(null), other.queryVector.orElse(null)) &&
                Objects.equals(k, other.k) &&
                Objects.equals(spaceType, other.spaceType) &&
                Objects.equals(efSearch, other.efSearch);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, shardId, addresses, tupleDomain, vectorField,
                Objects.hashCode(queryVector.orElse(null)), k, spaceType, efSearch);
    }

    @Override
    public String toString()
    {
        return indexName + "[" + shardId + "] @ " +
                addresses.stream()
                        .map(HostAddress::toString)
                        .collect(toImmutableList());
    }
}
