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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.raptorx.util.DatabaseUtil.boxedLong;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RaptorSplit
        implements ConnectorSplit
{
    private final long tableId;
    private final int bucketNumber;
    private final Set<Long> chunkIds;
    private final TupleDomain<RaptorColumnHandle> predicate;
    private final HostAddress address;
    private final OptionalLong transactionId;
    private final Map<Long, Type> chunkColumnTypes;
    private final Optional<CompressionType> compressionType;

    @JsonCreator
    public RaptorSplit(
            @JsonProperty("tableId") long tableId,
            @JsonProperty("bucketNumber") int bucketNumber,
            @JsonProperty("chunkIds") Set<Long> chunkIds,
            @JsonProperty("predicate") TupleDomain<RaptorColumnHandle> predicate,
            @JsonProperty("address") HostAddress address,
            @JsonProperty("transactionId") OptionalLong transactionId,
            @JsonProperty("chunkColumnTypes") Map<Long, Type> chunkColumnTypes,
            @JsonProperty("compressionType") Optional<CompressionType> compressionType)
    {
        this.tableId = tableId;
        this.bucketNumber = bucketNumber;
        this.chunkIds = ImmutableSet.copyOf(requireNonNull(chunkIds, "chunkIds is null"));
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.address = requireNonNull(address, "address is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.chunkColumnTypes = ImmutableMap.copyOf(requireNonNull(chunkColumnTypes, "chunkColumnTypes is null"));
        this.compressionType = requireNonNull(compressionType, "compressionType is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("tableId", tableId)
                .put("bucketNumber", bucketNumber)
                .put("chunkIds", chunkIds)
                .build();
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public int getBucketNumber()
    {
        return bucketNumber;
    }

    @JsonProperty
    public Set<Long> getChunkIds()
    {
        return chunkIds;
    }

    @JsonProperty
    public TupleDomain<RaptorColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @JsonProperty
    public OptionalLong getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public Map<Long, Type> getChunkColumnTypes()
    {
        return chunkColumnTypes;
    }

    @JsonProperty
    public Optional<CompressionType> getCompressionType()
    {
        return compressionType;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("bucketNumber", bucketNumber)
                .add("chunkIds", chunkIds)
                .add("address", address)
                .add("transactionId", boxedLong(transactionId))
                .omitNullValues()
                .toString();
    }
}
