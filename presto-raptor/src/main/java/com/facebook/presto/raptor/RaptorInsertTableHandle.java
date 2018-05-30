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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RaptorInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final String connectorId;
    private final long transactionId;
    private final long tableId;
    private final List<RaptorColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final Optional<String> externalBatchId;
    private final List<RaptorColumnHandle> sortColumnHandles;
    private final List<SortOrder> sortOrders;
    private final OptionalInt bucketCount;
    private final List<RaptorColumnHandle> bucketColumnHandles;
    private final Optional<RaptorColumnHandle> temporalColumnHandle;

    @JsonCreator
    public RaptorInsertTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("transactionId") long transactionId,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("columnHandles") List<RaptorColumnHandle> columnHandles,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("externalBatchId") Optional<String> externalBatchId,
            @JsonProperty("sortColumnHandles") List<RaptorColumnHandle> sortColumnHandles,
            @JsonProperty("sortOrders") List<SortOrder> sortOrders,
            @JsonProperty("bucketCount") OptionalInt bucketCount,
            @JsonProperty("bucketColumnHandles") List<RaptorColumnHandle> bucketColumnHandles,
            @JsonProperty("temporalColumnHandle") Optional<RaptorColumnHandle> temporalColumnHandle)
    {
        checkArgument(tableId > 0, "tableId must be greater than zero");

        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.transactionId = transactionId;
        this.tableId = tableId;
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.externalBatchId = requireNonNull(externalBatchId, "externalBatchId is null");

        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        this.sortColumnHandles = ImmutableList.copyOf(requireNonNull(sortColumnHandles, "sortColumnHandles is null"));
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        this.bucketColumnHandles = ImmutableList.copyOf(requireNonNull(bucketColumnHandles, "bucketColumnHandles is null"));
        this.temporalColumnHandle = requireNonNull(temporalColumnHandle, "temporalColumnHandle is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public long getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public List<RaptorColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public Optional<String> getExternalBatchId()
    {
        return externalBatchId;
    }

    @JsonProperty
    public List<RaptorColumnHandle> getSortColumnHandles()
    {
        return sortColumnHandles;
    }

    @JsonProperty
    public List<SortOrder> getSortOrders()
    {
        return sortOrders;
    }

    @JsonProperty
    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public List<RaptorColumnHandle> getBucketColumnHandles()
    {
        return bucketColumnHandles;
    }

    @JsonProperty
    public Optional<RaptorColumnHandle> getTemporalColumnHandle()
    {
        return temporalColumnHandle;
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + tableId;
    }
}
