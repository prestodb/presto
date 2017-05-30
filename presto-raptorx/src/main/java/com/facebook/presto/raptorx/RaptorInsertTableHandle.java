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
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RaptorInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final long transactionId;
    private final long tableId;
    private final List<RaptorColumnHandle> columnHandles;
    private final List<RaptorColumnHandle> sortColumnHandles;
    private final int bucketCount;
    private final List<RaptorColumnHandle> bucketColumnHandles;
    private final Optional<RaptorColumnHandle> temporalColumnHandle;
    private final CompressionType compressionType;

    @JsonCreator
    public RaptorInsertTableHandle(
            @JsonProperty("transactionId") long transactionId,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("columnHandles") List<RaptorColumnHandle> columnHandles,
            @JsonProperty("sortColumnHandles") List<RaptorColumnHandle> sortColumnHandles,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("bucketColumnHandles") List<RaptorColumnHandle> bucketColumnHandles,
            @JsonProperty("temporalColumnHandle") Optional<RaptorColumnHandle> temporalColumnHandle,
            @JsonProperty("compressionType") CompressionType compressionType)
    {
        this.transactionId = transactionId;
        this.tableId = tableId;
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.sortColumnHandles = ImmutableList.copyOf(requireNonNull(sortColumnHandles, "sortColumnHandles is null"));
        this.bucketCount = bucketCount;
        this.bucketColumnHandles = ImmutableList.copyOf(requireNonNull(bucketColumnHandles, "bucketColumnHandles is null"));
        this.temporalColumnHandle = requireNonNull(temporalColumnHandle, "temporalColumnHandle is null");
        this.compressionType = requireNonNull(compressionType, "compressionType is null");
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
    public List<RaptorColumnHandle> getSortColumnHandles()
    {
        return sortColumnHandles;
    }

    @JsonProperty
    public int getBucketCount()
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

    @JsonProperty
    public CompressionType getCompressionType()
    {
        return compressionType;
    }

    @Override
    public String toString()
    {
        return String.valueOf(tableId);
    }
}
