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
package com.facebook.presto.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitionFunctionBinding
{
    private final PartitioningHandle partitioningHandle;
    private final List<Symbol> outputLayout;
    private final List<Symbol> partitioningColumns;
    private final Optional<Symbol> hashColumn;
    private final boolean replicateNulls;
    private final Optional<int[]> bucketToPartition;

    public PartitionFunctionBinding(PartitioningHandle partitioningHandle, List<Symbol> outputLayout, List<Symbol> partitioningColumns)
    {
        this(partitioningHandle,
                outputLayout,
                partitioningColumns,
                Optional.empty(),
                false,
                Optional.empty());
    }

    public PartitionFunctionBinding(PartitioningHandle partitioningHandle, List<Symbol> outputLayout, List<Symbol> partitioningColumns, Optional<Symbol> hashColumn)
    {
        this(
                partitioningHandle,
                outputLayout,
                partitioningColumns,
                hashColumn,
                false,
                Optional.empty());
    }

    @JsonCreator
    public PartitionFunctionBinding(
            @JsonProperty("partitioningHandle") PartitioningHandle partitioningHandle,
            @JsonProperty("outputLayout") List<Symbol> outputLayout,
            @JsonProperty("partitioningColumns") List<Symbol> partitioningColumns,
            @JsonProperty("hashColumn") Optional<Symbol> hashColumn,
            @JsonProperty("replicateNulls") boolean replicateNulls,
            @JsonProperty("bucketToPartition") Optional<int[]> bucketToPartition)
    {
        this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
        this.outputLayout = ImmutableList.copyOf(requireNonNull(outputLayout, "outputLayout is null"));

        this.partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        checkArgument(ImmutableSet.copyOf(outputLayout).containsAll(partitioningColumns),
                "Output layout (%s) don't include all partition columns (%s)", outputLayout, partitioningColumns);

        this.hashColumn = requireNonNull(hashColumn, "hashColumn is null");
        hashColumn.ifPresent(column -> checkArgument(outputLayout.contains(column),
                "Output layout (%s) don't include hash column (%s)", outputLayout, column));

        checkArgument(!replicateNulls || partitioningColumns.size() == 1, "size of partitioningColumns is not 1 when nullPartition is REPLICATE.");
        this.replicateNulls = replicateNulls;
        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
    }

    @JsonProperty
    public PartitioningHandle getPartitioningHandle()
    {
        return partitioningHandle;
    }

    @JsonProperty
    public List<Symbol> getOutputLayout()
    {
        return outputLayout;
    }

    @JsonProperty
    public List<Symbol> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    @JsonProperty
    public Optional<Symbol> getHashColumn()
    {
        return hashColumn;
    }

    @JsonProperty
    public boolean isReplicateNulls()
    {
        return replicateNulls;
    }

    @JsonProperty
    public Optional<int[]> getBucketToPartition()
    {
        return bucketToPartition;
    }

    public PartitionFunctionBinding withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new PartitionFunctionBinding(partitioningHandle, outputLayout, partitioningColumns, hashColumn, replicateNulls, bucketToPartition);
    }

    public PartitionFunctionBinding translateOutputLayout(List<Symbol> newOutputLayout)
    {
        requireNonNull(newOutputLayout, "newOutputLayout is null");

        checkArgument(newOutputLayout.size() == outputLayout.size());

        List<Symbol> newPartitioningColumns = partitioningColumns.stream()
                .mapToInt(outputLayout::indexOf)
                .mapToObj(newOutputLayout::get)
                .collect(toImmutableList());

        Optional<Symbol> newHashSymbol = hashColumn
                .map(outputLayout::indexOf)
                .map(newOutputLayout::get);

        return new PartitionFunctionBinding(partitioningHandle, newOutputLayout, newPartitioningColumns, newHashSymbol, replicateNulls, bucketToPartition);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionFunctionBinding that = (PartitionFunctionBinding) o;
        return Objects.equals(partitioningHandle, that.partitioningHandle) &&
                Objects.equals(outputLayout, that.outputLayout) &&
                Objects.equals(partitioningColumns, that.partitioningColumns) &&
                replicateNulls == that.replicateNulls &&
                Objects.equals(bucketToPartition, that.bucketToPartition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioningHandle, outputLayout, partitioningColumns, replicateNulls, bucketToPartition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitioningHandle", partitioningHandle)
                .add("outputLayout", outputLayout)
                .add("partitioningChannels", partitioningColumns)
                .add("hashChannel", hashColumn)
                .add("replicateNulls", replicateNulls)
                .add("bucketToPartition", bucketToPartition)
                .toString();
    }
}
