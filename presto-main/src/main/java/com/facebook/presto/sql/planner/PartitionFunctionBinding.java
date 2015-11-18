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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitionFunctionBinding
{
    private final PartitionFunctionHandle functionHandle;
    private final List<Symbol> partitioningColumns;
    private final Optional<Symbol> hashColumn;
    private final boolean replicateNulls;
    private final Optional<int[]> bucketToPartition;

    public PartitionFunctionBinding(PartitionFunctionHandle functionHandle, List<Symbol> partitioningColumns)
    {
        this(functionHandle,
                partitioningColumns,
                Optional.empty(),
                false,
                Optional.empty());
    }

    public PartitionFunctionBinding(PartitionFunctionHandle functionHandle, List<Symbol> partitioningColumns, Optional<Symbol> hashColumn)
    {
        this(functionHandle,
                partitioningColumns,
                hashColumn,
                false,
                Optional.empty());
    }

    @JsonCreator
    public PartitionFunctionBinding(
            @JsonProperty("functionHandle") PartitionFunctionHandle functionHandle,
            @JsonProperty("partitioningColumns") List<Symbol> partitioningColumns,
            @JsonProperty("hashColumn") Optional<Symbol> hashColumn,
            @JsonProperty("replicateNulls") boolean replicateNulls,
            @JsonProperty("bucketToPartition") Optional<int[]> bucketToPartition)
    {
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        this.hashColumn = requireNonNull(hashColumn, "hashColumn is null");
        checkArgument(!replicateNulls || partitioningColumns.size() == 1, "size of partitioningColumns is not 1 when nullPartition is REPLICATE.");
        this.replicateNulls = replicateNulls;
        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
    }

    @JsonProperty
    public PartitionFunctionHandle getFunctionHandle()
    {
        return functionHandle;
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
        return new PartitionFunctionBinding(functionHandle, partitioningColumns, hashColumn, replicateNulls, bucketToPartition);
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
        return Objects.equals(functionHandle, that.functionHandle) &&
                Objects.equals(partitioningColumns, that.partitioningColumns) &&
                Objects.equals(hashColumn, that.hashColumn) &&
                replicateNulls == that.replicateNulls &&
                Objects.equals(bucketToPartition, that.bucketToPartition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionHandle, partitioningColumns, replicateNulls, bucketToPartition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("functionHandle", functionHandle)
                .add("partitioningChannels", partitioningColumns)
                .add("hashChannel", hashColumn)
                .add("replicateNulls", replicateNulls)
                .add("bucketToPartition", bucketToPartition)
                .toString();
    }
}
