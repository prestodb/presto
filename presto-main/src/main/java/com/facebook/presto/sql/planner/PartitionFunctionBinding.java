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

import com.facebook.presto.spi.predicate.NullableValue;
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
    private final List<PartitionFunctionArgumentBinding> partitionFunctionArguments;
    private final Optional<Symbol> hashColumn;
    private final boolean replicateNulls;
    private final Optional<int[]> bucketToPartition;

    public PartitionFunctionBinding(PartitioningHandle partitioningHandle, List<Symbol> outputLayout, List<PartitionFunctionArgumentBinding> partitionFunctionArguments)
    {
        this(
                partitioningHandle,
                outputLayout,
                partitionFunctionArguments,
                Optional.empty(),
                false,
                Optional.empty());
    }

    public PartitionFunctionBinding(PartitioningHandle partitioningHandle, List<Symbol> outputLayout, List<PartitionFunctionArgumentBinding> partitionFunctionArguments, Optional<Symbol> hashColumn)
    {
        this(
                partitioningHandle,
                outputLayout,
                partitionFunctionArguments,
                hashColumn,
                false,
                Optional.empty());
    }

    @JsonCreator
    public PartitionFunctionBinding(
            @JsonProperty("partitioningHandle") PartitioningHandle partitioningHandle,
            @JsonProperty("outputLayout") List<Symbol> outputLayout,
            @JsonProperty("partitionFunctionArguments") List<PartitionFunctionArgumentBinding> partitionFunctionArguments,
            @JsonProperty("hashColumn") Optional<Symbol> hashColumn,
            @JsonProperty("replicateNulls") boolean replicateNulls,
            @JsonProperty("bucketToPartition") Optional<int[]> bucketToPartition)
    {
        this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
        this.outputLayout = ImmutableList.copyOf(requireNonNull(outputLayout, "outputLayout is null"));

        this.partitionFunctionArguments = ImmutableList.copyOf(requireNonNull(partitionFunctionArguments, "partitionFunctionArguments is null"));
        List<Symbol> partitionFunctionColumns = partitionFunctionArguments.stream()
                .filter(PartitionFunctionArgumentBinding::isVariable)
                .map(PartitionFunctionArgumentBinding::getColumn)
                .collect(toImmutableList());
        checkArgument(ImmutableSet.copyOf(outputLayout).containsAll(partitionFunctionColumns),
                "Output layout (%s) don't include all partition columns (%s)", outputLayout, partitionFunctionColumns);

        this.hashColumn = requireNonNull(hashColumn, "hashColumn is null");
        hashColumn.ifPresent(column -> checkArgument(outputLayout.contains(column),
                "Output layout (%s) don't include hash column (%s)", outputLayout, column));

        checkArgument(!replicateNulls || partitionFunctionArguments.size() == 1, "size of partitionFunctionArguments is not 1 when nullPartition is REPLICATE.");
        checkArgument(!replicateNulls || partitionFunctionArguments.get(0).isVariable(), "partition function argument must be variable when nullPartition is REPLICATE.");
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
    public List<PartitionFunctionArgumentBinding> getPartitionFunctionArguments()
    {
        return partitionFunctionArguments;
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
        return new PartitionFunctionBinding(partitioningHandle, outputLayout, partitionFunctionArguments, hashColumn, replicateNulls, bucketToPartition);
    }

    public PartitionFunctionBinding translateOutputLayout(List<Symbol> newOutputLayout)
    {
        requireNonNull(newOutputLayout, "newOutputLayout is null");

        checkArgument(newOutputLayout.size() == outputLayout.size());

        List<PartitionFunctionArgumentBinding> newPartitioningColumns = partitionFunctionArguments.stream()
                .map(argument -> translateOutputLayout(argument, newOutputLayout))
                .collect(toImmutableList());

        Optional<Symbol> newHashSymbol = hashColumn
                .map(outputLayout::indexOf)
                .map(newOutputLayout::get);

        return new PartitionFunctionBinding(partitioningHandle, newOutputLayout, newPartitioningColumns, newHashSymbol, replicateNulls, bucketToPartition);
    }

    private PartitionFunctionArgumentBinding translateOutputLayout(PartitionFunctionArgumentBinding argumentBinding, List<Symbol> newOutputLayout)
    {
        if (argumentBinding.isConstant()) {
            return argumentBinding;
        }
        int symbolIndex = outputLayout.indexOf(argumentBinding.getColumn());
        return new PartitionFunctionArgumentBinding(newOutputLayout.get(symbolIndex));
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
                Objects.equals(partitionFunctionArguments, that.partitionFunctionArguments) &&
                replicateNulls == that.replicateNulls &&
                Objects.equals(bucketToPartition, that.bucketToPartition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioningHandle, outputLayout, partitionFunctionArguments, replicateNulls, bucketToPartition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitioningHandle", partitioningHandle)
                .add("outputLayout", outputLayout)
                .add("partitioningChannels", partitionFunctionArguments)
                .add("hashChannel", hashColumn)
                .add("replicateNulls", replicateNulls)
                .add("bucketToPartition", bucketToPartition)
                .toString();
    }

    public static final class PartitionFunctionArgumentBinding
    {
        private final Symbol column;
        private final NullableValue constant;

        public PartitionFunctionArgumentBinding(Symbol column)
        {
            this.column = requireNonNull(column, "column is null");
            this.constant = null;
        }

        @JsonCreator
        public PartitionFunctionArgumentBinding(
                @JsonProperty("column") Symbol column,
                @JsonProperty("constant") NullableValue constant)
        {
            this.column = requireNonNull(column, "column is null");
            this.constant = constant;
        }

        public boolean isConstant()
        {
            return constant != null;
        }

        public boolean isVariable()
        {
            return constant == null;
        }

        @JsonProperty
        public Symbol getColumn()
        {
            return column;
        }

        @JsonProperty
        public NullableValue getConstant()
        {
            return constant;
        }

        @Override
        public String toString()
        {
            if (constant != null) {
                return constant.toString();
            }
            return "\"" + column + "\"";
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
            PartitionFunctionArgumentBinding that = (PartitionFunctionArgumentBinding) o;
            return Objects.equals(column, that.column) &&
                    Objects.equals(constant, that.constant);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(column, constant);
        }
    }
}
