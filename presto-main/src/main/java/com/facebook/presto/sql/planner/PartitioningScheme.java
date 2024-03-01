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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitioningScheme
{
    private final Partitioning partitioning;
    private final List<VariableReferenceExpression> outputLayout;
    private final Optional<VariableReferenceExpression> hashColumn;
    private final boolean replicateNullsAndAny;
    private final Optional<int[]> bucketToPartition;

    public PartitioningScheme(Partitioning partitioning, List<VariableReferenceExpression> outputLayout)
    {
        this(
                partitioning,
                outputLayout,
                Optional.empty(),
                false,
                Optional.empty());
    }

    public PartitioningScheme(Partitioning partitioning, List<VariableReferenceExpression> outputLayout, Optional<VariableReferenceExpression> hashColumn)
    {
        this(
                partitioning,
                outputLayout,
                hashColumn,
                false,
                Optional.empty());
    }

    @JsonCreator
    public PartitioningScheme(
            @JsonProperty("partitioning") Partitioning partitioning,
            @JsonProperty("outputLayout") List<VariableReferenceExpression> outputLayout,
            @JsonProperty("hashColumn") Optional<VariableReferenceExpression> hashColumn,
            @JsonProperty("replicateNullsAndAny") boolean replicateNullsAndAny,
            @JsonProperty("bucketToPartition") Optional<int[]> bucketToPartition)
    {
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.outputLayout = ImmutableList.copyOf(requireNonNull(outputLayout, "outputLayout is null"));

        Set<VariableReferenceExpression> columns = partitioning.getVariableReferences();
        checkArgument(ImmutableSet.copyOf(outputLayout).containsAll(columns),
                "Output layout (%s) don't include all partition columns (%s)", outputLayout, columns);

        this.hashColumn = requireNonNull(hashColumn, "hashColumn is null");
        hashColumn.ifPresent(column -> checkArgument(outputLayout.contains(column),
                "Output layout (%s) don't include hash column (%s)", outputLayout, column));

        checkArgument(!replicateNullsAndAny || columns.size() <= 1, "Must have at most one partitioning column when nullPartition is REPLICATE.");
        this.replicateNullsAndAny = replicateNullsAndAny;
        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
    }

    @JsonProperty
    public Partitioning getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getOutputLayout()
    {
        return outputLayout;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getHashColumn()
    {
        return hashColumn;
    }

    @JsonProperty
    public boolean isReplicateNullsAndAny()
    {
        return replicateNullsAndAny;
    }

    @JsonProperty
    public Optional<int[]> getBucketToPartition()
    {
        return bucketToPartition;
    }

    public PartitioningScheme withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new PartitioningScheme(partitioning, outputLayout, hashColumn, replicateNullsAndAny, bucketToPartition);
    }

    public PartitioningScheme translateOutputLayout(List<VariableReferenceExpression> newOutputLayout)
    {
        requireNonNull(newOutputLayout, "newOutputLayout is null");

        checkArgument(newOutputLayout.size() == outputLayout.size());

        Partitioning newPartitioning = partitioning.translateVariable(variable -> newOutputLayout.get(outputLayout.indexOf(variable)));

        Optional<VariableReferenceExpression> newHashSymbol = hashColumn
                .map(outputLayout::indexOf)
                .map(newOutputLayout::get);

        return new PartitioningScheme(newPartitioning, newOutputLayout, newHashSymbol, replicateNullsAndAny, bucketToPartition);
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
        PartitioningScheme that = (PartitioningScheme) o;
        return Objects.equals(partitioning, that.partitioning) &&
                Objects.equals(outputLayout, that.outputLayout) &&
                replicateNullsAndAny == that.replicateNullsAndAny &&
                Objects.equals(bucketToPartition, that.bucketToPartition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioning, outputLayout, replicateNullsAndAny, bucketToPartition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitioning", partitioning)
                .add("outputLayout", outputLayout)
                .add("hashChannel", hashColumn)
                .add("replicateNullsAndAny", replicateNullsAndAny)
                .add("bucketToPartition", bucketToPartition)
                .toString();
    }
}
