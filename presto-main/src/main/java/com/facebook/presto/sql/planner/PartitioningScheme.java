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
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitioningScheme
{
    public enum Replication
    {
        NONE {
            @Override
            public boolean isNoReplication()
            {
                return true;
            }

            boolean isCompatibleWithColumns(int columnsCount)
            {
                return true;
            }
        },

        /**
         * Replicates nulls and also makes sures every node gets some data if there is any data globally.
         */
        REPLICATE_NULLS_AND_ANY {
            boolean isCompatibleWithColumns(int columnsCount)
            {
                return columnsCount <= 1;
            }
        },

        REPLICATE_NULLS {
            boolean isCompatibleWithColumns(int columnsCount)
            {
                return columnsCount <= 1;
            }
        };

        public boolean isNoReplication()
        {
            return false;
        }

        abstract boolean isCompatibleWithColumns(int columnsCount);
    }

    private final Partitioning partitioning;
    private final List<Symbol> outputLayout;
    private final Optional<Symbol> hashColumn;
    private final Replication replication;
    private final Optional<int[]> bucketToPartition;

    public PartitioningScheme(Partitioning partitioning, List<Symbol> outputLayout)
    {
        this(
                partitioning,
                outputLayout,
                Optional.empty(),
                Replication.NONE,
                Optional.empty());
    }

    public PartitioningScheme(Partitioning partitioning, List<Symbol> outputLayout, Optional<Symbol> hashColumn)
    {
        this(
                partitioning,
                outputLayout,
                hashColumn,
                Replication.NONE,
                Optional.empty());
    }

    @JsonCreator
    public PartitioningScheme(
            @JsonProperty("partitioning") Partitioning partitioning,
            @JsonProperty("outputLayout") List<Symbol> outputLayout,
            @JsonProperty("hashColumn") Optional<Symbol> hashColumn,
            @JsonProperty("replication") Replication replication,
            @JsonProperty("bucketToPartition") Optional<int[]> bucketToPartition)
    {
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.outputLayout = ImmutableList.copyOf(requireNonNull(outputLayout, "outputLayout is null"));

        Set<Symbol> columns = partitioning.getColumns();
        checkArgument(ImmutableSet.copyOf(outputLayout).containsAll(columns),
                "Output layout (%s) don't include all partition columns (%s)", outputLayout, columns);

        this.hashColumn = requireNonNull(hashColumn, "hashColumn is null");
        hashColumn.ifPresent(column -> checkArgument(outputLayout.contains(column),
                "Output layout (%s) don't include hash column (%s)", outputLayout, column));

        requireNonNull(replication, "replication is null");
        checkArgument(replication.isCompatibleWithColumns(columns.size()), "Replication %s cannot be used with %s partitioning columns", replication, columns.size());
        this.replication = replication;

        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
    }

    @JsonProperty
    public Partitioning getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public List<Symbol> getOutputLayout()
    {
        return outputLayout;
    }

    @JsonProperty
    public Optional<Symbol> getHashColumn()
    {
        return hashColumn;
    }

    @JsonProperty
    public Replication getReplication()
    {
        return replication;
    }

    @JsonProperty
    public Optional<int[]> getBucketToPartition()
    {
        return bucketToPartition;
    }

    public PartitioningScheme withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new PartitioningScheme(partitioning, outputLayout, hashColumn, replication, bucketToPartition);
    }

    public PartitioningScheme translateOutputLayout(List<Symbol> newOutputLayout)
    {
        requireNonNull(newOutputLayout, "newOutputLayout is null");

        checkArgument(newOutputLayout.size() == outputLayout.size());

        Partitioning newPartitioning = partitioning.translate(symbol -> newOutputLayout.get(outputLayout.indexOf(symbol)));

        Optional<Symbol> newHashSymbol = hashColumn
                .map(outputLayout::indexOf)
                .map(newOutputLayout::get);

        return new PartitioningScheme(newPartitioning, newOutputLayout, newHashSymbol, replication, bucketToPartition);
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
                replication == that.replication &&
                Objects.equals(bucketToPartition, that.bucketToPartition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioning, outputLayout, replication, bucketToPartition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitioning", partitioning)
                .add("outputLayout", outputLayout)
                .add("hashChannel", hashColumn)
                .add("replication", replication)
                .add("bucketToPartition", bucketToPartition)
                .toString();
    }
}
