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
package com.facebook.presto.hive;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BaseHiveTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final List<BaseHiveColumnHandle> partitionColumns;
    private final TupleDomain<Subfield> domainPredicate;
    private final RowExpression remainingPredicate;
    private final boolean pushdownFilterEnabled;
    private final TupleDomain<ColumnHandle> partitionColumnPredicate;

    // coordinator-only properties
    private final Optional<List<HivePartition>> partitions;

    @JsonCreator
    public BaseHiveTableLayoutHandle(
            @JsonProperty("partitionColumns") List<BaseHiveColumnHandle> partitionColumns,
            @JsonProperty("domainPredicate") TupleDomain<Subfield> domainPredicate,
            @JsonProperty("remainingPredicate") RowExpression remainingPredicate,
            @JsonProperty("pushdownFilterEnabled") boolean pushdownFilterEnabled,
            @JsonProperty("partitionColumnPredicate") TupleDomain<ColumnHandle> partitionColumnPredicate,
            @JsonProperty("partitions") Optional<List<HivePartition>> partitions)
    {
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
        this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
        this.pushdownFilterEnabled = pushdownFilterEnabled;
        this.partitionColumnPredicate = requireNonNull(partitionColumnPredicate, "partitionColumnPredicate is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
    }

    @JsonProperty
    public List<BaseHiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public TupleDomain<Subfield> getDomainPredicate()
    {
        return domainPredicate;
    }

    @JsonProperty
    public RowExpression getRemainingPredicate()
    {
        return remainingPredicate;
    }

    @JsonProperty
    public boolean isPushdownFilterEnabled()
    {
        return pushdownFilterEnabled;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPartitionColumnPredicate()
    {
        return partitionColumnPredicate;
    }

    /**
     * Partitions are dropped when HiveTableLayoutHandle is serialized.
     *
     * @return list of partitions if available, {@code Optional.empty()} if dropped
     */
    @JsonIgnore
    public Optional<List<HivePartition>> getPartitions()
    {
        return partitions;
    }
}
