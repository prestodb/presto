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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.BaseHiveTableLayoutHandle;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergTableLayoutHandle
        extends BaseHiveTableLayoutHandle
{
    private final List<IcebergColumnHandle> partitionColumns;
    private final Map<String, IcebergColumnHandle> predicateColumns;
    private final Optional<Set<IcebergColumnHandle>> requestedColumns;
    private final IcebergTableHandle table;

    @JsonCreator
    public IcebergTableLayoutHandle(
            @JsonProperty("partitionColumns") List<IcebergColumnHandle> partitionColumns,
            @JsonProperty("domainPredicate") TupleDomain<Subfield> domainPredicate,
            @JsonProperty("remainingPredicate") RowExpression remainingPredicate,
            @JsonProperty("predicateColumns") Map<String, IcebergColumnHandle> predicateColumns,
            @JsonProperty("requestedColumns") Optional<Set<IcebergColumnHandle>> requestedColumns,
            @JsonProperty("pushdownFilterEnabled") boolean pushdownFilterEnabled,
            @JsonProperty("partitionColumnPredicate") TupleDomain<ColumnHandle> partitionColumnPredicate,
            @JsonProperty("table") IcebergTableHandle table)
    {
        this(
                partitionColumns,
                domainPredicate,
                remainingPredicate,
                predicateColumns,
                requestedColumns,
                pushdownFilterEnabled,
                partitionColumnPredicate,
                Optional.empty(),
                table);
    }

    protected IcebergTableLayoutHandle(
            List<IcebergColumnHandle> partitionColumns,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            Map<String, IcebergColumnHandle> predicateColumns,
            Optional<Set<IcebergColumnHandle>> requestedColumns,
            boolean pushdownFilterEnabled,
            TupleDomain<ColumnHandle> partitionColumnPredicate,
            Optional<List<HivePartition>> partitions,
            IcebergTableHandle table)
    {
        super(domainPredicate, remainingPredicate, pushdownFilterEnabled, partitionColumnPredicate, partitions);

        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
        this.requestedColumns = requireNonNull(requestedColumns, "requestedColumns is null");
        this.table = requireNonNull(table, "table is null");
    }

    @JsonProperty
    public List<IcebergColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public Map<String, IcebergColumnHandle> getPredicateColumns()
    {
        return predicateColumns;
    }

    @JsonProperty
    public Optional<Set<IcebergColumnHandle>> getRequestedColumns()
    {
        return requestedColumns;
    }

    @JsonProperty
    public IcebergTableHandle getTable()
    {
        return table;
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
        IcebergTableLayoutHandle that = (IcebergTableLayoutHandle) o;
        return Objects.equals(getDomainPredicate(), that.getDomainPredicate()) &&
                Objects.equals(getRemainingPredicate(), that.getRemainingPredicate()) &&
                Objects.equals(partitionColumns, that.partitionColumns) &&
                Objects.equals(predicateColumns, that.predicateColumns) &&
                Objects.equals(requestedColumns, that.requestedColumns) &&
                Objects.equals(isPushdownFilterEnabled(), that.isPushdownFilterEnabled()) &&
                Objects.equals(getPartitionColumnPredicate(), that.getPartitionColumnPredicate()) &&
                Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getDomainPredicate(), getRemainingPredicate(), predicateColumns, requestedColumns, isPushdownFilterEnabled(), getPartitionColumnPredicate(), table);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }

    public static class Builder
    {
        private List<IcebergColumnHandle> partitionColumns;
        private TupleDomain<Subfield> domainPredicate;
        private RowExpression remainingPredicate;
        private Map<String, IcebergColumnHandle> predicateColumns;
        private Optional<Set<IcebergColumnHandle>> requestedColumns;
        private boolean pushdownFilterEnabled;
        private TupleDomain<ColumnHandle> partitionColumnPredicate;
        private Optional<List<HivePartition>> partitions;
        private IcebergTableHandle table;

        public Builder setPartitionColumns(List<IcebergColumnHandle> partitionColumns)
        {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder setDomainPredicate(TupleDomain<Subfield> domainPredicate)
        {
            this.domainPredicate = domainPredicate;
            return this;
        }

        public Builder setRemainingPredicate(RowExpression remainingPredicate)
        {
            this.remainingPredicate = remainingPredicate;
            return this;
        }

        public Builder setPredicateColumns(Map<String, IcebergColumnHandle> predicateColumns)
        {
            this.predicateColumns = predicateColumns;
            return this;
        }

        public Builder setRequestedColumns(Optional<Set<IcebergColumnHandle>> requestedColumns)
        {
            this.requestedColumns = requestedColumns;
            return this;
        }

        public Builder setPushdownFilterEnabled(boolean pushdownFilterEnabled)
        {
            this.pushdownFilterEnabled = pushdownFilterEnabled;
            return this;
        }

        public Builder setPartitionColumnPredicate(TupleDomain<ColumnHandle> partitionColumnPredicate)
        {
            this.partitionColumnPredicate = partitionColumnPredicate;
            return this;
        }

        public Builder setPartitions(Optional<List<HivePartition>> partitions)
        {
            this.partitions = partitions;
            return this;
        }

        public Builder setTable(IcebergTableHandle table)
        {
            this.table = table;
            return this;
        }

        public IcebergTableLayoutHandle build()
        {
            return new IcebergTableLayoutHandle(
                    partitionColumns,
                    domainPredicate,
                    remainingPredicate,
                    predicateColumns,
                    requestedColumns,
                    pushdownFilterEnabled,
                    partitionColumnPredicate,
                    partitions,
                    table);
        }
    }
}
