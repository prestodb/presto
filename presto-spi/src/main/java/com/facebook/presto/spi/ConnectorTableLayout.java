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
package com.facebook.presto.spi;

import com.facebook.presto.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class ConnectorTableLayout
{
    private final ConnectorTableLayoutHandle handle;
    private final Optional<List<ColumnHandle>> columns;
    private final TupleDomain<ColumnHandle> predicate;
    private final Optional<List<TupleDomain<ColumnHandle>>> discretePredicates;
    private final Optional<Set<ColumnHandle>> partitioningColumns;
    private final List<LocalProperty<ColumnHandle>> localProperties;

    public ConnectorTableLayout(ConnectorTableLayoutHandle handle)
    {
        this(handle,
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                emptyList());
    }

    public ConnectorTableLayout(
            ConnectorTableLayoutHandle handle,
            Optional<List<ColumnHandle>> columns,
            TupleDomain<ColumnHandle> predicate,
            Optional<Set<ColumnHandle>> partitioningColumns,
            Optional<List<TupleDomain<ColumnHandle>>> discretePredicates,
            List<LocalProperty<ColumnHandle>> localProperties)
    {
        requireNonNull(handle, "handle is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(partitioningColumns, "partitioningColumns is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(discretePredicates, "discretePredicates is null");
        requireNonNull(localProperties, "localProperties is null");

        this.handle = handle;
        this.columns = columns;
        this.partitioningColumns = partitioningColumns;
        this.predicate = predicate;
        this.discretePredicates = discretePredicates;
        this.localProperties = localProperties;
    }

    public ConnectorTableLayoutHandle getHandle()
    {
        return handle;
    }

    /**
     * The columns from the original table provided by this layout. A layout may provide only a subset of columns.
     */
    public Optional<List<ColumnHandle>> getColumns()
    {
        return columns;
    }

    /**
     * A predicate that describes the universe of data in this layout. It may be used by the query engine to
     * infer additional properties and perform further optimizations
     */
    public TupleDomain<ColumnHandle> getPredicate()
    {
        return predicate;
    }

    /**
     * The partitioning for the table.
     * If empty, the table layout is partitioned arbitrarily.
     * Otherwise, it is partitioned on the given set of columns (or unpartitioned, if the set is empty)
     * <p>
     * If the table is partitioned, the connector guarantees that each combination of values for
     * the partition columns will be contained within a single split (i.e., partitions cannot
     * straddle multiple splits)
     */
    public Optional<Set<ColumnHandle>> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    /**
     * A collection of discrete predicates describing the data in this layout. The union of
     * these predicates is expected to be equivalent to the overall predicate returned
     * by {@link #getPredicate()}. They may be used by the engine for further optimizations.
     */
    public Optional<List<TupleDomain<ColumnHandle>>> getDiscretePredicates()
    {
        return discretePredicates;
    }

    /**
     * Properties describing the layout of the data (grouping/sorting) within each partition
     */
    public List<LocalProperty<ColumnHandle>> getLocalProperties()
    {
        return localProperties;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(handle, columns, predicate, discretePredicates, partitioningColumns, localProperties);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final ConnectorTableLayout other = (ConnectorTableLayout) obj;
        return Objects.equals(this.handle, other.handle)
                && Objects.equals(this.columns, other.columns)
                && Objects.equals(this.predicate, other.predicate)
                && Objects.equals(this.discretePredicates, other.discretePredicates)
                && Objects.equals(this.partitioningColumns, other.partitioningColumns)
                && Objects.equals(this.localProperties, other.localProperties);
    }
}
