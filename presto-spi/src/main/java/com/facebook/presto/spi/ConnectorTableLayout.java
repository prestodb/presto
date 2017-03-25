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
    private final Optional<ConnectorNodePartitioning> nodePartitioning;
    private final Optional<Set<ColumnHandle>> streamPartitioningColumns;
    private final Optional<DiscretePredicates> discretePredicates;
    private final List<LocalProperty<ColumnHandle>> localProperties;

    public ConnectorTableLayout(ConnectorTableLayoutHandle handle)
    {
        this(handle,
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                emptyList());
    }

    public ConnectorTableLayout(
            ConnectorTableLayoutHandle handle,
            Optional<List<ColumnHandle>> columns,
            TupleDomain<ColumnHandle> predicate,
            Optional<ConnectorNodePartitioning> nodePartitioning,
            Optional<Set<ColumnHandle>> streamPartitioningColumns,
            Optional<DiscretePredicates> discretePredicates,
            List<LocalProperty<ColumnHandle>> localProperties)
    {
        requireNonNull(handle, "handle is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(streamPartitioningColumns, "partitioningColumns is null");
        requireNonNull(nodePartitioning, "nodePartitioning is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(discretePredicates, "discretePredicates is null");
        requireNonNull(localProperties, "localProperties is null");

        this.handle = handle;
        this.columns = columns;
        this.nodePartitioning = nodePartitioning;
        this.streamPartitioningColumns = streamPartitioningColumns;
        this.predicate = predicate;
        this.discretePredicates = discretePredicates;
        this.localProperties = localProperties;
    }

    public ConnectorTableLayoutHandle getHandle()
    {
        return handle;
    }

    /**
     * The columns from the original table provided by this layout.
     * <p>
     * A layout may provide only a subset of columns.
     * Optional.empty() means "all columns" while empty list means "no columns".
     */
    public Optional<List<ColumnHandle>> getColumns()
    {
        return columns;
    }

    /**
     * A predicate that describes the universe of data in this layout.
     * <p>
     * For example, one of the columns might be always greater than zero due to the nature of the data.
     * It may be used by the query engine to infer additional properties and perform further optimizations,
     * such as removing unnecessary filters.
     */
    public TupleDomain<ColumnHandle> getPredicate()
    {
        return predicate;
    }

    /**
     * The partitioning of the table across the worker nodes.
     * <p>
     * If the table is node partitioned, the connector guarantees that each combination of values for
     * the distributed columns will be contained within a single worker.
     * Can be used across tables.
     * For example, it's used in Raptor for collocated joins.
     */
    public Optional<ConnectorNodePartitioning> getNodePartitioning()
    {
        return nodePartitioning;
    }

    /**
     * The partitioning for the table streams (splits).
     * <p>
     * If empty, the table layout is partitioned arbitrarily.
     * Otherwise, table steams are partitioned on the given set of columns (or unpartitioned, if the set is empty)
     * <p>
     * If the table is partitioned, the connector guarantees that each combination of values for
     * the partition columns will be contained within a single split (i.e., partitions cannot
     * straddle multiple splits).
     * This property is useful for sharded tables and can be used by the engine to avoid partial aggregations.
     */
    public Optional<Set<ColumnHandle>> getStreamPartitioningColumns()
    {
        return streamPartitioningColumns;
    }

    /**
     * A collection of discrete predicates describing the data in this layout. The union of
     * these predicates is expected to be equivalent to the overall predicate returned
     * by {@link #getPredicate()}. They may be used by the engine for further optimizations.
     * This is a way to support Hive partitions.
     */
    public Optional<DiscretePredicates> getDiscretePredicates()
    {
        return discretePredicates;
    }

    /**
     * Describes the layout of the data (grouping/sorting) within a split.
     * <p>
     * There are the following properties:
     * - sorting property says that data per split will be coming in a sorted order. Example: 1 2 2 3 5.
     * - grouping property is a weaker guarantee that says that data will be coming in groups of the same values coming together, but
     * not necessarily in the overall sorted order. Example: 3 3 1 5 5 5 2 2 2.
     * - constant property says that all values are the same.
     * Local properties can be combined in a hierarchical way, for example values are sorted by column A then (within the same value of A)
     * grouped by column B and then (within the same value of A and B) are constant.
     * This information can be used to efficiently run window functions by avoiding sorting and re-partitioning of incoming data.
     */
    public List<LocalProperty<ColumnHandle>> getLocalProperties()
    {
        return localProperties;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(handle, columns, predicate, discretePredicates, streamPartitioningColumns, nodePartitioning, localProperties);
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
        ConnectorTableLayout other = (ConnectorTableLayout) obj;
        return Objects.equals(this.handle, other.handle)
                && Objects.equals(this.columns, other.columns)
                && Objects.equals(this.predicate, other.predicate)
                && Objects.equals(this.discretePredicates, other.discretePredicates)
                && Objects.equals(this.streamPartitioningColumns, other.streamPartitioningColumns)
                && Objects.equals(this.nodePartitioning, other.nodePartitioning)
                && Objects.equals(this.localProperties, other.localProperties);
    }
}
