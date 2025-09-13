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
package com.facebook.presto.metadata;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TableLayout
{
    private final ConnectorId connectorId;
    private final ConnectorTableHandle connectorTableHandle;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorTableLayout layout;

    public TableLayout(ConnectorId connectorId, ConnectorTableHandle connectorTableHandle, ConnectorTransactionHandle transactionHandle, ConnectorTableLayout layout)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(connectorTableHandle, "connectorTableHandle is null");
        requireNonNull(transactionHandle, "transactionHandle is null");
        requireNonNull(layout, "layout is null");
        this.connectorTableHandle = connectorTableHandle;
        this.connectorId = connectorId;
        this.transactionHandle = transactionHandle;
        this.layout = layout;
    }

    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    public Optional<List<ColumnHandle>> getColumns()
    {
        return layout.getColumns();
    }

    public TupleDomain<ColumnHandle> getPredicate()
    {
        return layout.getPredicate();
    }

    public Optional<RowExpression> getRemainingPredicate()
    {
        return layout.getRemainingPredicate();
    }

    public List<LocalProperty<ColumnHandle>> getLocalProperties()
    {
        return layout.getLocalProperties();
    }

    public Optional<ColumnHandle> getUniqueColumn()
    {
        return layout.getUniqueColumn();
    }

    public ConnectorTableLayoutHandle getLayoutHandle()
    {
        return layout.getHandle();
    }

    public TableHandle getNewTableHandle()
    {
        return new TableHandle(connectorId, connectorTableHandle, transactionHandle, Optional.of(layout.getHandle()));
    }

    public Optional<TablePartitioning> getTablePartitioning()
    {
        return layout.getTablePartitioning()
                .map(nodePartitioning -> new TablePartitioning(
                        new PartitioningHandle(
                                Optional.of(connectorId),
                                Optional.of(transactionHandle),
                                nodePartitioning.getPartitioningHandle()),
                        nodePartitioning.getPartitioningColumns()));
    }

    public Optional<Set<ColumnHandle>> getStreamPartitioningColumns()
    {
        return layout.getStreamPartitioningColumns();
    }

    public Optional<DiscretePredicates> getDiscretePredicates()
    {
        return layout.getDiscretePredicates();
    }

    public static TableLayout fromConnectorLayout(ConnectorId connectorId, ConnectorTableHandle connectorTableHandle, ConnectorTransactionHandle transactionHandle, ConnectorTableLayout layout)
    {
        return new TableLayout(connectorId, connectorTableHandle, transactionHandle, layout);
    }

    public static class TablePartitioning
    {
        private final PartitioningHandle partitioningHandle;
        private final List<ColumnHandle> partitioningColumns;

        public TablePartitioning(PartitioningHandle partitioningHandle, List<ColumnHandle> partitioningColumns)
        {
            this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
            this.partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle;
        }

        public List<ColumnHandle> getPartitioningColumns()
        {
            return partitioningColumns;
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
            TablePartitioning that = (TablePartitioning) o;
            return Objects.equals(partitioningHandle, that.partitioningHandle) &&
                    Objects.equals(partitioningColumns, that.partitioningColumns);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitioningHandle, partitioningColumns);
        }
    }
}
