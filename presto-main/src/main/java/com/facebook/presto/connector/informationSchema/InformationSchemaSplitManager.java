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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class InformationSchemaSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public InformationSchemaSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorSession session, ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain)
    {
        requireNonNull(tupleDomain, "tupleDomain is null");
        InformationSchemaTableHandle informationSchemaTableHandle = checkType(table, InformationSchemaTableHandle.class, "table");

        Map<ColumnHandle, NullableValue> bindings = TupleDomain.extractFixedValues(tupleDomain).get();

        List<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new InformationSchemaPartition(informationSchemaTableHandle, bindings));
        // We don't strip out the bindings that we have created from the undeterminedTupleDomain b/c the current InformationSchema
        // system requires that all filters be re-applied at execution time.
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorSession session, ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        requireNonNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(null, ImmutableList.<ConnectorSplit>of());
        }

        InformationSchemaTableHandle tableHandle = checkType(table, InformationSchemaTableHandle.class, "table");

        ConnectorPartition partition = Iterables.getOnlyElement(partitions);
        InformationSchemaPartition informationSchemaPartition = checkType(partition, InformationSchemaPartition.class, "partition");

        List<HostAddress> localAddress = ImmutableList.of(nodeManager.getCurrentNode().getHostAndPort());

        ImmutableMap.Builder<String, NullableValue> filters = ImmutableMap.builder();
        for (Entry<ColumnHandle, NullableValue> entry : informationSchemaPartition.getFilters().entrySet()) {
            InformationSchemaColumnHandle informationSchemaColumnHandle = (InformationSchemaColumnHandle) entry.getKey();
            filters.put(informationSchemaColumnHandle.getColumnName(), entry.getValue());
        }

        ConnectorSplit split = new InformationSchemaSplit(tableHandle.getConnectorId(), informationSchemaPartition.getTable(), filters.build(), localAddress);

        return new FixedSplitSource(null, ImmutableList.of(split));
    }

    public static class InformationSchemaPartition
            implements ConnectorPartition
    {
        private final InformationSchemaTableHandle table;
        private final Map<ColumnHandle, NullableValue> filters;

        public InformationSchemaPartition(InformationSchemaTableHandle table, Map<ColumnHandle, NullableValue> filters)
        {
            this.table = requireNonNull(table, "table is null");
            this.filters = ImmutableMap.copyOf(requireNonNull(filters, "filters is null"));
        }

        public InformationSchemaTableHandle getTable()
        {
            return table;
        }

        @Override
        public String getPartitionId()
        {
            return table.getSchemaTableName().toString();
        }

        @Override
        public TupleDomain<ColumnHandle> getTupleDomain()
        {
            return TupleDomain.fromFixedValues(filters);
        }

        public Map<ColumnHandle, NullableValue> getFilters()
        {
            return filters;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("table", table)
                    .add("filters", filters)
                    .toString();
        }
    }
}
